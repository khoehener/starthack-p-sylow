import os
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union

import mysql.connector
import pandas as pd
from mysql.connector import Error


@dataclass
class DBConfig:
    host: str = "127.0.0.1"
    port: int = 3306
    user: str = "root"
    password: str = ""
    database: str = "hack2026"


class MySQLDBConnector:
    """
    Zentrale DB-Kommunikationsschicht für eure MySQL-Datenbank.

    Zuständigkeiten:
    - Verbindung aufbauen
    - Tabellen/Spalten introspektieren
    - Datensätze einfügen / updaten / lesen
    - Pandas DataFrames schema-konform in Tabellen schreiben
    - Case-zentrierte Hilfsfunktionen für tbCaseData + Importtabellen

    Nicht zuständig für:
    - Parsing / Normalisierung / Business-Logik
    """

    ALLOWED_TABLES = {
        "tbCaseData",
        "tbImportAcData",
        "tbImportLabsData",
        "tbImportIcd10Data",
        "tbImportDeviceMotionData",
        "tbImportDevice1HzMotionData",
        "tbImportMedicationInpatientData",
        "tbImportNursingDailyReportsData",
    }

    CASE_CHILD_TABLES = {
        "tbImportAcData",
        "tbImportLabsData",
        "tbImportIcd10Data",
        "tbImportDeviceMotionData",
        "tbImportDevice1HzMotionData",
        "tbImportMedicationInpatientData",
        "tbImportNursingDailyReportsData",
    }

    def __init__(self, config: DBConfig):
        self.config = config
        self._column_cache: Dict[str, List[str]] = {}

    @classmethod
    def from_env(cls) -> "MySQLDBConnector":
        return cls(
            DBConfig(
                host=os.getenv("MYSQL_HOST", "127.0.0.1"),
                port=int(os.getenv("MYSQL_PORT", "3306")),
                user=os.getenv("MYSQL_USER", "root"),
                password=os.getenv("MYSQL_PASSWORD", ""),
                database=os.getenv("MYSQL_DATABASE", "hack2026"),
            )
        )

    @contextmanager
    def connection(self):
        conn = None
        try:
            conn = mysql.connector.connect(
                host=self.config.host,
                port=self.config.port,
                user=self.config.user,
                password=self.config.password,
                database=self.config.database,
                autocommit=False,
            )
            yield conn
            conn.commit()
        except Exception:
            if conn is not None:
                conn.rollback()
            raise
        finally:
            if conn is not None and conn.is_connected():
                conn.close()

    def test_connection(self) -> bool:
        try:
            with self.connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT DATABASE(), VERSION()")
                db_name, version = cursor.fetchone()
                cursor.close()
                print(f"Verbunden mit DB='{db_name}', MySQL='{version}'")
            return True
        except Error as e:
            print(f"DB-Verbindung fehlgeschlagen: {e}")
            return False

    def _validate_table(self, table: str) -> None:
        if table not in self.ALLOWED_TABLES:
            raise ValueError(f"Unbekannte oder nicht erlaubte Tabelle: {table}")

    def get_table_columns(self, table: str) -> List[str]:
        self._validate_table(table)

        if table in self._column_cache:
            return self._column_cache[table]

        query = """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
        """

        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, (self.config.database, table))
            columns = [row[0] for row in cursor.fetchall()]
            cursor.close()

        if not columns:
            raise ValueError(f"Keine Spalten für Tabelle {table} gefunden.")

        self._column_cache[table] = columns
        return columns

    def list_tables(self) -> List[str]:
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SHOW TABLES")
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
        return tables

    def _to_db_value(self, value: Any) -> Any:
        if pd.isna(value):
            return None

        if isinstance(value, pd.Timestamp):
            return value.to_pydatetime()

        # numpy scalar / pandas scalar abfangen
        if hasattr(value, "item"):
            try:
                return value.item()
            except Exception:
                pass

        return value

    def _sanitize_record(
        self,
        table: str,
        record: Dict[str, Any],
        drop_unknown: bool = True,
        exclude_auto_id: bool = True,
    ) -> Dict[str, Any]:
        columns = self.get_table_columns(table)
        valid_columns = set(columns)

        cleaned: Dict[str, Any] = {}
        for key, value in record.items():
            if key not in valid_columns:
                if drop_unknown:
                    continue
                raise ValueError(f"Spalte '{key}' existiert nicht in Tabelle '{table}'.")

            if exclude_auto_id and key == "coId":
                continue

            cleaned[key] = self._to_db_value(value)

        return cleaned

    def _sanitize_dataframe(
        self,
        table: str,
        df: pd.DataFrame,
        drop_unknown: bool = True,
        exclude_auto_id: bool = True,
    ) -> pd.DataFrame:
        columns = self.get_table_columns(table)
        valid_columns = set(columns)

        keep_cols = []
        for col in df.columns:
            if col in valid_columns:
                if exclude_auto_id and col == "coId":
                    continue
                keep_cols.append(col)
            elif not drop_unknown:
                raise ValueError(f"Spalte '{col}' existiert nicht in Tabelle '{table}'.")

        cleaned = df[keep_cols].copy()

        for col in cleaned.columns:
            cleaned[col] = cleaned[col].map(self._to_db_value)

        return cleaned

    def insert_one(self, table: str, record: Dict[str, Any], drop_unknown: bool = True) -> int:
        self._validate_table(table)
        clean = self._sanitize_record(table, record, drop_unknown=drop_unknown)

        if not clean:
            raise ValueError(f"Keine gültigen Daten zum Einfügen in {table}.")

        columns = list(clean.keys())
        placeholders = ", ".join(["%s"] * len(columns))
        column_sql = ", ".join(f"`{col}`" for col in columns)
        sql = f"INSERT INTO `{table}` ({column_sql}) VALUES ({placeholders})"

        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, tuple(clean[col] for col in columns))
            new_id = cursor.lastrowid
            cursor.close()

        return int(new_id)

    def insert_many(
        self,
        table: str,
        records: List[Dict[str, Any]],
        drop_unknown: bool = True,
        chunk_size: int = 1000,
    ) -> int:
        self._validate_table(table)

        if not records:
            return 0

        cleaned_records = [
            self._sanitize_record(table, r, drop_unknown=drop_unknown) for r in records
        ]
        cleaned_records = [r for r in cleaned_records if r]

        if not cleaned_records:
            return 0

        schema_columns = self.get_table_columns(table)
        insert_columns = [
            col for col in schema_columns
            if col != "coId" and any(col in rec for rec in cleaned_records)
        ]

        if not insert_columns:
            return 0

        placeholders = ", ".join(["%s"] * len(insert_columns))
        column_sql = ", ".join(f"`{col}`" for col in insert_columns)
        sql = f"INSERT INTO `{table}` ({column_sql}) VALUES ({placeholders})"

        total_inserted = 0
        with self.connection() as conn:
            cursor = conn.cursor()
            for start in range(0, len(cleaned_records), chunk_size):
                batch = cleaned_records[start:start + chunk_size]
                values = [
                    tuple(rec.get(col) for col in insert_columns)
                    for rec in batch
                ]
                cursor.executemany(sql, values)
                total_inserted += cursor.rowcount
            cursor.close()

        return total_inserted

    def insert_dataframe(
        self,
        table: str,
        df: pd.DataFrame,
        case_id: Optional[int] = None,
        drop_unknown: bool = True,
        chunk_size: int = 1000,
    ) -> int:
        self._validate_table(table)

        if df.empty:
            return 0

        cleaned_df = self._sanitize_dataframe(table, df, drop_unknown=drop_unknown)

        if case_id is not None and "coCaseId" in self.get_table_columns(table):
            if "coCaseId" not in cleaned_df.columns:
                cleaned_df["coCaseId"] = case_id
            else:
                cleaned_df["coCaseId"] = cleaned_df["coCaseId"].fillna(case_id)

        records = cleaned_df.to_dict(orient="records")
        return self.insert_many(
            table=table,
            records=records,
            drop_unknown=drop_unknown,
            chunk_size=chunk_size,
        )

    def read(
        self,
        table: str,
        where: Optional[Dict[str, Any]] = None,
        columns: Optional[Sequence[str]] = None,
        limit: Optional[int] = None,
        order_by: Optional[str] = None,
        as_dataframe: bool = True,
    ) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
        self._validate_table(table)

        table_columns = self.get_table_columns(table)
        valid_columns = set(table_columns)

        if columns is None:
            select_sql = "*"
        else:
            invalid = [c for c in columns if c not in valid_columns]
            if invalid:
                raise ValueError(f"Ungültige Spalten in SELECT: {invalid}")
            select_sql = ", ".join(f"`{c}`" for c in columns)

        sql = f"SELECT {select_sql} FROM `{table}`"
        params: List[Any] = []

        if where:
            invalid = [k for k in where.keys() if k not in valid_columns]
            if invalid:
                raise ValueError(f"Ungültige WHERE-Spalten: {invalid}")

            clauses = []
            for key, value in where.items():
                if value is None:
                    clauses.append(f"`{key}` IS NULL")
                else:
                    clauses.append(f"`{key}` = %s")
                    params.append(self._to_db_value(value))
            sql += " WHERE " + " AND ".join(clauses)

        if order_by:
            if order_by not in valid_columns:
                raise ValueError(f"Ungültige ORDER BY-Spalte: {order_by}")
            sql += f" ORDER BY `{order_by}`"

        if limit is not None:
            sql += " LIMIT %s"
            params.append(limit)

        with self.connection() as conn:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall()
            cursor.close()

        if as_dataframe:
            return pd.DataFrame(rows)
        return rows

    def update_by_id(
        self,
        table: str,
        row_id: int,
        values: Dict[str, Any],
        drop_unknown: bool = True,
    ) -> int:
        self._validate_table(table)

        clean = self._sanitize_record(table, values, drop_unknown=drop_unknown)
        clean.pop("coId", None)

        if not clean:
            return 0

        set_sql = ", ".join(f"`{col}` = %s" for col in clean.keys())
        sql = f"UPDATE `{table}` SET {set_sql} WHERE `coId` = %s"
        params = [clean[col] for col in clean.keys()] + [row_id]

        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, tuple(params))
            affected = cursor.rowcount
            cursor.close()

        return affected

    def find_case(
        self,
        coId: Optional[int] = None,
        coPatientId: Optional[int] = None,
        coE2I222: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        where: Dict[str, Any] = {}

        if coId is not None:
            where["coId"] = coId
        if coPatientId is not None:
            where["coPatientId"] = coPatientId
        if coE2I222 is not None:
            where["coE2I222"] = coE2I222

        if not where:
            raise ValueError("Mindestens ein Suchkriterium für find_case angeben.")

        rows = self.read("tbCaseData", where=where, limit=1, as_dataframe=False)
        return rows[0] if rows else None

    def upsert_case(
        self,
        case_record: Dict[str, Any],
        match_fields: Sequence[str] = ("coPatientId", "coE2I222"),
    ) -> int:
        """
        Sucht zuerst nach bestehendem Case über match_fields.
        Falls gefunden -> Update.
        Falls nicht gefunden -> Insert.
        """
        allowed_match_fields = {"coId", "coPatientId", "coE2I222"}
        invalid = [f for f in match_fields if f not in allowed_match_fields]
        if invalid:
            raise ValueError(f"Ungültige match_fields: {invalid}")

        search_criteria = {
            field: case_record.get(field)
            for field in match_fields
            if case_record.get(field) is not None
        }

        existing_case = None
        if search_criteria:
            rows = self.read("tbCaseData", where=search_criteria, limit=1, as_dataframe=False)
            existing_case = rows[0] if rows else None

        if existing_case:
            case_id = int(existing_case["coId"])
            update_values = dict(case_record)
            update_values.pop("coId", None)
            self.update_by_id("tbCaseData", case_id, update_values)
            return case_id

        return self.insert_one("tbCaseData", case_record)

    def insert_case_child_dataframe(
        self,
        table: str,
        df: pd.DataFrame,
        case_id: int,
        drop_unknown: bool = True,
        chunk_size: int = 1000,
    ) -> int:
        if table not in self.CASE_CHILD_TABLES:
            raise ValueError(f"{table} ist keine Case-Child-Tabelle.")

        return self.insert_dataframe(
            table=table,
            df=df,
            case_id=case_id,
            drop_unknown=drop_unknown,
            chunk_size=chunk_size,
        )

    def get_case_bundle(self, case_id: int) -> Dict[str, pd.DataFrame]:
        """
        Holt einen kompletten Fall:
        - tbCaseData (genau 1 Zeile)
        - alle Child-Tabellen mit coCaseId = case_id
        """
        bundle: Dict[str, pd.DataFrame] = {}
        bundle["tbCaseData"] = self.read("tbCaseData", where={"coId": case_id}, as_dataframe=True)

        for table in self.CASE_CHILD_TABLES:
            bundle[table] = self.read(table, where={"coCaseId": case_id}, as_dataframe=True)

        return bundle

    def store_case_package(
        self,
        case_record: Dict[str, Any],
        table_frames: Dict[str, pd.DataFrame],
        match_fields: Sequence[str] = ("coPatientId", "coE2I222"),
        drop_unknown: bool = True,
        chunk_size: int = 1000,
    ) -> int:
        """
        Komfortfunktion:
        1. Case in tbCaseData anlegen oder updaten
        2. Alle übergebenen DataFrames in die passenden Importtabellen schreiben
           und automatisch coCaseId setzen
        """
        case_id = self.upsert_case(case_record, match_fields=match_fields)

        for table, df in table_frames.items():
            self.insert_case_child_dataframe(
                table=table,
                df=df,
                case_id=case_id,
                drop_unknown=drop_unknown,
                chunk_size=chunk_size,
            )

        return case_id
    
    def execute_sql(self, sql: str, params: tuple = ()) -> None:
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            cursor.close()

    def fetch_all_sql(self, sql: str, params: tuple = ()):
        with self.connection() as conn:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(sql, params)
            rows = cursor.fetchall()
            cursor.close()
        return rows

    def clear_table(self, table: str) -> None:
        self._validate_table(table)
        self.execute_sql(f"DELETE FROM `{table}`")

    def clear_all_tables(self) -> None:
        # Reihenfolge: erst Child-Tabellen, dann tbCaseData
        ordered_tables = [
            "tbImportAcData",
            "tbImportLabsData",
            "tbImportIcd10Data",
            "tbImportDeviceMotionData",
            "tbImportDevice1HzMotionData",
            "tbImportMedicationInpatientData",
            "tbImportNursingDailyReportsData",
            "tbCaseData",
        ]
        for table in ordered_tables:
            self.clear_table(table)