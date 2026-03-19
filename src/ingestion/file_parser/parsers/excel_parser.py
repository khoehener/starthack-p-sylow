from __future__ import annotations

import csv
from io import StringIO
from pathlib import Path

import pandas as pd

from ingestion.file_parser.models import FileIssue, ParsingPlan, ReadResult


def _rebuild_table_from_embedded_column(
    df: pd.DataFrame,
    delimiter: str,
    target_column: int = 0,
) -> pd.DataFrame:
    header_text = str(df.columns[target_column])
    column_values = df.iloc[:, target_column].astype(str).tolist()
    raw_text = "\n".join([header_text] + column_values)
    parsed_rows = list(csv.reader(StringIO(raw_text), delimiter=delimiter))

    if not parsed_rows:
        return df

    header = parsed_rows[0]
    expected_len = len(header)
    cleaned_rows: list[list[str]] = []

    for row in parsed_rows[1:]:
        if len(row) < expected_len:
            row = row + [""] * (expected_len - len(row))
        elif len(row) > expected_len:
            row = row[:expected_len]
        cleaned_rows.append(row)

    return pd.DataFrame(cleaned_rows, columns=header)


def _apply_excel_parsing_plan(
    file_path: str,
    plan: ParsingPlan,
) -> ReadResult:
    path = Path(file_path)
    issues: list[FileIssue] = []

    excel_file = pd.ExcelFile(file_path)
    sheet_names = excel_file.sheet_names
    sheet_name = plan.sheet_name or sheet_names[0]

    if sheet_name not in sheet_names:
        sheet_name = sheet_names[0]
        issues.append(
            FileIssue(
                severity="warning",
                message="Plan-Sheet nicht gefunden, erstes Sheet wurde verwendet.",
            )
        )

    if plan.strategy == "direct_excel":
        df = pd.read_excel(
            file_path,
            sheet_name=sheet_name,
            header=plan.header_row_index,
        )
        if plan.data_start_row_index > plan.header_row_index + 1:
            data_offset = plan.data_start_row_index - (plan.header_row_index + 1)
            if data_offset > 0:
                df = df.iloc[data_offset:].reset_index(drop=True)

        return ReadResult(
            file_path=str(path),
            file_type="excel",
            success=True,
            data=df,
            issues=issues,
            metadata={
                "sheet_names": sheet_names,
                "used_sheet": sheet_name,
                "columns": list(df.columns),
                "row_count": len(df),
                "parsing_plan": plan.__dict__,
            },
        )

    if plan.strategy == "split_embedded_delimited_column":
        raw_df = pd.read_excel(file_path, sheet_name=sheet_name)
        delimiter = plan.delimiter or ","
        target_column = 0
        if plan.target_columns:
            target_column = plan.target_columns[0]

        rebuilt_df = _rebuild_table_from_embedded_column(
            raw_df,
            delimiter=delimiter,
            target_column=target_column,
        )
        issues.append(
            FileIssue(
                severity="warning",
                message=(
                    "Excel-Datei enthielt eingebetteten delimiter-basierten Text "
                    "und wurde über ParsingPlan in Spalten aufgeteilt."
                ),
            )
        )
        return ReadResult(
            file_path=str(path),
            file_type="excel",
            success=True,
            data=rebuilt_df,
            issues=issues,
            metadata={
                "sheet_names": sheet_names,
                "used_sheet": sheet_name,
                "columns": list(rebuilt_df.columns),
                "row_count": len(rebuilt_df),
                "parsing_plan": plan.__dict__,
            },
        )

    if plan.strategy == "drop_top_rows_then_parse":
        raw_df = pd.read_excel(
            file_path,
            sheet_name=sheet_name,
            header=None,
        )
        header_row_index = max(plan.header_row_index, 0)

        if header_row_index >= len(raw_df):
            return ReadResult(
                file_path=str(path),
                file_type="excel",
                success=False,
                issues=[
                    FileIssue(
                        severity="error",
                        message="ParsingPlan verweist auf eine Header-Zeile außerhalb des Sheets.",
                    )
                ],
                metadata={"parsing_plan": plan.__dict__},
            )

        header = raw_df.iloc[header_row_index].astype(str).tolist()
        data_df = raw_df.iloc[plan.data_start_row_index:].reset_index(drop=True)
        data_df.columns = header[: data_df.shape[1]]

        return ReadResult(
            file_path=str(path),
            file_type="excel",
            success=True,
            data=data_df,
            issues=issues,
            metadata={
                "sheet_names": sheet_names,
                "used_sheet": sheet_name,
                "columns": list(data_df.columns),
                "row_count": len(data_df),
                "parsing_plan": plan.__dict__,
            },
        )

    return ReadResult(
        file_path=str(path),
        file_type="excel",
        success=False,
        issues=[
            FileIssue(
                severity="error",
                message="ParsingPlan konnte für Excel nicht angewendet werden.",
            )
        ],
        metadata={"parsing_plan": plan.__dict__},
    )