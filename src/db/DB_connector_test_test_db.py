import pandas as pd
import pytest

from DB_connector import MySQLDBConnector, DBConfig


def make_db():
    return MySQLDBConnector(
        DBConfig(
            host="127.0.0.1",
            port=3306,
            user="root",
            password="p-sylow26",
            database="hack2026_test",
        )
    )


def setup_function():
    db = make_db()
    db.clear_all_tables()


def teardown_function():
    db = make_db()
    db.clear_all_tables()


def test_connection_works():
    db = make_db()
    assert db.test_connection() is True


def test_list_tables_contains_expected_tables():
    db = make_db()
    tables = db.list_tables()

    expected = {
        "tbCaseData",
        "tbImportAcData",
        "tbImportLabsData",
        "tbImportIcd10Data",
        "tbImportDeviceMotionData",
        "tbImportDevice1HzMotionData",
        "tbImportMedicationInpatientData",
        "tbImportNursingDailyReportsData",
    }

    assert expected.issubset(set(tables))


def test_get_table_columns_case_data_contains_expected_columns():
    db = make_db()
    columns = db.get_table_columns("tbCaseData")

    assert "coId" in columns
    assert "coPatientId" in columns
    assert "coE2I222" in columns
    assert "coLastname" in columns
    assert "coFirstname" in columns


def test_upsert_case_creates_case():
    db = make_db()

    case_id = db.upsert_case({
        "coPatientId": 1,
        "coE2I222": 111,
        "coLastname": "Muster",
    })

    rows = db.fetch_all_sql("SELECT * FROM tbCaseData WHERE coId = %s", (case_id,))
    assert len(rows) == 1
    assert rows[0]["coPatientId"] == 1
    assert rows[0]["coLastname"] == "Muster"


def test_upsert_case_updates_existing_case_instead_of_creating_duplicate():
    db = make_db()

    case_id_1 = db.upsert_case({
        "coPatientId": 10,
        "coE2I222": 1000,
        "coLastname": "Alt",
        "coFirstname": "Anna",
    })

    case_id_2 = db.upsert_case({
        "coPatientId": 10,
        "coE2I222": 1000,
        "coLastname": "Neu",
        "coFirstname": "Berta",
    })

    assert case_id_1 == case_id_2

    rows = db.fetch_all_sql(
        "SELECT * FROM tbCaseData WHERE coId = %s",
        (case_id_1,)
    )
    assert len(rows) == 1
    assert rows[0]["coLastname"] == "Neu"
    assert rows[0]["coFirstname"] == "Berta"

    count_rows = db.fetch_all_sql("SELECT COUNT(*) AS cnt FROM tbCaseData")
    assert count_rows[0]["cnt"] == 1


def test_find_case_by_patient_id():
    db = make_db()

    case_id = db.upsert_case({
        "coPatientId": 77,
        "coE2I222": 7700,
        "coLastname": "Patient77",
    })

    found = db.find_case(coPatientId=77)

    assert found is not None
    assert found["coId"] == case_id
    assert found["coLastname"] == "Patient77"


def test_find_case_returns_none_if_not_found():
    db = make_db()

    found = db.find_case(coPatientId=999999)
    assert found is None


def test_update_by_id_updates_only_target_row():
    db = make_db()

    id1 = db.upsert_case({
        "coPatientId": 201,
        "coE2I222": 2010,
        "coFirstname": "Alice",
    })
    id2 = db.upsert_case({
        "coPatientId": 202,
        "coE2I222": 2020,
        "coFirstname": "Bob",
    })

    affected = db.update_by_id("tbCaseData", id1, {"coFirstname": "Changed"})
    assert affected == 1

    rows1 = db.fetch_all_sql("SELECT coFirstname FROM tbCaseData WHERE coId = %s", (id1,))
    rows2 = db.fetch_all_sql("SELECT coFirstname FROM tbCaseData WHERE coId = %s", (id2,))

    assert rows1[0]["coFirstname"] == "Changed"
    assert rows2[0]["coFirstname"] == "Bob"


def test_insert_case_child_dataframe_sets_case_id():
    db = make_db()

    case_id = db.upsert_case({
        "coPatientId": 2,
        "coE2I222": 222,
    })

    df = pd.DataFrame([
        {"coSodium_mmol_L": "140"},
        {"coSodium_mmol_L": "141"},
    ])

    inserted = db.insert_case_child_dataframe(
        table="tbImportLabsData",
        df=df,
        case_id=case_id,
    )

    assert inserted == 2

    rows = db.fetch_all_sql(
        "SELECT * FROM tbImportLabsData WHERE coCaseId = %s ORDER BY coId",
        (case_id,)
    )
    assert len(rows) == 2
    assert rows[0]["coCaseId"] == case_id
    assert rows[0]["coSodium_mmol_L"] == "140"
    assert rows[1]["coSodium_mmol_L"] == "141"


def test_insert_case_child_dataframe_rejects_non_child_table():
    db = make_db()

    df = pd.DataFrame([{"coLastname": "X"}])

    with pytest.raises(ValueError):
        db.insert_case_child_dataframe(
            table="tbCaseData",
            df=df,
            case_id=1,
        )


def test_insert_dataframe_drops_unknown_columns_by_default():
    db = make_db()

    case_id = db.upsert_case({
        "coPatientId": 3,
        "coE2I222": 333,
    })

    df = pd.DataFrame([
        {
            "coSodium_mmol_L": "139",
            "coSodium_flag": "ok",
            "totally_unknown_column": "should_be_ignored",
        }
    ])

    inserted = db.insert_dataframe(
        table="tbImportLabsData",
        df=df,
        case_id=case_id,
    )

    assert inserted == 1

    rows = db.fetch_all_sql(
        "SELECT * FROM tbImportLabsData WHERE coCaseId = %s",
        (case_id,)
    )
    assert len(rows) == 1
    assert rows[0]["coSodium_mmol_L"] == "139"
    assert rows[0]["coSodium_flag"] == "ok"


def test_insert_dataframe_raises_on_unknown_columns_if_drop_unknown_false():
    db = make_db()

    df = pd.DataFrame([
        {
            "coSodium_mmol_L": "139",
            "not_a_real_column": "boom",
        }
    ])

    with pytest.raises(ValueError):
        db.insert_dataframe(
            table="tbImportLabsData",
            df=df,
            drop_unknown=False,
        )


def test_insert_dataframe_empty_df_returns_zero():
    db = make_db()

    empty_df = pd.DataFrame()

    inserted = db.insert_dataframe(
        table="tbImportLabsData",
        df=empty_df,
    )

    assert inserted == 0


def test_insert_one_ignores_auto_increment_id():
    db = make_db()

    inserted_id = db.insert_one("tbCaseData", {
        "coId": 999999,   # sollte ignoriert werden
        "coPatientId": 44,
        "coE2I222": 4444,
        "coLastname": "AutoID",
    })

    row = db.fetch_all_sql("SELECT * FROM tbCaseData WHERE coId = %s", (inserted_id,))
    assert len(row) == 1
    assert row[0]["coPatientId"] == 44
    assert row[0]["coLastname"] == "AutoID"
    assert row[0]["coId"] != 999999


def test_insert_one_raises_if_all_columns_invalid():
    db = make_db()

    with pytest.raises(ValueError):
        db.insert_one("tbCaseData", {
            "not_a_column": "x"
        }, drop_unknown=False)


def test_nan_values_become_null_in_database():
    db = make_db()

    case_id = db.upsert_case({
        "coPatientId": 55,
        "coE2I222": 5555,
    })

    df = pd.DataFrame([
        {
            "coSodium_mmol_L": pd.NA,
            "coSodium_flag": "missing",
        }
    ])

    inserted = db.insert_dataframe(
        table="tbImportLabsData",
        df=df,
        case_id=case_id,
    )

    assert inserted == 1

    rows = db.fetch_all_sql(
        "SELECT coSodium_mmol_L, coSodium_flag FROM tbImportLabsData WHERE coCaseId = %s",
        (case_id,)
    )
    assert len(rows) == 1
    assert rows[0]["coSodium_mmol_L"] is None
    assert rows[0]["coSodium_flag"] == "missing"


def test_read_returns_dataframe_with_expected_rows():
    db = make_db()

    id1 = db.upsert_case({
        "coPatientId": 61,
        "coE2I222": 6100,
        "coLastname": "A",
    })
    id2 = db.upsert_case({
        "coPatientId": 62,
        "coE2I222": 6200,
        "coLastname": "B",
    })

    result = db.read(
        table="tbCaseData",
        where={"coLastname": "A"},
        columns=["coId", "coLastname"],
        as_dataframe=True,
    )

    assert len(result) == 1
    assert result.iloc[0]["coId"] == id1
    assert result.iloc[0]["coLastname"] == "A"
    assert id2 not in result["coId"].tolist()


def test_read_with_limit_and_order_by():
    db = make_db()

    db.upsert_case({"coPatientId": 71, "coE2I222": 7100, "coLastname": "Z"})
    db.upsert_case({"coPatientId": 72, "coE2I222": 7200, "coLastname": "A"})
    db.upsert_case({"coPatientId": 73, "coE2I222": 7300, "coLastname": "M"})

    result = db.read(
        table="tbCaseData",
        columns=["coLastname"],
        order_by="coLastname",
        limit=2,
        as_dataframe=True,
    )

    assert len(result) == 2
    assert result.iloc[0]["coLastname"] == "A"
    assert result.iloc[1]["coLastname"] == "M"


def test_get_case_bundle_returns_case_and_children():
    db = make_db()

    case_id = db.upsert_case({
        "coPatientId": 88,
        "coE2I222": 8800,
        "coLastname": "Bundle",
    })

    labs_df = pd.DataFrame([
        {"coSodium_mmol_L": "140"},
        {"coSodium_mmol_L": "141"},
    ])
    meds_df = pd.DataFrame([
        {"coPatient_id": "88", "coMedication_name": "Paracetamol"},
    ])

    db.insert_case_child_dataframe("tbImportLabsData", labs_df, case_id=case_id)
    db.insert_case_child_dataframe("tbImportMedicationInpatientData", meds_df, case_id=case_id)

    bundle = db.get_case_bundle(case_id)

    assert "tbCaseData" in bundle
    assert "tbImportLabsData" in bundle
    assert "tbImportMedicationInpatientData" in bundle

    assert len(bundle["tbCaseData"]) == 1
    assert len(bundle["tbImportLabsData"]) == 2
    assert len(bundle["tbImportMedicationInpatientData"]) == 1

    assert bundle["tbCaseData"].iloc[0]["coLastname"] == "Bundle"


def test_store_case_package_writes_multiple_tables():
    db = make_db()

    case_record = {
        "coPatientId": 99,
        "coE2I222": 9900,
        "coLastname": "Package",
        "coFirstname": "Tester",
    }

    labs_df = pd.DataFrame([
        {"coSodium_mmol_L": "137", "coSodium_flag": "normal"},
        {"coSodium_mmol_L": "136", "coSodium_flag": "normal"},
    ])

    nursing_df = pd.DataFrame([
        {"coPatient_id": "99", "coWard": "A1", "coShift": "night"},
    ])

    case_id = db.store_case_package(
        case_record=case_record,
        table_frames={
            "tbImportLabsData": labs_df,
            "tbImportNursingDailyReportsData": nursing_df,
        },
    )

    case_rows = db.fetch_all_sql("SELECT * FROM tbCaseData WHERE coId = %s", (case_id,))
    lab_rows = db.fetch_all_sql("SELECT * FROM tbImportLabsData WHERE coCaseId = %s", (case_id,))
    nursing_rows = db.fetch_all_sql(
        "SELECT * FROM tbImportNursingDailyReportsData WHERE coCaseId = %s",
        (case_id,)
    )

    assert len(case_rows) == 1
    assert case_rows[0]["coLastname"] == "Package"

    assert len(lab_rows) == 2
    assert all(row["coCaseId"] == case_id for row in lab_rows)

    assert len(nursing_rows) == 1
    assert nursing_rows[0]["coCaseId"] == case_id
    assert nursing_rows[0]["coWard"] == "A1"


def test_store_case_package_updates_existing_case_if_match_fields_hit():
    db = make_db()

    first_case_id = db.store_case_package(
        case_record={
            "coPatientId": 111,
            "coE2I222": 11100,
            "coLastname": "OldName",
        },
        table_frames={},
    )

    second_case_id = db.store_case_package(
        case_record={
            "coPatientId": 111,
            "coE2I222": 11100,
            "coLastname": "NewName",
        },
        table_frames={},
    )

    assert first_case_id == second_case_id

    rows = db.fetch_all_sql("SELECT * FROM tbCaseData WHERE coId = %s", (first_case_id,))
    assert len(rows) == 1
    assert rows[0]["coLastname"] == "NewName"


def test_invalid_table_name_raises_value_error():
    db = make_db()

    with pytest.raises(ValueError):
        db.read("totally_fake_table")


def test_read_with_invalid_column_raises_value_error():
    db = make_db()

    with pytest.raises(ValueError):
        db.read(
            table="tbCaseData",
            columns=["coId", "not_a_column"],
        )


def test_get_table_columns_invalid_table_raises_value_error():
    db = make_db()

    with pytest.raises(ValueError):
        db.get_table_columns("not_a_real_table")