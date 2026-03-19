from __future__ import annotations

import sys
import tempfile
import urllib.request
from pathlib import Path

import pytest

# Make project root importable
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from ingestion.file_parser import read_file
from ingestion.file_parser.models import ReadResult
from data_sources import DATASETS, DATASETS_CLINIC, DATASETS_ERROR


# ─────────────────────────────────────────
# Helper
# ─────────────────────────────────────────

def download_and_read(url: str) -> ReadResult:
    """
    Downloads a file from a URL into a temp file,
    runs read_file on it, then cleans up.
    """
    suffix = Path(url).suffix  # gets .csv, .xlsx, .pdf etc.

    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        tmp_path = tmp.name

    try:
        urllib.request.urlretrieve(url, tmp_path)
        result = read_file(tmp_path)
    finally:
        Path(tmp_path).unlink(missing_ok=True)

    return result


def print_result_summary(name: str, result: ReadResult) -> None:
    print(f"\n{'─'*50}")
    print(f"Dataset : {name}")
    print(f"Success : {result.success}")
    if result.data is not None:
        print(f"Columns : {list(result.data.columns)}")
        print(f"Rows    : {len(result.data)}")
        print(f"Preview :")
        print(result.data.head(3).to_string())
    if result.issues:
        for issue in result.issues:
            print(f"[{issue.severity.upper()}] {issue.message}")
    if result.metadata:
        for key in ["encoding", "separator", "used_sheet", "parsing_plan"]:
            if key in result.metadata:
                print(f"{key}: {result.metadata[key]}")


# ─────────────────────────────────────────
# DATASETS tests (no errors)
# ─────────────────────────────────────────

class TestDatasets:

    def test_epa_1(self):
        result = download_and_read(DATASETS["epa_1"])
        print_result_summary("epa_1", result)
        assert result.success is True
        assert result.data is not None
        assert len(result.data) > 0
        assert len(result.data.columns) > 1

    def test_epa_2(self):
        result = download_and_read(DATASETS["epa_2"])
        print_result_summary("epa_2", result)
        assert result.success is True
        assert result.data is not None
        assert len(result.data) > 0
        assert len(result.data.columns) > 1

    def test_epa_3(self):
        result = download_and_read(DATASETS["epa_3"])
        print_result_summary("epa_3", result)
        assert result.success is True
        assert result.data is not None
        assert len(result.data) > 0
        assert len(result.data.columns) > 1

    def test_epa_4_excel(self):
        result = download_and_read(DATASETS["epa_4"])
        print_result_summary("epa_4", result)
        assert result.success is True
        assert result.data is not None
        assert len(result.data) > 0
        assert "used_sheet" in result.metadata

    def test_epa_5(self):
        result = download_and_read(DATASETS["epa_5"])
        print_result_summary("epa_5", result)
        assert result.success is True
        assert result.data is not None
        assert len(result.data) > 0

    def test_labs_csv(self):
        result = download_and_read(DATASETS["labs_csv"])
        print_result_summary("labs_csv", result)
        assert result.success is True
        assert result.data is not None
        assert len(result.data) > 0

    def test_labs_xlsx(self):
        result = download_and_read(DATASETS["labs_xlsx"])
        print_result_summary("labs_xlsx", result)
        assert result.success is True
        assert result.data is not None
        assert len(result.data) > 0

    def test_cases(self):
        result = download_and_read(DATASETS["cases"])
        print_result_summary("cases", result)
        assert result.success is True
        assert result.data is not None
        assert len(result.data) > 0

    def test_motion(self):
        result = download_and_read(DATASETS["motion"])
        print_result_summary("motion", result)
        assert result.success is True
        assert result.data is not None
        assert len(result.data) > 0

    def test_motion_raw(self):
        result = download_and_read(DATASETS["motion_raw"])
        print_result_summary("motion_raw", result)
        assert result.success is True
        assert result.data is not None
        assert len(result.data) > 0

    def test_medication(self):
        result = download_and_read(DATASETS["medication"])
        print_result_summary("medication", result)
        assert result.success is True
        assert result.data is not None
        assert len(result.data) > 0

    def test_nursing(self):
        result = download_and_read(DATASETS["nursing"])
        print_result_summary("nursing", result)
        assert result.success is True
        assert result.data is not None
        assert len(result.data) > 0


# ─────────────────────────────────────────
# DATASETS_CLINIC tests
# ─────────────────────────────────────────

class TestDatasetsClinic:

    # --- Clinic 1 ---

    def test_clinic_1_device(self):
        result = download_and_read(DATASETS_CLINIC["clinic_1_device"])
        print_result_summary("clinic_1_device", result)
        assert result.success is True
        assert result.data is not None

    def test_clinic_1_device_1hz(self):
        result = download_and_read(DATASETS_CLINIC["clinic_1_device_1hz"])
        print_result_summary("clinic_1_device_1hz", result)
        assert result.success is True
        assert result.data is not None

    def test_clinic_1_epa(self):
        result = download_and_read(DATASETS_CLINIC["clinic_1_epa"])
        print_result_summary("clinic_1_epa", result)
        assert result.success is True
        assert result.data is not None

    def test_clinic_1_icd_ops(self):
        result = download_and_read(DATASETS_CLINIC["clinic_1_icd_ops"])
        print_result_summary("clinic_1_icd_ops", result)
        assert result.success is True
        assert result.data is not None

    def test_clinic_1_labs(self):
        result = download_and_read(DATASETS_CLINIC["clinic_1_labs"])
        print_result_summary("clinic_1_labs", result)
        assert result.success is True
        assert result.data is not None

    def test_clinic_1_medication(self):
        result = download_and_read(DATASETS_CLINIC["clinic_1_medication"])
        print_result_summary("clinic_1_medication", result)
        assert result.success is True
        assert result.data is not None

    def test_clinic_1_nursing(self):
        result = download_and_read(DATASETS_CLINIC["clinic_1_nursing"])
        print_result_summary("clinic_1_nursing", result)
        assert result.success is True
        assert result.data is not None

    # --- Clinic 2 ---

    def test_clinic_2_device(self):
        result = download_and_read(DATASETS_CLINIC["clinic_2_device"])
        print_result_summary("clinic_2_device", result)
        assert result.success is True
        assert result.data is not None

    def test_clinic_2_epa(self):
        result = download_and_read(DATASETS_CLINIC["clinic_2_epa"])
        print_result_summary("clinic_2_epa", result)
        assert result.success is True
        assert result.data is not None

    def test_clinic_2_icd_ops(self):
        result = download_and_read(DATASETS_CLINIC["clinic_2_icd_ops"])
        print_result_summary("clinic_2_icd_ops", result)
        assert result.success is True
        assert result.data is not None

    def test_clinic_2_labs(self):
        result = download_and_read(DATASETS_CLINIC["clinic_2_labs"])
        print_result_summary("clinic_2_labs", result)
        assert result.success is True
        assert result.data is not None

    def test_clinic_2_medication(self):
        result = download_and_read(DATASETS_CLINIC["clinic_2_medication"])
        print_result_summary("clinic_2_medication", result)
        assert result.success is True
        assert result.data is not None

    def test_clinic_2_nursing(self):
        result = download_and_read(DATASETS_CLINIC["clinic_2_nursing"])
        print_result_summary("clinic_2_nursing", result)
        assert result.success is True
        assert result.data is not None

    # --- Clinic 3 ---

    def test_clinic_3_device(self):
        result = download_and_read(DATASETS_CLINIC["clinic_3_device"])
        print_result_summary("clinic_3_device", result)
        assert result.success is True
        assert result.data is not None

    def test_clinic_3_device_1hz(self):
        result = download_and_read(DATASETS_CLINIC["clinic_3_device_1hz"])
        print_result_summary("clinic_3_device_1hz", result)
        assert result.success is True
        assert result.data is not None

    def test_clinic_3_epa(self):
        result = download_and_read(DATASETS_CLINIC["clinic_3_epa"])
        print_result_summary("clinic_3_epa", result)
        assert result.success is True
        assert result.data is not None

    def test_clinic_3_icd_ops(self):
        result = download_and_read(DATASETS_CLINIC["clinic_3_icd_ops"])
        print_result_summary("clinic_3_icd_ops", result)
        assert result.success is True
        assert result.data is not None

    def test_clinic_3_labs(self):
        result = download_and_read(DATASETS_CLINIC["clinic_3_labs"])
        print_result_summary("clinic_3_labs", result)
        assert result.success is True
        assert result.data is not None

    def test_clinic_3_medication(self):
        result = download_and_read(DATASETS_CLINIC["clinic_3_medication"])
        print_result_summary("clinic_3_medication", result)
        assert result.success is True
        assert result.data is not None

    def test_clinic_3_nursing(self):
        result = download_and_read(DATASETS_CLINIC["clinic_3_nursing"])
        print_result_summary("clinic_3_nursing", result)
        assert result.success is True
        assert result.data is not None

    # --- Clinic 4 ---

    def test_clinic_4_device(self):
        result = download_and_read(DATASETS_CLINIC["clinic_4_device"])
        print_result_summary("clinic_4_device", result)
        assert result.success is True
        assert result.data is not None

    def test_clinic_4_device_1hz(self):
        result = download_and_read(DATASETS_CLINIC["clinic_4_device_1hz"])
        print_result_summary("clinic_4_device_1hz", result)
        assert result.success is True
        assert result.data is not None

    def test_clinic_4_epa_excel(self):
        result = download_and_read(DATASETS_CLINIC["clinic_4_epa"])
        print_result_summary("clinic_4_epa", result)
        assert result.success is True
        assert result.data is not None
        assert "used_sheet" in result.metadata

    def test_clinic_4_icd_ops(self):
        result = download_and_read(DATASETS_CLINIC["clinic_4_icd_ops"])
        print_result_summary("clinic_4_icd_ops", result)
        assert result.success is True
        assert result.data is not None

    def test_clinic_4_labs(self):
        result = download_and_read(DATASETS_CLINIC["clinic_4_labs"])
        print_result_summary("clinic_4_labs", result)
        assert result.success is True
        assert result.data is not None

    def test_clinic_4_medication(self):
        result = download_and_read(DATASETS_CLINIC["clinic_4_medication"])
        print_result_summary("clinic_4_medication", result)
        assert result.success is True
        assert result.data is not None

    def test_clinic_4_nursing_pdf(self):
        # PDF is not yet implemented - expect success=False with a warning
        result = download_and_read(DATASETS_CLINIC["clinic_4_nursing_pdf"])
        print_result_summary("clinic_4_nursing_pdf", result)
        assert result.success is False
        assert result.file_type == "pdf"
        assert any(issue.severity == "warning" for issue in result.issues)


# ─────────────────────────────────────────
# DATASETS_ERROR tests
# These files are intentionally broken so we
# check the parser handles them gracefully
# ─────────────────────────────────────────

class TestDatasetsError:

    def test_epa_3_error(self):
        result = download_and_read(DATASETS_ERROR["epa_3"])
        print_result_summary("epa_3 [error]", result)
        # Should still attempt to read, just may have warnings
        assert isinstance(result, ReadResult)
        assert result.file_type == "csv"

    def test_cases_icd_ops_csv_error(self):
        result = download_and_read(DATASETS_ERROR["cases_icd_ops_csv"])
        print_result_summary("cases_icd_ops_csv [error]", result)
        assert isinstance(result, ReadResult)

    def test_cases_icd_ops_xlsx_error(self):
        result = download_and_read(DATASETS_ERROR["cases_icd_ops_xlsx"])
        print_result_summary("cases_icd_ops_xlsx [error]", result)
        assert isinstance(result, ReadResult)

    def test_device_motion_csv_error(self):
        result = download_and_read(DATASETS_ERROR["device_motion_csv"])
        print_result_summary("device_motion_csv [error]", result)
        assert isinstance(result, ReadResult)

    def test_device_motion_xlsx_error(self):
        result = download_and_read(DATASETS_ERROR["device_motion_xlsx"])
        print_result_summary("device_motion_xlsx [error]", result)
        assert isinstance(result, ReadResult)

    def test_device_raw_1hz_csv_error(self):
        result = download_and_read(DATASETS_ERROR["device_raw_1hz_csv"])
        print_result_summary("device_raw_1hz_csv [error]", result)
        assert isinstance(result, ReadResult)

    def test_device_raw_1hz_xlsx_error(self):
        result = download_and_read(DATASETS_ERROR["device_raw_1hz_xlsx"])
        print_result_summary("device_raw_1hz_xlsx [error]", result)
        assert isinstance(result, ReadResult)

    def test_labs_csv_error(self):
        result = download_and_read(DATASETS_ERROR["labs_csv"])
        print_result_summary("labs_csv [error]", result)
        assert isinstance(result, ReadResult)

    def test_labs_xlsx_error(self):
        result = download_and_read(DATASETS_ERROR["labs_xlsx"])
        print_result_summary("labs_xlsx [error]", result)
        assert isinstance(result, ReadResult)

    def test_medication_csv_error(self):
        result = download_and_read(DATASETS_ERROR["medication_csv"])
        print_result_summary("medication_csv [error]", result)
        assert isinstance(result, ReadResult)

    def test_medication_xlsx_error(self):
        result = download_and_read(DATASETS_ERROR["medication_xlsx"])
        print_result_summary("medication_xlsx [error]", result)
        assert isinstance(result, ReadResult)

    def test_nursing_csv_error(self):
        result = download_and_read(DATASETS_ERROR["nursing_csv"])
        print_result_summary("nursing_csv [error]", result)
        assert isinstance(result, ReadResult)

    def test_nursing_xlsx_error(self):
        result = download_and_read(DATASETS_ERROR["nursing_xlsx"])
        print_result_summary("nursing_xlsx [error]", result)
        assert isinstance(result, ReadResult)