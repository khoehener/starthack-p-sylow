from __future__ import annotations

from pathlib import Path


SUPPORTED_DELIMITERS = [",", ";", "\t", "|"]
CSV_ENCODINGS_TO_TRY = ["utf-8", "utf-8-sig", "latin1"]


def detect_file_type(file_path: str) -> str:
    suffix = Path(file_path).suffix.lower()

    if suffix == ".csv":
        return "csv"
    if suffix in [".xlsx", ".xls"]:
        return "excel"
    if suffix == ".pdf":
        return "pdf"
    return "unknown"