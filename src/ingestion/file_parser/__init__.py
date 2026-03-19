from __future__ import annotations

from ingestion.file_parser.models import FileIssue, ParsingPlan, ReadResult
from ingestion.file_parser.detection import detect_file_type


def read_file(file_path: str) -> ReadResult:
    file_type = detect_file_type(file_path)

    if file_type == "csv":
        from ingestion.file_parser.readers.csv_reader import read_csv_file
        return read_csv_file(file_path)

    if file_type == "excel":
        from ingestion.file_parser.readers.excel_reader import read_excel_file
        return read_excel_file(file_path)

    if file_type == "pdf":
        from ingestion.file_parser.readers.pdf_reader import read_pdf_file
        return read_pdf_file(file_path)

    return ReadResult(
        file_path=file_path,
        file_type="unknown",
        success=False,
        issues=[FileIssue(severity="error", message="Nicht unterstützter Dateityp.")],
    )


__all__ = [
    "read_file",
    "ReadResult",
    "FileIssue",
    "ParsingPlan",
    "detect_file_type",
]