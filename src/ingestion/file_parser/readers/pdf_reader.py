from __future__ import annotations

from ingestion.file_parser.models import FileIssue, ReadResult


def read_pdf_file(file_path: str) -> ReadResult:
    return ReadResult(
        file_path=file_path,
        file_type="pdf",
        success=False,
        issues=[
            FileIssue(
                severity="warning",
                message="PDF-Parsing ist noch nicht implementiert.",
            )
        ],
    )