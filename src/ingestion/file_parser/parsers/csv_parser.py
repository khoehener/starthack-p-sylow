from __future__ import annotations

from pathlib import Path

from ingestion.file_parser.models import FileIssue, ParsingPlan, ReadResult
from ingestion.file_parser.detection import CSV_ENCODINGS_TO_TRY, SUPPORTED_DELIMITERS
from ingestion.file_parser.heuristics.csv_heuristics import _try_read_csv_candidate

import pandas as pd


def _apply_csv_parsing_plan(
    file_path: str,
    plan: ParsingPlan,
) -> ReadResult:
    path = Path(file_path)
    issues: list[FileIssue] = []

    if plan.strategy == "direct_csv":
        encodings = [plan.encoding] if plan.encoding else CSV_ENCODINGS_TO_TRY
        delimiters = [plan.delimiter] if plan.delimiter else SUPPORTED_DELIMITERS

        best_df = None
        best_score = float("-inf")
        best_meta: dict = {}

        for encoding in [enc for enc in encodings if enc is not None]:
            for delimiter in [delim for delim in delimiters if delim is not None]:
                candidate = _try_read_csv_candidate(file_path, encoding, delimiter)
                if candidate is None:
                    continue
                if candidate.score > best_score:
                    best_score = candidate.score
                    best_df = candidate.df
                    best_meta = {
                        "encoding": encoding,
                        "separator": delimiter,
                        "columns": list(candidate.df.columns),
                        "row_count": len(candidate.df),
                        "parsing_plan": plan.__dict__,
                    }
                    issues = candidate.issues[:]

        if best_df is not None:
            return ReadResult(
                file_path=str(path),
                file_type="csv",
                success=True,
                data=best_df,
                issues=issues,
                metadata=best_meta,
            )

    if plan.strategy == "drop_top_rows_then_parse":
        encoding = plan.encoding or "utf-8"
        delimiter = plan.delimiter or ","
        skiprows = max(plan.header_row_index, 0)
        header = 0

        df = pd.read_csv(
            file_path,
            encoding=encoding,
            sep=delimiter,
            skiprows=skiprows,
            header=header,
        )
        return ReadResult(
            file_path=str(path),
            file_type="csv",
            success=True,
            data=df,
            metadata={
                "encoding": encoding,
                "separator": delimiter,
                "columns": list(df.columns),
                "row_count": len(df),
                "parsing_plan": plan.__dict__,
            },
        )

    return ReadResult(
        file_path=str(path),
        file_type="csv",
        success=False,
        issues=[
            FileIssue(
                severity="error",
                message="ParsingPlan konnte für CSV nicht angewendet werden.",
            )
        ],
        metadata={"parsing_plan": plan.__dict__},
    )