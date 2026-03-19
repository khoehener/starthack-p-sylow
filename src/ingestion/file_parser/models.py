from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

import pandas as pd


@dataclass
class FileIssue:
    severity: str
    message: str


@dataclass
class ReadResult:
    file_path: str
    file_type: str
    success: bool
    data: pd.DataFrame | None = None
    issues: list[FileIssue] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ParsingPlan:
    strategy: Literal[
        "direct_csv",
        "direct_excel",
        "split_embedded_delimited_column",
        "drop_top_rows_then_parse",
    ]
    source: Literal["heuristic", "llm"]
    confidence: float
    reasoning: str
    encoding: str | None = None
    delimiter: str | None = None
    sheet_name: str | None = None
    header_row_index: int = 0
    data_start_row_index: int = 1
    target_columns: list[int] | None = None


@dataclass
class CsvCandidate:
    df: pd.DataFrame
    encoding: str
    delimiter: str
    score: float
    reasoning: str
    column_count: int
    issues: list[FileIssue] = field(default_factory=list)


@dataclass
class ExcelSheetCandidate:
    sheet_name: str
    df: pd.DataFrame
    score: float
    reasoning: str