from __future__ import annotations

import pandas as pd

from ingestion.file_parser.models import ExcelSheetCandidate
from ingestion.file_parser.detection import SUPPORTED_DELIMITERS
from ingestion.file_parser.heuristics.csv_heuristics import _score_dataframe_shape


def _score_excel_sheet(df: pd.DataFrame) -> tuple[float, str]:
    score, reasoning = _score_dataframe_shape(df)

    if len(df.columns) == 1:
        first_col_name = str(df.columns[0])
        if any(delimiter in first_col_name for delimiter in SUPPORTED_DELIMITERS):
            score += 8.0
            reasoning += "; Single-Column-Sheet mit eingebettetem Delimiter entdeckt"

    return score, reasoning


def _select_best_excel_sheet(
    file_path: str,
) -> tuple[list[str], ExcelSheetCandidate | None]:
    excel_file = pd.ExcelFile(file_path)
    sheet_names = excel_file.sheet_names
    best_candidate: ExcelSheetCandidate | None = None

    for sheet_name in sheet_names:
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        score, reasoning = _score_excel_sheet(df)
        candidate = ExcelSheetCandidate(
            sheet_name=sheet_name,
            df=df,
            score=score,
            reasoning=reasoning,
        )
        if best_candidate is None or candidate.score > best_candidate.score:
            best_candidate = candidate

    return sheet_names, best_candidate


def _excel_is_ambiguous(best_sheet: ExcelSheetCandidate | None) -> bool:
    if best_sheet is None:
        return True

    df = best_sheet.df
    if len(df.columns) == 1:
        return True

    if df.empty:
        return True

    return False