from __future__ import annotations

import pandas as pd

from ingestion.file_parser.models import CsvCandidate, FileIssue
from ingestion.file_parser.detection import SUPPORTED_DELIMITERS, CSV_ENCODINGS_TO_TRY
from ingestion.file_parser.utils import _sample_text_lines


def _score_dataframe_shape(
    df: pd.DataFrame,
    expected_delimiter: str | None = None,
) -> tuple[float, str]:
    if df.empty:
        return -100.0, "DataFrame ist leer"

    column_count = len(df.columns)
    row_count = len(df)
    first_column_name = str(df.columns[0]) if column_count else ""

    score = 0.0
    reasons: list[str] = []

    score += min(column_count * 5.0, 40.0)
    reasons.append(f"{column_count} Spalten erkannt")

    score += min(row_count / 50.0, 10.0)
    reasons.append(f"{row_count} Zeilen erkannt")

    if column_count == 1 and any(char in first_column_name for char in SUPPORTED_DELIMITERS):
        score -= 50.0
        reasons.append("nur 1 Spalte und Header enthält weiterhin Trennzeichen")

    unnamed_count = sum(str(col).startswith("Unnamed:") for col in df.columns)
    if unnamed_count:
        unnamed_ratio = unnamed_count / max(column_count, 1)
        score -= min(unnamed_ratio * 35.0, 30.0)
        reasons.append(f"{unnamed_count} Unnamed-Spalten (Quote {unnamed_ratio:.2f})")

    non_empty_ratio = 1.0 - float(df.isna().mean().mean())
    score += non_empty_ratio * 15.0
    reasons.append(f"Nichtleer-Quote {non_empty_ratio:.2f}")

    unique_column_names = len(set(map(str, df.columns)))
    if unique_column_names != column_count:
        score -= (column_count - unique_column_names) * 2.0
        reasons.append("doppelte Spaltennamen")

    if expected_delimiter is not None:
        leftover_delimiters = sum(expected_delimiter in str(col) for col in df.columns)
        if leftover_delimiters:
            score -= leftover_delimiters * 4.0
            reasons.append(
                f"{leftover_delimiters} Spaltennamen enthalten weiterhin den gewählten Delimiter"
            )

        other_delimiters = [d for d in SUPPORTED_DELIMITERS if d != expected_delimiter]
        suspicious_other_delims = sum(
            any(d in str(col) for d in other_delimiters) for col in df.columns
        )
        if suspicious_other_delims:
            score -= suspicious_other_delims * 1.5
            reasons.append(f"{suspicious_other_delims} Spaltennamen enthalten andere Delimiter")

    return score, "; ".join(reasons)


def _delimiter_consistency_bonus(
    lines: list[str],
    delimiter: str,
) -> tuple[float, str]:
    if not lines:
        return 0.0, "keine Textvorschau verfügbar"

    counts = [line.count(delimiter) for line in lines]
    avg = sum(counts) / len(counts)
    max_count = max(counts)
    min_count = min(counts)

    if max_count == 0:
        return -25.0, f"Delimiter {repr(delimiter)} kommt in der Vorschau nicht vor"

    spread = max_count - min_count
    bonus = min(avg / 3.0, 40.0) - min(spread / 5.0, 10.0)
    return bonus, f"Delimiter-Vorkommen Ø {avg:.1f}, Spread {spread}"


def _try_read_csv_candidate(
    file_path: str,
    encoding: str,
    delimiter: str,
) -> CsvCandidate | None:
    try:
        df = pd.read_csv(file_path, encoding=encoding, sep=delimiter)
    except Exception:
        return None

    score, reasoning = _score_dataframe_shape(df, expected_delimiter=delimiter)

    raw_lines = _sample_text_lines(file_path, encoding=encoding, max_lines=8)
    delimiter_bonus, delimiter_reason = _delimiter_consistency_bonus(raw_lines, delimiter)
    score += delimiter_bonus
    reasoning = reasoning + "; " + delimiter_reason

    issues: list[FileIssue] = []
    if df.empty:
        issues.append(
            FileIssue(
                severity="warning",
                message="CSV wurde gelesen, enthält aber keine Datenzeilen.",
            )
        )

    return CsvCandidate(
        df=df,
        encoding=encoding,
        delimiter=delimiter,
        score=score,
        reasoning=reasoning,
        column_count=len(df.columns),
        issues=issues,
    )


def _best_csv_candidate(
    file_path: str,
) -> tuple[CsvCandidate | None, list[CsvCandidate]]:
    candidates: list[CsvCandidate] = []

    for encoding in CSV_ENCODINGS_TO_TRY:
        for delimiter in SUPPORTED_DELIMITERS:
            candidate = _try_read_csv_candidate(file_path, encoding, delimiter)
            if candidate is not None:
                candidates.append(candidate)

    if not candidates:
        return None, []

    candidates.sort(key=lambda item: item.score, reverse=True)
    return candidates[0], candidates


def _csv_is_ambiguous(
    best_candidate: CsvCandidate | None,
    candidates: list[CsvCandidate],
) -> bool:
    if best_candidate is None:
        return True

    if len(candidates) < 2:
        return best_candidate.column_count <= 1

    second_best = candidates[1]

    if best_candidate.column_count <= 1:
        return True

    if abs(best_candidate.score - second_best.score) < 6.0:
        return True

    return False