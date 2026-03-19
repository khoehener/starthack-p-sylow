from __future__ import annotations

from dataclasses import dataclass, field
from io import StringIO
from pathlib import Path
from typing import Any, Literal
import csv
import json
import os
import re

import pandas as pd

try:
    import anthropic
except ImportError:  # pragma: no cover - optional dependency
    anthropic = None


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


# -----------------------------
# Kleine allgemeine Hilfsfunktionen
# -----------------------------
def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _limit_text(text: str, max_len: int = 120) -> str:
    text = text.replace("\n", " ").replace("\r", " ")
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def _redact_value(value: Any) -> str:
    """
    Optionale leichte Redaktion für den LLM-Preview.
    - Zahlenfolgen werden grob maskiert
    - E-Mails werden maskiert
    - Text wird gekürzt
    """
    text = str(value)
    text = re.sub(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+", "<EMAIL>", text)
    text = re.sub(r"\b\d{3,}\b", "<NUM>", text)
    return _limit_text(text)


def _sample_text_lines(file_path: str, encoding: str, max_lines: int = 8) -> list[str]:
    try:
        with open(file_path, "r", encoding=encoding, errors="replace") as handle:
            lines: list[str] = []
            for _ in range(max_lines):
                line = handle.readline()
                if not line:
                    break
                line = line.rstrip("\n")
                if line.strip():
                    lines.append(line)
            return lines
    except Exception:
        return []


def _delimiter_consistency_bonus(lines: list[str], delimiter: str) -> tuple[float, str]:
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


# -----------------------------
# CSV-Heuristiken
# -----------------------------
@dataclass
class CsvCandidate:
    df: pd.DataFrame
    encoding: str
    delimiter: str
    score: float
    reasoning: str
    column_count: int
    issues: list[FileIssue] = field(default_factory=list)


def _score_dataframe_shape(df: pd.DataFrame, expected_delimiter: str | None = None) -> tuple[float, str]:
    """
    Bewertet grob, wie plausibel ein DataFrame aussieht.
    Höherer Score = wahrscheinlicher korrekt geparst.
    """
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

    non_empty_ratio = 1.0 - _safe_float(df.isna().mean().mean(), 0.0)
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
            reasons.append(f"{leftover_delimiters} Spaltennamen enthalten weiterhin den gewählten Delimiter")

        other_delimiters = [d for d in SUPPORTED_DELIMITERS if d != expected_delimiter]
        suspicious_other_delims = sum(any(d in str(col) for d in other_delimiters) for col in df.columns)
        if suspicious_other_delims:
            score -= suspicious_other_delims * 1.5
            reasons.append(f"{suspicious_other_delims} Spaltennamen enthalten andere Delimiter")

    return score, "; ".join(reasons)


def _try_read_csv_candidate(file_path: str, encoding: str, delimiter: str) -> CsvCandidate | None:
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
        issues.append(FileIssue(severity="warning", message="CSV wurde gelesen, enthält aber keine Datenzeilen."))

    return CsvCandidate(
        df=df,
        encoding=encoding,
        delimiter=delimiter,
        score=score,
        reasoning=reasoning,
        column_count=len(df.columns),
        issues=issues,
    )


def _best_csv_candidate(file_path: str) -> tuple[CsvCandidate | None, list[CsvCandidate]]:
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


# -----------------------------
# Excel-Heuristiken
# -----------------------------
def _score_excel_sheet(df: pd.DataFrame) -> tuple[float, str]:
    score, reasoning = _score_dataframe_shape(df)

    # Spezieller Fall: eigentlich CSV-Text in einer Spalte
    if len(df.columns) == 1:
        first_col_name = str(df.columns[0])
        if any(delimiter in first_col_name for delimiter in SUPPORTED_DELIMITERS):
            score += 8.0
            reasoning += "; Single-Column-Sheet mit eingebettetem Delimiter entdeckt"

    return score, reasoning


@dataclass
class ExcelSheetCandidate:
    sheet_name: str
    df: pd.DataFrame
    score: float
    reasoning: str


def _select_best_excel_sheet(file_path: str) -> tuple[list[str], ExcelSheetCandidate | None]:
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


# -----------------------------
# LLM-Vorschau / ParsingPlan via Claude
# -----------------------------
def _preview_csv_lines(file_path: str, max_lines: int = 12) -> dict[str, Any]:
    previews: dict[str, list[str]] = {}

    for encoding in CSV_ENCODINGS_TO_TRY:
        try:
            with open(file_path, "r", encoding=encoding, errors="replace") as handle:
                lines = []
                for _ in range(max_lines):
                    line = handle.readline()
                    if not line:
                        break
                    lines.append(_limit_text(_redact_value(line.rstrip("\n")), 200))
                previews[encoding] = lines
        except Exception:
            continue

    return {"raw_line_preview": previews}


def _preview_excel_sheet(file_path: str, sheet_name: str, max_rows: int = 8, max_cols: int = 8) -> dict[str, Any]:
    df = pd.read_excel(file_path, sheet_name=sheet_name, header=None, nrows=max_rows)
    clipped = df.iloc[:, :max_cols].fillna("")
    preview_rows = [
        [_redact_value(value) for value in row]
        for row in clipped.values.tolist()
    ]

    return {
        "sheet_name": sheet_name,
        "preview_rows": preview_rows,
        "observed_column_count": int(df.shape[1]),
        "observed_row_count": int(df.shape[0]),
    }


def _build_limited_profile(file_path: str, file_type: str) -> dict[str, Any]:
    profile: dict[str, Any] = {
        "file_name": Path(file_path).name,
        "file_type": file_type,
        "file_size_bytes": Path(file_path).stat().st_size,
    }

    if file_type == "csv":
        best_candidate, candidates = _best_csv_candidate(file_path)
        profile["csv_candidates"] = [
            {
                "encoding": c.encoding,
                "delimiter": c.delimiter,
                "score": round(c.score, 2),
                "column_count": c.column_count,
                "reasoning": c.reasoning,
                "sample_columns": [_limit_text(str(col), 80) for col in list(c.df.columns[:8])],
            }
            for c in candidates[:6]
        ]
        if best_candidate is not None:
            profile["heuristic_best"] = {
                "encoding": best_candidate.encoding,
                "delimiter": best_candidate.delimiter,
                "score": round(best_candidate.score, 2),
                "column_count": best_candidate.column_count,
            }
        profile.update(_preview_csv_lines(file_path))
        return profile

    if file_type == "excel":
        sheet_names, best_sheet = _select_best_excel_sheet(file_path)
        profile["sheet_names"] = sheet_names
        if best_sheet is not None:
            profile["heuristic_best_sheet"] = {
                "sheet_name": best_sheet.sheet_name,
                "score": round(best_sheet.score, 2),
                "reasoning": best_sheet.reasoning,
                "shape": list(best_sheet.df.shape),
                "sample_columns": [_limit_text(str(col), 80) for col in list(best_sheet.df.columns[:8])],
            }
            profile["sheet_preview"] = _preview_excel_sheet(file_path, best_sheet.sheet_name)
        return profile

    return profile


def _build_llm_prompt(file_profile: dict[str, Any]) -> str:
    return (
        "Du analysierst Dateistrukturen für einen Parser. "
        "Du darfst NUR einen JSON-Block zurückgeben. Kein Markdown, kein Fließtext.\n\n"
        "Wähle genau eine bekannte Strategie:\n"
        "- direct_csv\n"
        "- direct_excel\n"
        "- split_embedded_delimited_column\n"
        "- drop_top_rows_then_parse\n\n"
        "Regeln:\n"
        "1. Erfinde keine Informationen.\n"
        "2. Gib nur Felder zurück, die wirklich nötig sind.\n"
        "3. confidence ist eine Zahl zwischen 0 und 1.\n"
        "4. sheet_name nur bei Excel setzen.\n"
        "5. delimiter nur setzen, wenn relevant. Nutze genau einen von: ',', ';', '\\t', '|'.\n"
        "6. header_row_index und data_start_row_index als Integer setzen.\n\n"
        "Erwartetes JSON-Schema:\n"
        "{\n"
        '  "strategy": "direct_csv | direct_excel | split_embedded_delimited_column | drop_top_rows_then_parse",\n'
        '  "confidence": 0.0,\n'
        '  "reasoning": "kurze Begründung",\n'
        '  "encoding": "optional",\n'
        '  "delimiter": "optional",\n'
        '  "sheet_name": "optional",\n'
        '  "header_row_index": 0,\n'
        '  "data_start_row_index": 1,\n'
        '  "target_columns": [0]\n'
        "}\n\n"
        "Dateiprofil:\n"
        f"{json.dumps(file_profile, ensure_ascii=False, indent=2)}"
    )


def _extract_text_from_claude_response(message: Any) -> str:
    content_blocks = getattr(message, "content", [])
    texts: list[str] = []
    for block in content_blocks:
        block_type = getattr(block, "type", None)
        if block_type == "text":
            texts.append(getattr(block, "text", ""))
    return "\n".join(texts).strip()


def _call_claude_for_parsing_plan(
    file_profile: dict[str, Any],
    model: str | None = None,
) -> ParsingPlan | None:
    if anthropic is None:
        return None

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return None

    client = anthropic.Anthropic(api_key=api_key)
    prompt = _build_llm_prompt(file_profile)
    chosen_model = model or os.getenv("CLAUDE_MODEL", "claude-sonnet-4-6")

    message = client.messages.create(
        model=chosen_model,
        max_tokens=600,
        temperature=0,
        messages=[{"role": "user", "content": prompt}],
    )
    text = _extract_text_from_claude_response(message)

    if not text:
        return None

    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return None

    strategy = payload.get("strategy")
    if strategy not in {
        "direct_csv",
        "direct_excel",
        "split_embedded_delimited_column",
        "drop_top_rows_then_parse",
    }:
        return None

    delimiter = payload.get("delimiter")
    if delimiter not in {None, ",", ";", "\t", "|"}:
        delimiter = None

    target_columns = payload.get("target_columns")
    if not isinstance(target_columns, list) or not all(isinstance(v, int) for v in target_columns):
        target_columns = None

    return ParsingPlan(
        strategy=strategy,
        source="llm",
        confidence=max(0.0, min(1.0, _safe_float(payload.get("confidence"), 0.0))),
        reasoning=str(payload.get("reasoning", ""))[:500],
        encoding=payload.get("encoding"),
        delimiter=delimiter,
        sheet_name=payload.get("sheet_name"),
        header_row_index=int(payload.get("header_row_index", 0)),
        data_start_row_index=int(payload.get("data_start_row_index", 1)),
        target_columns=target_columns,
    )


# -----------------------------
# LLM nur bei unklaren Fällen fragen
# -----------------------------
def _csv_is_ambiguous(best_candidate: CsvCandidate | None, candidates: list[CsvCandidate]) -> bool:
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


def _excel_is_ambiguous(best_sheet: ExcelSheetCandidate | None) -> bool:
    if best_sheet is None:
        return True

    df = best_sheet.df
    if len(df.columns) == 1:
        return True

    if df.empty:
        return True

    return False


# -----------------------------
# ParsingPlan anwenden
# -----------------------------
def _apply_csv_parsing_plan(file_path: str, plan: ParsingPlan) -> ReadResult:
    path = Path(file_path)
    issues: list[FileIssue] = []

    if plan.strategy == "direct_csv":
        encodings = [plan.encoding] if plan.encoding else CSV_ENCODINGS_TO_TRY
        delimiters = [plan.delimiter] if plan.delimiter else SUPPORTED_DELIMITERS

        best_df: pd.DataFrame | None = None
        best_score = float("-inf")
        best_meta: dict[str, Any] = {}

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
        df = pd.read_csv(file_path, encoding=encoding, sep=delimiter, skiprows=skiprows, header=header)
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
        issues=[FileIssue(severity="error", message="ParsingPlan konnte für CSV nicht angewendet werden.")],
        metadata={"parsing_plan": plan.__dict__},
    )


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


def _apply_excel_parsing_plan(file_path: str, plan: ParsingPlan) -> ReadResult:
    path = Path(file_path)
    issues: list[FileIssue] = []
    excel_file = pd.ExcelFile(file_path)
    sheet_names = excel_file.sheet_names
    sheet_name = plan.sheet_name or sheet_names[0]

    if sheet_name not in sheet_names:
        sheet_name = sheet_names[0]
        issues.append(FileIssue(severity="warning", message="Plan-Sheet nicht gefunden, erstes Sheet wurde verwendet."))

    if plan.strategy == "direct_excel":
        df = pd.read_excel(file_path, sheet_name=sheet_name, header=plan.header_row_index)
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
        rebuilt_df = _rebuild_table_from_embedded_column(raw_df, delimiter=delimiter, target_column=target_column)
        issues.append(
            FileIssue(
                severity="warning",
                message="Excel-Datei enthielt eingebetteten delimiter-basierten Text und wurde über ParsingPlan in Spalten aufgeteilt.",
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
        raw_df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
        header_row_index = max(plan.header_row_index, 0)
        if header_row_index >= len(raw_df):
            return ReadResult(
                file_path=str(path),
                file_type="excel",
                success=False,
                issues=[FileIssue(severity="error", message="ParsingPlan verweist auf eine Header-Zeile außerhalb des Sheets.")],
                metadata={"parsing_plan": plan.__dict__},
            )
        header = raw_df.iloc[header_row_index].astype(str).tolist()
        data_df = raw_df.iloc[plan.data_start_row_index :].reset_index(drop=True)
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
        issues=[FileIssue(severity="error", message="ParsingPlan konnte für Excel nicht angewendet werden.")],
        metadata={"parsing_plan": plan.__dict__},
    )


# -----------------------------
# Öffentliche Reader-Funktionen
# -----------------------------
def read_csv_file(file_path: str) -> ReadResult:
    path = Path(file_path)

    if not path.exists():
        return ReadResult(
            file_path=str(path),
            file_type="csv",
            success=False,
            issues=[FileIssue(severity="error", message="Datei existiert nicht.")],
        )

    if path.stat().st_size == 0:
        return ReadResult(
            file_path=str(path),
            file_type="csv",
            success=False,
            issues=[FileIssue(severity="error", message="CSV-Datei ist leer.")],
        )

    best_candidate, candidates = _best_csv_candidate(file_path)

    if best_candidate is not None and not _csv_is_ambiguous(best_candidate, candidates):
        plan = ParsingPlan(
            strategy="direct_csv",
            source="heuristic",
            confidence=0.9,
            reasoning=best_candidate.reasoning,
            encoding=best_candidate.encoding,
            delimiter=best_candidate.delimiter,
        )
        return _apply_csv_parsing_plan(file_path, plan)

    file_profile = _build_limited_profile(file_path, "csv")
    llm_plan = _call_claude_for_parsing_plan(file_profile)

    if llm_plan is not None:
        result = _apply_csv_parsing_plan(file_path, llm_plan)
        if result.success:
            result.issues.append(
                FileIssue(
                    severity="warning",
                    message="CSV wurde mit Unterstützung eines LLM-generierten ParsingPlans eingelesen. Ergebnis prüfen.",
                )
            )
            return result

    if best_candidate is not None:
        fallback_plan = ParsingPlan(
            strategy="direct_csv",
            source="heuristic",
            confidence=0.5,
            reasoning="LLM nicht verfügbar oder ohne gültigen Plan; bestes heuristisches Ergebnis verwendet.",
            encoding=best_candidate.encoding,
            delimiter=best_candidate.delimiter,
        )
        result = _apply_csv_parsing_plan(file_path, fallback_plan)
        if result.success:
            result.issues.append(
                FileIssue(
                    severity="warning",
                    message="CSV war mehrdeutig; bestes heuristisches Ergebnis wurde ohne LLM-Plan verwendet.",
                )
            )
            return result

    return ReadResult(
        file_path=str(path),
        file_type="csv",
        success=False,
        issues=[FileIssue(severity="error", message="CSV konnte nicht plausibel gelesen werden.")],
    )


def read_excel_file(file_path: str) -> ReadResult:
    path = Path(file_path)

    if not path.exists():
        return ReadResult(
            file_path=str(path),
            file_type="excel",
            success=False,
            issues=[FileIssue(severity="error", message="Datei existiert nicht.")],
        )

    try:
        sheet_names, best_sheet = _select_best_excel_sheet(file_path)
    except Exception as exc:
        return ReadResult(
            file_path=str(path),
            file_type="excel",
            success=False,
            issues=[FileIssue(severity="error", message=f"Excel konnte nicht gelesen werden: {exc}")],
        )

    if not sheet_names:
        return ReadResult(
            file_path=str(path),
            file_type="excel",
            success=False,
            issues=[FileIssue(severity="error", message="Excel-Datei enthält keine Tabellenblätter.")],
        )

    if best_sheet is not None and not _excel_is_ambiguous(best_sheet):
        plan = ParsingPlan(
            strategy="direct_excel",
            source="heuristic",
            confidence=0.9,
            reasoning=best_sheet.reasoning,
            sheet_name=best_sheet.sheet_name,
            header_row_index=0,
            data_start_row_index=1,
        )
        return _apply_excel_parsing_plan(file_path, plan)

    file_profile = _build_limited_profile(file_path, "excel")
    llm_plan = _call_claude_for_parsing_plan(file_profile)

    if llm_plan is not None:
        result = _apply_excel_parsing_plan(file_path, llm_plan)
        if result.success:
            result.issues.append(
                FileIssue(
                    severity="warning",
                    message="Excel wurde mit Unterstützung eines LLM-generierten ParsingPlans eingelesen. Ergebnis prüfen.",
                )
            )
            return result

    if best_sheet is not None:
        if len(best_sheet.df.columns) == 1:
            first_col = str(best_sheet.df.columns[0])
            detected_delimiter = next((d for d in SUPPORTED_DELIMITERS if d in first_col), ",")
            fallback_plan = ParsingPlan(
                strategy="split_embedded_delimited_column",
                source="heuristic",
                confidence=0.5,
                reasoning="LLM nicht verfügbar oder ohne gültigen Plan; eingebettete delimiter-basierte Spalte heuristisch aufgeteilt.",
                sheet_name=best_sheet.sheet_name,
                delimiter=detected_delimiter,
                target_columns=[0],
            )
        else:
            fallback_plan = ParsingPlan(
                strategy="direct_excel",
                source="heuristic",
                confidence=0.5,
                reasoning="LLM nicht verfügbar oder ohne gültigen Plan; bestes heuristisches Sheet direkt verwendet.",
                sheet_name=best_sheet.sheet_name,
                header_row_index=0,
                data_start_row_index=1,
            )

        result = _apply_excel_parsing_plan(file_path, fallback_plan)
        if result.success:
            result.issues.append(
                FileIssue(
                    severity="warning",
                    message="Excel-Struktur war mehrdeutig; heuristischer Fallback wurde verwendet.",
                )
            )
            return result

    return ReadResult(
        file_path=str(path),
        file_type="excel",
        success=False,
        issues=[FileIssue(severity="error", message="Excel konnte nicht plausibel gelesen werden.")],
    )


def read_pdf_file(file_path: str) -> ReadResult:
    return ReadResult(
        file_path=file_path,
        file_type="pdf",
        success=False,
        issues=[FileIssue(severity="warning", message="PDF-Parsing ist noch nicht implementiert.")],
    )


def read_file(file_path: str) -> ReadResult:
    file_type = detect_file_type(file_path)

    if file_type == "csv":
        return read_csv_file(file_path)

    if file_type == "excel":
        return read_excel_file(file_path)

    if file_type == "pdf":
        return read_pdf_file(file_path)

    return ReadResult(
        file_path=file_path,
        file_type="unknown",
        success=False,
        issues=[FileIssue(severity="error", message="Nicht unterstützter Dateityp.")],
    )
