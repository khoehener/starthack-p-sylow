from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from ingestion.file_parser.utils import _limit_text, _redact_value
from ingestion.file_parser.heuristics.csv_heuristics import _best_csv_candidate
from ingestion.file_parser.heuristics.excel_heuristics import _select_best_excel_sheet


def _preview_csv_lines(
    file_path: str,
    max_lines: int = 12,
) -> dict[str, Any]:
    from ingestion.file_parser.detection import CSV_ENCODINGS_TO_TRY

    previews: dict[str, list[str]] = {}

    for encoding in CSV_ENCODINGS_TO_TRY:
        try:
            with open(file_path, "r", encoding=encoding, errors="replace") as handle:
                lines = []
                for _ in range(max_lines):
                    line = handle.readline()
                    if not line:
                        break
                    lines.append(
                        _limit_text(_redact_value(line.rstrip("\n")), 200)
                    )
                previews[encoding] = lines
        except Exception:
            continue

    return {"raw_line_preview": previews}


def _preview_excel_sheet(
    file_path: str,
    sheet_name: str,
    max_rows: int = 8,
    max_cols: int = 8,
) -> dict[str, Any]:
    df = pd.read_excel(
        file_path,
        sheet_name=sheet_name,
        header=None,
        nrows=max_rows,
    )
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


def _build_limited_profile(
    file_path: str,
    file_type: str,
) -> dict[str, Any]:
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
                "sample_columns": [
                    _limit_text(str(col), 80)
                    for col in list(c.df.columns[:8])
                ],
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
                "sample_columns": [
                    _limit_text(str(col), 80)
                    for col in list(best_sheet.df.columns[:8])
                ],
            }
            profile["sheet_preview"] = _preview_excel_sheet(
                file_path,
                best_sheet.sheet_name,
            )
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