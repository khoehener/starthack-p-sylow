from __future__ import annotations

from pathlib import Path

from ingestion.file_parser.models import FileIssue, ParsingPlan, ReadResult
from ingestion.file_parser.detection import SUPPORTED_DELIMITERS
from ingestion.file_parser.heuristics.excel_heuristics import (
    _select_best_excel_sheet,
    _excel_is_ambiguous,
)
from ingestion.file_parser.llm.profile_builder import _build_limited_profile
from ingestion.file_parser.llm.claude_client import _call_claude_for_parsing_plan
from ingestion.file_parser.parsers.excel_parser import _apply_excel_parsing_plan


def read_excel_file(file_path: str) -> ReadResult:
    path = Path(file_path)

    # Step 1 - file must exist
    if not path.exists():
        return ReadResult(
            file_path=str(path),
            file_type="excel",
            success=False,
            issues=[
                FileIssue(
                    severity="error",
                    message="Datei existiert nicht.",
                )
            ],
        )

    # Step 2 - file must be openable
    try:
        sheet_names, best_sheet = _select_best_excel_sheet(file_path)
    except Exception as exc:
        return ReadResult(
            file_path=str(path),
            file_type="excel",
            success=False,
            issues=[
                FileIssue(
                    severity="error",
                    message=f"Excel konnte nicht gelesen werden: {exc}",
                )
            ],
        )

    # Step 3 - file must have sheets
    if not sheet_names:
        return ReadResult(
            file_path=str(path),
            file_type="excel",
            success=False,
            issues=[
                FileIssue(
                    severity="error",
                    message="Excel-Datei enthält keine Tabellenblätter.",
                )
            ],
        )

    # Step 4 - heuristic result is clear enough
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

    # Step 5 - heuristic was ambiguous, ask Claude
    file_profile = _build_limited_profile(file_path, "excel")
    llm_plan = _call_claude_for_parsing_plan(file_profile)

    # Step 6 - apply LLM plan if valid
    if llm_plan is not None:
        result = _apply_excel_parsing_plan(file_path, llm_plan)
        if result.success:
            result.issues.append(
                FileIssue(
                    severity="warning",
                    message=(
                        "Excel wurde mit Unterstützung eines LLM-generierten "
                        "ParsingPlans eingelesen. Ergebnis prüfen."
                    ),
                )
            )
            return result

    # Step 7 - LLM failed, fall back to best heuristic
    if best_sheet is not None:
        if len(best_sheet.df.columns) == 1:
            first_col = str(best_sheet.df.columns[0])
            detected_delimiter = next(
                (d for d in SUPPORTED_DELIMITERS if d in first_col), ","
            )
            fallback_plan = ParsingPlan(
                strategy="split_embedded_delimited_column",
                source="heuristic",
                confidence=0.5,
                reasoning=(
                    "LLM nicht verfügbar oder ohne gültigen Plan; "
                    "eingebettete delimiter-basierte Spalte heuristisch aufgeteilt."
                ),
                sheet_name=best_sheet.sheet_name,
                delimiter=detected_delimiter,
                target_columns=[0],
            )
        else:
            fallback_plan = ParsingPlan(
                strategy="direct_excel",
                source="heuristic",
                confidence=0.5,
                reasoning=(
                    "LLM nicht verfügbar oder ohne gültigen Plan; "
                    "bestes heuristisches Sheet direkt verwendet."
                ),
                sheet_name=best_sheet.sheet_name,
                header_row_index=0,
                data_start_row_index=1,
            )

        result = _apply_excel_parsing_plan(file_path, fallback_plan)
        if result.success:
            result.issues.append(
                FileIssue(
                    severity="warning",
                    message=(
                        "Excel-Struktur war mehrdeutig; "
                        "heuristischer Fallback wurde verwendet."
                    ),
                )
            )
            return result

    # Step 8 - everything failed
    return ReadResult(
        file_path=str(path),
        file_type="excel",
        success=False,
        issues=[
            FileIssue(
                severity="error",
                message="Excel konnte nicht plausibel gelesen werden.",
            )
        ],
    )