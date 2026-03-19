from __future__ import annotations

from pathlib import Path

from ingestion.file_parser.models import FileIssue, ParsingPlan, ReadResult
from ingestion.file_parser.heuristics.csv_heuristics import (
    _best_csv_candidate,
    _csv_is_ambiguous,
)
from ingestion.file_parser.llm.profile_builder import _build_limited_profile
from ingestion.file_parser.llm.claude_client import _call_claude_for_parsing_plan
from ingestion.file_parser.parsers.csv_parser import _apply_csv_parsing_plan


def read_csv_file(file_path: str) -> ReadResult:
    path = Path(file_path)

    # Step 1 - file must exist
    if not path.exists():
        return ReadResult(
            file_path=str(path),
            file_type="csv",
            success=False,
            issues=[
                FileIssue(
                    severity="error",
                    message="Datei existiert nicht.",
                )
            ],
        )

    # Step 2 - file must not be empty
    if path.stat().st_size == 0:
        return ReadResult(
            file_path=str(path),
            file_type="csv",
            success=False,
            issues=[
                FileIssue(
                    severity="error",
                    message="CSV-Datei ist leer.",
                )
            ],
        )

    # Step 3 - run heuristics
    best_candidate, candidates = _best_csv_candidate(file_path)

    # Step 4 - heuristic result is clear enough
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

    # Step 5 - heuristic was ambiguous, ask Claude
    file_profile = _build_limited_profile(file_path, "csv")
    llm_plan = _call_claude_for_parsing_plan(file_profile)

    # Step 6 - apply LLM plan if valid
    if llm_plan is not None:
        result = _apply_csv_parsing_plan(file_path, llm_plan)
        if result.success:
            result.issues.append(
                FileIssue(
                    severity="warning",
                    message=(
                        "CSV wurde mit Unterstützung eines LLM-generierten "
                        "ParsingPlans eingelesen. Ergebnis prüfen."
                    ),
                )
            )
            return result

    # Step 7 - LLM failed, fall back to best heuristic
    if best_candidate is not None:
        fallback_plan = ParsingPlan(
            strategy="direct_csv",
            source="heuristic",
            confidence=0.5,
            reasoning=(
                "LLM nicht verfügbar oder ohne gültigen Plan; "
                "bestes heuristisches Ergebnis verwendet."
            ),
            encoding=best_candidate.encoding,
            delimiter=best_candidate.delimiter,
        )
        result = _apply_csv_parsing_plan(file_path, fallback_plan)
        if result.success:
            result.issues.append(
                FileIssue(
                    severity="warning",
                    message=(
                        "CSV war mehrdeutig; bestes heuristisches Ergebnis "
                        "wurde ohne LLM-Plan verwendet."
                    ),
                )
            )
            return result

    # Step 8 - everything failed
    return ReadResult(
        file_path=str(path),
        file_type="csv",
        success=False,
        issues=[
            FileIssue(
                severity="error",
                message="CSV konnte nicht plausibel gelesen werden.",
            )
        ],
    )