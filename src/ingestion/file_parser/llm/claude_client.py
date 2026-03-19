from __future__ import annotations

import json
import os
from typing import Any

from ingestion.file_parser.models import ParsingPlan
from ingestion.file_parser.utils import _safe_float
from ingestion.file_parser.llm.profile_builder import _build_llm_prompt

try:
    import anthropic
except ImportError:
    anthropic = None


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

    try:
        client = anthropic.Anthropic(api_key=api_key)
        prompt = _build_llm_prompt(file_profile)
        chosen_model = model or os.getenv("CLAUDE_MODEL", "claude-sonnet-4-5")

        message = client.messages.create(
            model=chosen_model,
            max_tokens=600,
            temperature=0,
            messages=[{"role": "user", "content": prompt}],
        )
        text = _extract_text_from_claude_response(message)

    except Exception as e:
        print(f"[Claude ERROR] {type(e).__name__}: {e}")
        return None

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
    if not isinstance(target_columns, list) or not all(
        isinstance(v, int) for v in target_columns
    ):
        target_columns = None

    return ParsingPlan(
        strategy=strategy,
        source="llm",
        confidence=max(
            0.0,
            min(1.0, _safe_float(payload.get("confidence"), 0.0)),
        ),
        reasoning=str(payload.get("reasoning", ""))[:500],
        encoding=payload.get("encoding"),
        delimiter=delimiter,
        sheet_name=payload.get("sheet_name"),
        header_row_index=int(payload.get("header_row_index", 0)),
        data_start_row_index=int(payload.get("data_start_row_index", 1)),
        target_columns=target_columns,
    )