# utils.py
from __future__ import annotations

import re
from typing import Any


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