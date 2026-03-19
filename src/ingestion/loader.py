# src/ingestion/loader.py
from __future__ import annotations

import tempfile
import requests
import pandas as pd
from dataclasses import dataclass, field
from typing import Literal

from data_sources import DATASETS, DATASETS_CLINIC, DATASETS_ERROR
from ingestion.file_parser.readers.csv_reader import read_csv_file
from ingestion.file_parser.readers.excel_reader import read_excel_file


# ─────────────────────────────────────────
# Result model
# ─────────────────────────────────────────

@dataclass
class LoadResult:
    name: str
    url: str
    success: bool
    data: pd.DataFrame | None = None
    error: str | None = None
    metadata: dict = field(default_factory=dict)


# ─────────────────────────────────────────
# Core loader
# ─────────────────────────────────────────

def _load_single(name: str, url: str) -> LoadResult:
    """Download file, save to temp, pass to file_parser."""

    if url.endswith(".pdf"):
        return LoadResult(
            name=name,
            url=url,
            success=False,
            error="PDF not supported yet - skipped.",
        )

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        return LoadResult(
            name=name,
            url=url,
            success=False,
            error=f"Download failed: {e}",
        )

    suffix = ".xlsx" if url.endswith(".xlsx") else ".csv"

    try:
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            tmp.write(response.content)
            tmp_path = tmp.name

        if suffix == ".csv":
            result = read_csv_file(tmp_path)
        else:
            result = read_excel_file(tmp_path)

        if result.success:
            return LoadResult(
                name=name,
                url=url,
                success=True,
                data=result.data,
                metadata=result.metadata,
            )
        else:
            return LoadResult(
                name=name,
                url=url,
                success=False,
                error=str([i.message for i in result.issues]),
            )

    except Exception as e:
        return LoadResult(
            name=name,
            url=url,
            success=False,
            error=f"Parsing failed: {e}",
        )


# ─────────────────────────────────────────
# Batch loader
# ─────────────────────────────────────────

DatasetGroup = Literal["standard", "clinic", "error", "all"]


def load_all(
    group: DatasetGroup = "standard",
    verbose: bool = True,
) -> dict[str, pd.DataFrame]:

    sources: dict[str, str] = {}
    if group in ("standard", "all"):
        sources.update(DATASETS)
    if group in ("clinic", "all"):
        sources.update(DATASETS_CLINIC)
    if group in ("error", "all"):
        sources.update(DATASETS_ERROR)

    results: list[LoadResult] = []

    for name, url in sources.items():
        if verbose:
            print(f"  Loading [{name}]...", end=" ")

        result = _load_single(name, url)
        results.append(result)

        if verbose:
            if result.success:
                print(f"✓  {result.metadata.get('row_count')} rows, "
                      f"{len(result.metadata.get('columns', []))} cols")
            else:
                print(f"✗  {result.error}")

    succeeded = [r for r in results if r.success]
    failed    = [r for r in results if not r.success]

    if verbose:
        print(f"\n{'─'*50}")
        print(f"Loaded:  {len(succeeded)}/{len(results)} files")
        if failed:
            print("Failed:")
            for r in failed:
                print(f"  - [{r.name}] {r.error}")
        print(f"{'─'*50}\n")

    return {r.name: r.data for r in succeeded}

if __name__ == "__main__":
    dataframes = load_all(group="standard", verbose=True)
    
    print("\n=== COLUMN OVERVIEW ===")
    for name, df in dataframes.items():
        print(f"\n[{name}]")
        print(df.columns.tolist())