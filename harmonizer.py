# src/harmonizer.py
from __future__ import annotations

import json
import os
import sqlite3
from typing import Any

import anthropic
import pandas as pd


# ─────────────────────────────────────────
# Step 1 - Ask Claude to map all columns
# ─────────────────────────────────────────

def _build_mapping_prompt(dataframes: dict[str, pd.DataFrame]) -> str:
    """Build a prompt that shows Claude all dataframes and their columns."""

    lines = [
        "You are a healthcare data integration expert.",
        "I have the following dataframes that all relate to hospital patients and cases.",
        "Your job is to:",
        "1. Find which column in each dataframe represents the CASE ID and PATIENT ID",
        "2. Map every column to a unified canonical name in English (snake_case)",
        "3. Identify what TYPE of data each dataframe contains",
        "",
        "Here are the dataframes and their columns:",
        "",
    ]

    for name, df in dataframes.items():
        # send first 2 rows as sample so Claude understands the data
        sample = df.head(2).to_dict(orient="records")
        lines.append(f"=== {name} ===")
        lines.append(f"Columns: {df.columns.tolist()}")
        lines.append(f"Sample rows: {json.dumps(sample, default=str)}")
        lines.append("")

    lines += [
        "Return a JSON object with this exact structure:",
        "{",
        '  "dataframe_roles": {',
        '    "<df_name>": {',
        '      "case_id_column": "<column name or null>",',
        '      "patient_id_column": "<column name or null>",',
        '      "data_type": "<one of: core_cases, labs, medication, nursing, epa_assessment, motion, other>",',
        '      "column_mapping": {',
        '        "<original_column>": "<canonical_name>",',
        '        ...',
        "      }",
        "    }",
        "  }",
        "}",
        "",
        "Rules:",
        "- canonical names must be snake_case English",
        "- dates should map to names ending in _date or _datetime",
        "- if a column is useless (Unnamed, base64 garbage) map it to null",
        "- case_id and patient_id should always map to exactly 'case_id' and 'patient_id'",
        "- be consistent: same concept = same canonical name across all dataframes",
        "- only return valid JSON, no explanation",
    ]

    return "\n".join(lines)


def _call_claude_for_mapping(
    dataframes: dict[str, pd.DataFrame],
    model: str | None = None,
) -> dict | None:
    """Ask Claude to return a full column mapping for all dataframes."""

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY not set")

    client = anthropic.Anthropic(api_key=api_key)
    prompt = _build_mapping_prompt(dataframes)
    chosen_model = model or os.getenv("CLAUDE_MODEL", "claude-sonnet-4-5")

    print("  Asking Claude to map all columns...")

    message = client.messages.create(
        model=chosen_model,
        max_tokens=8000,        # we need a lot of tokens - there are 500+ columns
        temperature=0,
        messages=[{"role": "user", "content": prompt}],
    )

    # extract text
    text = ""
    for block in getattr(message, "content", []):
        if getattr(block, "type", None) == "text":
            text += getattr(block, "text", "")
    text = text.strip()

    # strip markdown if wrapped
    if text.startswith("```"):
        lines = text.splitlines()
        text = "\n".join(lines[1:-1]).strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        print(f"  Claude returned invalid JSON: {e}")
        print(f"  Raw response: {text[:500]}")
        return None


# ─────────────────────────────────────────
# Step 2 - Apply mapping to dataframes
# ─────────────────────────────────────────

def _apply_mapping(
    dataframes: dict[str, pd.DataFrame],
    mapping: dict,
) -> dict[str, pd.DataFrame]:
    """
    Rename columns according to Claude's mapping.
    Drop columns mapped to null.
    Add case_id / patient_id where Claude found them.
    """
    roles = mapping.get("dataframe_roles", {})
    result = {}

    for name, df in dataframes.items():
        if name not in roles:
            print(f"  [{name}] not in mapping - skipping")
            continue

        role_info = roles[name]
        col_map: dict = role_info.get("column_mapping", {})

        # build rename dict - skip nulls
        rename = {
            orig: canon
            for orig, canon in col_map.items()
            if canon is not None and orig in df.columns
        }

        # drop columns mapped to null
        drop = [
            orig for orig, canon in col_map.items()
            if canon is None and orig in df.columns
        ]

        df = df.drop(columns=drop, errors="ignore")
        df = df.rename(columns=rename)

        # tag with source so we know where data came from
        df["_source"] = name
        df["_data_type"] = role_info.get("data_type", "unknown")

        result[name] = df
        print(f"  [{name}] mapped {len(rename)} columns, dropped {len(drop)}")

    return result


# ─────────────────────────────────────────
# Step 3 - Merge everything by case_id
# ─────────────────────────────────────────

def _merge_by_case(
    mapped_dfs: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """
    Use cases as master table.
    Merge everything else onto it by case_id or patient_id.
    One-to-many tables (labs, medication etc) are kept as separate tables in DB.
    """

    # find the core cases table
    master = None
    for name, df in mapped_dfs.items():
        if "case_id" in df.columns and "admission_date" in df.columns:
            master = df.copy()
            print(f"  Master table: [{name}] with {len(master)} cases")
            break

    if master is None:
        # fallback - use first df with case_id
        for name, df in mapped_dfs.items():
            if "case_id" in df.columns:
                master = df.copy()
                print(f"  Fallback master: [{name}]")
                break

    if master is None:
        raise RuntimeError("Could not find a master cases table with case_id")

    return master


# ─────────────────────────────────────────
# Step 4 - Save to SQLite
# ─────────────────────────────────────────

def _save_to_db(
    mapped_dfs: dict[str, pd.DataFrame],
    db_path: str = "healthcare_unified.db",
) -> None:
    """
    Save each dataframe as its own table in SQLite.
    Tables are named by their data_type + source name.
    """
    conn = sqlite3.connect(db_path)

    # group by data_type
    type_counts: dict[str, int] = {}

    for name, df in mapped_dfs.items():
        # use the name directly as table name - clean it up
        table_name = name.replace("-", "_").replace(" ", "_").lower()

        df.to_sql(
            table_name,
            conn,
            if_exists="replace",
            index=False,
        )

        rows = len(df)
        cols = len(df.columns)
        print(f"  Saved [{table_name}] → {rows} rows, {cols} cols")

        data_type = df["_data_type"].iloc[0] if "_data_type" in df.columns else "unknown"
        type_counts[data_type] = type_counts.get(data_type, 0) + 1

    # also save a unified view: cases + key fields joined
    print("\n  Building unified_cases view...")
    _save_unified_view(mapped_dfs, conn)

    conn.close()
    print(f"\n  Database saved to: {db_path}")
    print(f"  Table type summary: {type_counts}")


def _save_unified_view(
    mapped_dfs: dict[str, pd.DataFrame],
    conn: sqlite3.Connection,
) -> None:
    """
    Create one wide unified_cases table:
    cases + one row per case with aggregated info from other tables.
    """

    # find master cases df
    master = None
    for name, df in mapped_dfs.items():
        if "_data_type" in df.columns and df["_data_type"].iloc[0] == "core_cases":
            master = df.copy()
            break

    if master is None:
        print("  No core_cases table found for unified view")
        return

    if "case_id" not in master.columns:
        print("  Master has no case_id - skipping unified view")
        return

    unified = master.copy()

    # merge epa assessments (one-to-one on case_id)
    for name, df in mapped_dfs.items():
        if "_data_type" not in df.columns:
            continue
        dtype = df["_data_type"].iloc[0]

        if dtype == "epa_assessment" and "case_id" in df.columns:
            # drop columns already in unified to avoid duplicates
            existing = set(unified.columns)
            new_cols = [c for c in df.columns
                       if c not in existing or c == "case_id"]
            df_subset = df[new_cols].drop_duplicates(subset=["case_id"])
            unified = unified.merge(df_subset, on="case_id", how="left")
            print(f"    Merged epa [{name}] into unified view")

    # for one-to-many tables: add summary columns
    for name, df in mapped_dfs.items():
        if "_data_type" not in df.columns:
            continue
        dtype = df["_data_type"].iloc[0]

        if dtype == "labs" and "case_id" in df.columns:
            lab_count = df.groupby("case_id").size().reset_index(name="lab_test_count")
            unified = unified.merge(lab_count, on="case_id", how="left")
            print(f"    Added lab count from [{name}]")

        if dtype == "medication" and "case_id" in df.columns:
            med_count = df.groupby("case_id").size().reset_index(name="medication_count")
            unified = unified.merge(med_count, on="case_id", how="left")
            print(f"    Added medication count from [{name}]")

        if dtype == "nursing" and "case_id" in df.columns:
            note_count = df.groupby("case_id").size().reset_index(name="nursing_note_count")
            unified = unified.merge(note_count, on="case_id", how="left")
            print(f"    Added nursing note count from [{name}]")

    unified.to_sql("unified_cases", conn, if_exists="replace", index=False)
    print(f"  unified_cases saved: {len(unified)} rows, {len(unified.columns)} cols")


# ─────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────

def unify(
    dataframes: dict[str, pd.DataFrame],
    db_path: str = "healthcare_unified.db",
    model: str | None = None,
) -> dict[str, pd.DataFrame]:
    """
    Main function. Takes raw dataframes, uses Claude to map columns,
    merges by case_id, saves to SQLite.

    Returns the mapped dataframes dict.
    """

    print("=" * 50)
    print("HARMONIZER STARTING")
    print(f"Input: {len(dataframes)} dataframes")
    print("=" * 50)

    # Step 1 - Claude maps all columns
    print("\n[1/4] Calling Claude for column mapping...")
    mapping = _call_claude_for_mapping(dataframes, model=model)

    if mapping is None:
        raise RuntimeError("Claude could not produce a mapping")

    # optionally save mapping for inspection
    with open("column_mapping.json", "w", encoding="utf-8") as f:
        json.dump(mapping, f, indent=2, ensure_ascii=False)
    print("  Mapping saved to column_mapping.json")

    # Step 2 - apply mapping
    print("\n[2/4] Applying column mapping...")
    mapped_dfs = _apply_mapping(dataframes, mapping)

    # Step 3 - build master
    print("\n[3/4] Building master cases table...")
    master = _merge_by_case(mapped_dfs)
    print(f"  Master: {len(master)} cases, {len(master.columns)} columns")

    # Step 4 - save to db
    print("\n[4/4] Saving to database...")
    _save_to_db(mapped_dfs, db_path=db_path)

    print("\n" + "=" * 50)
    print("HARMONIZER DONE")
    print("=" * 50)

    return mapped_dfs