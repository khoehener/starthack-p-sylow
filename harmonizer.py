# harmonizer.py
"""
Healthcare Data Harmonization Pipeline - Hackathon Edition
Liest Daten von 4 Kliniken via GitHub, harmonisiert Schema und schreibt in SQLite.
"""

import re
import logging
import sqlite3
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional
import pandas as pd

# ─────────────────────────── Konfiguration ────────────────────────────────────

DB_PATH = Path("healthcare_unified.db")
LOG_LEVEL = logging.INFO
NULL_STRINGS = frozenset(["missing", "unknow", "unknown", "nan", "n/a", "na", "none", "-", ""])

# ─────────────────────── Ziel-Schema Definition ────────────────────────────────

TARGET_SCHEMAS: dict[str, list[str]] = {
    "vitals": ["patient_id", "case_id", "timestamp", "source_clinic", "vital_type", "vital_value", "unit"],
    "labs": ["patient_id", "case_id", "timestamp", "source_clinic", "lab_parameter", "lab_value", "unit", "reference_range"],
    "medication": ["patient_id", "case_id", "timestamp", "source_clinic", "drug_name", "dose", "unit", "route"],
    "nursing": ["patient_id", "case_id", "timestamp", "source_clinic", "nursing_note_free_text", "report_date", "shift"],
    "device": ["patient_id", "timestamp", "source_clinic", "movement_index_0_100", "fall_event_0_1", "impact_magnitude_g"]
}

# ─────────────────── Spalten-Mapping Dictionary ───────────────────────────────

COLUMN_MAPPING: dict[str, str] = {
    "patient_id": "patient_id", "pat_id": "patient_id", "pid": "patient_id",
    "case_id": "case_id", "fall_id": "case_id", "fallnummer": "case_id",
    "timestamp": "timestamp", "zeit": "timestamp", "datetime": "timestamp", "messzeitpunkt": "timestamp",
    "vital_type": "vital_type", "vitalparameter": "vital_type",
    "vital_value": "vital_value", "messwert": "vital_value", "wert": "vital_value",
    "lab_parameter": "lab_parameter", "labor_parameter": "lab_parameter", "analyt": "lab_parameter",
    "lab_value": "lab_value", "labor_wert": "lab_value", "result": "lab_value",
    "drug_name": "drug_name", "medikament": "drug_name", "arzneimittel": "drug_name",
    "dose": "dose", "dosis": "dose", "unit": "unit", "einheit": "unit",
    "nursing_note_free_text": "nursing_note_free_text", "pflegebericht": "nursing_note_free_text",
    "movement_index_0_100": "movement_index_0_100", "fall_event_0_1": "fall_event_0_1"
}

# ──────────────────────── Klinik-Manifest (DEINE LINKS) ────────────────────────

BASE_URL = "https://raw.githubusercontent.com/adriank71/epaCC-START-Hack-2026/main/Endtestdaten_ohne_Fehler_einheitliche%20ID/split_data_pat_case_altered/split_data_pat_case_altered/"

CLINIC_MANIFEST = []
for i in range(1, 5):
    clinic_id = f"clinic_{i}"
    # Jeder Klinik-Satz besteht aus verschiedenen Dateien
    CLINIC_MANIFEST.append({"source_clinic": clinic_id, "table_type": "vitals", "url": f"{BASE_URL}{clinic_id}_epaAC-Data.csv"})
    CLINIC_MANIFEST.append({"source_clinic": clinic_id, "table_type": "labs", "url": f"{BASE_URL}{clinic_id}_labs.csv"})
    CLINIC_MANIFEST.append({"source_clinic": clinic_id, "table_type": "medication", "url": f"{BASE_URL}{clinic_id}_medication.csv"})
    CLINIC_MANIFEST.append({"source_clinic": clinic_id, "table_type": "nursing", "url": f"{BASE_URL}{clinic_id}_nursing.csv"})
    CLINIC_MANIFEST.append({"source_clinic": clinic_id, "table_type": "device", "url": f"{BASE_URL}{clinic_id}_device.csv"})

# Sonderfall Klinik 4 (Excel & PDF)
CLINIC_MANIFEST = [entry for entry in CLINIC_MANIFEST if not (entry["source_clinic"] == "clinic_4" and entry["table_type"] == "vitals")]
CLINIC_MANIFEST.append({"source_clinic": "clinic_4", "table_type": "vitals", "url": f"{BASE_URL}clinic_4_epaAC-Data.xlsx"})

# ─────────────────────────── Pipeline-Logik ────────────────────────────────────

def setup_logging():
    logger = logging.getLogger("harmonizer")
    logger.setLevel(LOG_LEVEL)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(handler)
    return logger

log = setup_logging()

def normalize_id(val):
    if pd.isna(val) or str(val).strip() == "": return None
    digits = re.sub(r"[^\d]", "", str(val))
    return int(digits) if digits else None

def process_file(entry, conn):
    try:
        log.info(f"Verarbeite {entry['source_clinic']} - {entry['table_type']}...")
        if entry['url'].endswith('.xlsx'):
            df = pd.read_excel(entry['url'])
        else:
            df = pd.read_csv(entry['url'], dtype=str)
        
        # 1. Clean IDs
        if 'case_id' in df.columns: df['case_id'] = df['case_id'].apply(normalize_id)
        if 'patient_id' in df.columns: df['patient_id'] = df['patient_id'].apply(lambda x: str(x).strip() if pd.notna(x) else None)
        
        # 2. Drop rows without IDs
        df = df.dropna(subset=['patient_id']) if 'patient_id' in df.columns else df

        # 3. Simple Column Mapping
        df.columns = [COLUMN_MAPPING.get(c.lower(), c) for c in df.columns]

        # 4. Align to Schema
        target_cols = TARGET_SCHEMAS.get(entry['table_type'], [])
        for col in target_cols:
            if col not in df.columns: df[col] = None
        df['source_clinic'] = entry['source_clinic']
        
        # 5. Save to SQL
        df[target_cols].to_sql(entry['table_type'], conn, if_exists='append', index=False)
        return len(df)
    except Exception as e:
        log.error(f"Fehler bei {entry['source_clinic']}: {e}")
        return 0

def main():
    conn = sqlite3.connect(DB_PATH)
    # Tabellen initialisieren
    for table, cols in TARGET_SCHEMAS.items():
        pd.DataFrame(columns=cols).to_sql(table, conn, if_exists='replace', index=False)
    
    total_rows = 0
    for entry in CLINIC_MANIFEST:
        total_rows += process_file(entry, conn)
    
    conn.close()
    print(f"\n✅ Fertig! {total_rows} Zeilen in '{DB_PATH}' gespeichert.")

if __name__ == "__main__":
    main()