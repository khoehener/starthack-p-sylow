# starthack-p-sylow
# 🏥 Healthcare Data Unifier — epaCC START Hack 2026

An intelligent pipeline that automatically ingests, maps and unifies scattered 
healthcare data from multiple sources into one clean, queryable database.
No manual column mapping. No data engineering. Just plug in your files and go.

---

## 🚀 What It Does

Hospitals store patient data across dozens of systems with inconsistent naming:

- One system calls it `ENTRYDATE`
- Another calls it `AufnDat`
- Another calls it `admission_date`

This pipeline automatically understands they are all the same thing and merges 
everything into one unified patient record per case ID.

---

## ⚡ Quick Start

**1. Clone and Install**

    git clone https://github.com/your-repo/starthack-p-sylow
    cd starthack-p-sylow
    pip install -e .
    pip install python-dotenv

**2. Set API Key**

Create a `.env` file in the root folder:

    ANTHROPIC_API_KEY=sk-ant-your-key-here

**3. Run**

Windows:

    $env:PYTHONPATH = "src"
    python main.py

Mac/Linux:

    PYTHONPATH=src python main.py

**4. View Results**

Open `healthcare_unified.db` with [DB Browser for SQLite](https://sqlitebrowser.org/)

---

## 🏗️ Architecture

    Raw Files (CSV / Excel)
            │
            ▼
    ┌───────────────────┐
    │   File Parser     │  Detects encoding, delimiter, structure
    │  (heuristics +    │  Falls back to Claude if ambiguous
    │   Claude AI)      │
    └───────────────────┘
            │
            ▼
    ┌───────────────────┐
    │     Loader        │  Downloads files from URLs
    │  loader.py        │  Passes each to correct parser
    └───────────────────┘
            │
            ▼
    ┌───────────────────┐
    │   Harmonizer      │  Sends all column names to Claude
    │  harmonizer.py    │  Claude maps everything to unified schema
    │                   │  Merges all data by case_id
    └───────────────────┘
            │
            ▼
    ┌───────────────────┐
    │  SQLite Database  │  One table per data type
    │  unified.db       │  + unified_cases: one row per patient
    └───────────────────┘

---

## 📁 Project Structure

    starthack-p-sylow/
    │
    ├── main.py                      # Entry point - run this
    ├── harmonizer.py                # AI-powered data unification engine
    ├── data_sources.py              # All dataset URLs and names
    ├── setup.py                     # Package setup and dependencies
    ├── healthcare_unified.db        # Output database (generated)
    ├── column_mapping.json          # Claude mapping decisions (generated)
    ├── .env                         # API keys - never commit this
    │
    └── src/
        └── ingestion/
            ├── loader.py            # Batch file downloader and loader
            └── file_parser/
                ├── models.py        # Data models (ReadResult, ParsingPlan)
                ├── detection.py     # Supported encodings and delimiters
                ├── csv_reader.py    # Main CSV read orchestrator
                ├── excel_reader.py  # Main Excel read orchestrator
                ├── heuristics/
                │   └── csv_heuristics.py   # Scoring-based CSV detection
                ├── parsers/
                │   ├── csv_parser.py       # Applies CSV parsing plan
                │   └── excel_parser.py     # Applies Excel parsing plan
                └── llm/
                    ├── claude_client.py    # Claude API client
                    └── profile_builder.py  # Builds file profiles for Claude

---

## 📊 Data Sources

The pipeline handles three groups of datasets:

### Standard Datasets

| Name | Description |
|------|-------------|
| epa_1 | EPA patient assessments - simple format |
| epa_2 | EPA assessments - SAP coded columns |
| epa_3 | EPA assessments - German full column names |
| epa_4 | EPA assessments - Excel format |
| epa_5 | EPA assessments - encoded format |
| labs_csv | Lab results for 1000 cases (CSV) |
| labs_xlsx | Lab results for 1000 cases (Excel) |
| cases | Master case table with ICD-10 diagnoses |
| motion | Bed sensor motion and fall detection data |
| motion_raw | Raw 1Hz accelerometer sensor data |
| medication | Inpatient medication orders and administrations |
| nursing | Daily nursing reports (free text) |

### Clinic Datasets

Split data across 4 clinics - each with device, EPA, ICD/OPS,
labs, medication and nursing data.

### Error Datasets

Same datasets with intentional errors - used for testing robustness.

---

## 🗄️ Output Database

`healthcare_unified.db` contains:

| Table | Description |
|-------|-------------|
| unified_cases | Main table - one row per case with everything joined |
| cases | Master case table with diagnoses and procedures |
| labs_csv | All lab results with reference ranges |
| medication | All medication orders and administrations |
| nursing | All nursing notes |
| motion | Motion and fall detection events |
| motion_raw | Raw sensor readings |
| epa_1/2/3/4 | EPA nursing assessments |

---

## 🧠 How The AI Mapping Works

1. All column names from all 12 dataframes are sent to Claude
2. Claude identifies:
   - Which column is the case_id in each dataframe
   - Which column is the patient_id in each dataframe
   - What type of data each dataframe contains
   - The canonical English snake_case name for every column
3. The mapping is saved to `column_mapping.json` for inspection
4. Columns are renamed and dataframes are merged

Example mappings Claude resolves automatically:

| Original | Mapped To |
|----------|-----------|
| epa_1.FallID | case_id |
| epa_3.EinschIDFall | case_id |
| epa_3.AufnDat | admission_date |
| epa_3.EntlassDat | discharge_date |
| medication.encounter_id | case_id |

---

## 🔧 Key Components

### file_parser — Smart File Ingestion

- Tries multiple encodings (utf-8, latin-1, cp1252 etc.)
- Tries multiple delimiters (, ; tab |)
- Scores candidates by column count and row count
- Falls back to Claude if heuristics are ambiguous
- Handles CSV and Excel transparently

### loader.py — Batch Loader

- Downloads files from GitHub URLs
- Saves to temp files
- Passes to correct parser
- Returns clean dict of DataFrames

### harmonizer.py — AI Unification Engine

- Sends column schema to Claude API
- Applies returned column mapping
- Merges all tables by case_id
- Builds unified view with summary counts
- Saves everything to SQLite

---

## 📦 Dependencies

| Package | Purpose |
|---------|---------|
| pandas | Data manipulation |
| anthropic | Claude AI API |
| requests | File downloads |
| openpyxl | Excel file support |
| python-dotenv | Environment variables |
| setuptools | Package setup |

Install all:

    pip install -e .
    pip install python-dotenv

---

## 👥 Team

**Team P-Sylow** — START Hack 2026

---

## ⚠️ Notes

- epa_5 contains base64-encoded column names and is loaded but not fully mapped
- epa_2 uses SAP internal codes — partially mapped by Claude
- clinic_4_nursing_pdf is skipped (PDF support not yet implemented)
- labs_csv and labs_xlsx are identical
