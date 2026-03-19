# starthack-p-sylow
Healthcare Data Unifier — epaCC START Hack 2026

An intelligent pipeline that automatically ingests, maps and unifies scattered 
healthcare data from multiple sources into one clean, queryable database.
No manual column mapping. No data engineering. Just plug in your files and go.

---

## What It Does

Hospitals store patient data across dozens of systems with inconsistent naming:
- One system calls it `ENTRYDATE`
- Another calls it `AufnDat`  
- Another calls it `admission_date`

This pipeline automatically understands they are all the same thing — and merges 
everything into one unified patient record per case ID.

---

## Quick Start

### 1. Clone & Install

git clone https://github.com/your-repo/starthack-p-sylow
cd starthack-p-sylow
pip install -e .
pip install python-dotenv
2. Set API Key
Create a .env file in the root folder:
CopyANTHROPIC_API_KEY=sk-ant-your-key-here
3. Run
bashCopy$env:PYTHONPATH = "src"   # Windows
python main.py
bashCopyPYTHONPATH=src python main.py  # Mac/Linux
4. View Results
Open healthcare_unified.db with DB Browser for SQLite

Architecture
CopyRaw Files (CSV / Excel)
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
│healthcare_unified │  + unified_cases: one row per patient
│     .db           │
└───────────────────┘

Project Structure
Copystarthack-p-sylow/
│
├── main.py                          # Entry point - run this
├── harmonizer.py                    # AI-powered data unification engine
├── data_sources.py                  # All dataset URLs and names
├── setup.py                         # Package setup and dependencies
├── healthcare_unified.db            # Output database (generated)
├── column_mapping.json              # Claude's mapping decisions (generated)
├── .env                             # API keys (never commit this!)
│
└── src/
    └── ingestion/
        ├── loader.py                # Batch file downloader and loader
        └── file_parser/
            ├── models.py            # Data models (ReadResult, ParsingPlan etc.)
            ├── detection.py         # Supported encodings and delimiters
            ├── csv_reader.py        # Main CSV read orchestrator
            ├── excel_reader.py      # Main Excel read orchestrator
            ├── heuristics/
            │   └── csv_heuristics.py    # Scoring-based CSV detection
            ├── parsers/
            │   ├── csv_parser.py        # Applies CSV parsing plan
            │   └── excel_parser.py      # Applies Excel parsing plan
            └── llm/
                ├── claude_client.py     # Claude API client
                └── profile_builder.py   # Builds file profiles for Claude
Data Sources
The pipeline handles three groups of datasets:
Standard Datasets (DATASETS)
NameDescriptionepa_1EPA patient assessments - simple formatepa_2EPA assessments - SAP coded columnsepa_3EPA assessments - German full column namesepa_4EPA assessments - Excel formatepa_5EPA assessments - encoded formatlabs_csvLab results for 1000 cases (CSV)labs_xlsxLab results for 1000 cases (Excel)casesMaster case table with ICD-10 diagnosesmotionBed sensor motion and fall detection datamotion_rawRaw 1Hz accelerometer sensor datamedicationInpatient medication orders and administrationsnursingDaily nursing reports (free text)
Clinic Datasets (DATASETS_CLINIC)
Split data across 4 clinics - each with device, EPA, ICD/OPS,
labs, medication and nursing data.
Error Datasets (DATASETS_ERROR)
Same datasets with intentional errors - for testing robustness.

Output Database
healthcare_unified.db contains:
TableDescriptionunified_cases Main table - one row per case with everything joinedcasesMaster case table with diagnoses and procedureslabs_csvAll lab results with reference rangesmedicationAll medication orders and administrationsnursingAll nursing notesmotionMotion and fall detection eventsmotion_rawRaw sensor readingsepa_1/2/3/4EPA nursing assessments

How The AI Mapping Works

All column names from all 12 dataframes are sent to Claude
Claude identifies:

Which column is the case_id in each dataframe
Which column is the patient_id in each dataframe
What type of data each dataframe contains
The canonical English snake_case name for every column


The mapping is saved to column_mapping.json for inspection
Columns are renamed and dataframes are merged

Example mappings Claude resolves automatically:
Copyepa_1.FallID         →  case_id
epa_3.EinschIDFall   →  case_id
epa_3.AufnDat        →  admission_date
epa_3.EntlassDat     →  discharge_date
medication.encounter_id → case_id

Key Components

file_parser — Smart File Ingestion

Tries multiple encodings (utf-8, latin-1, cp1252 etc.)
Tries multiple delimiters (, ; \t |)
Scores candidates by column count and row count
Falls back to Claude if heuristics are ambiguous
Handles CSV and Excel transparently

loader.py — Batch Loader

Downloads files from GitHub URLs
Saves to temp files
Passes to correct parser
Returns clean dict[str, pd.DataFrame]

harmonizer.py — AI Unification Engine

Sends column schema to Claude API
Applies returned column mapping
Merges all tables by case_id
Builds unified view with summary counts
Saves everything to SQLite


Dependencies

Copypandas
anthropic
requests
openpyxl
python-dotenv
setuptools
Install all:
bashCopypip install -e .
pip install python-dotenv

Team

Team P-Sylow — START Hack 2026

Notes

epa_5 contains base64-encoded column names and is loaded but not fully mapped
epa_2 uses SAP internal codes — partially mapped by Claude
clinic_4_nursing_pdf is skipped (PDF support not yet implemented)
labs_csv and labs_xlsx are identical — both loaded, use either
Never commit your .env file or API keys to GitHub

Add to Conversation
