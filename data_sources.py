import pandas as pd

BASE_ROOT = "https://raw.githubusercontent.com/adriank71/epaCC-START-Hack-2026/main/Endtestdaten_ohne_Fehler_einheitliche%20ID/"

DATASETS = {
    "epa_1": BASE_ROOT + "epaAC-Data-1.csv",
    "epa_2": BASE_ROOT + "epaAC-Data-2.csv",
    "epa_3": BASE_ROOT + "epaAC-Data-3.csv",
    "epa_4": BASE_ROOT + "epaAC-Data-4.xlsx",
    "epa_5": BASE_ROOT + "epaAC-Data-5.csv",

    "labs_csv": BASE_ROOT + "synth_labs_1000_cases.csv",
    "labs_xlsx": BASE_ROOT + "synth_labs_1000_cases.xlsx",

    "cases": BASE_ROOT + "synthetic_cases_icd10_ops.csv",
    "motion": BASE_ROOT + "synthetic_device_motion_fall_data.csv",
    "motion_raw": BASE_ROOT + "synthetic_device_raw_1hz_motion_fall.csv",
    "medication": BASE_ROOT + "synthetic_medication_raw_inpatient.csv",
    "nursing": BASE_ROOT + "synthetic_nursing_daily_reports_en.csv",
}
BASE_SPLIT = BASE_ROOT + "split_data_pat_case_altered/split_data_pat_case_altered/"

DATASETS_CLINIC = {
    # --- Clinic 1 ---
    "clinic_1_device": BASE_SPLIT + "clinic_1_device.csv",
    "clinic_1_device_1hz": BASE_SPLIT + "clinic_1_device_1hz.csv",
    "clinic_1_epa": BASE_SPLIT + "clinic_1_epaAC-Data.csv",
    "clinic_1_icd_ops": BASE_SPLIT + "clinic_1_icd_ops.csv",
    "clinic_1_labs": BASE_SPLIT + "clinic_1_labs.csv",
    "clinic_1_medication": BASE_SPLIT + "clinic_1_medication.csv",
    "clinic_1_nursing": BASE_SPLIT + "clinic_1_nursing.csv",

    # --- Clinic 2 ---
    "clinic_2_device": BASE_SPLIT + "clinic_2_device.csv",
    "clinic_2_epa": BASE_SPLIT + "clinic_2_epaAC-Data.csv",
    "clinic_2_icd_ops": BASE_SPLIT + "clinic_2_icd_ops.csv",
    "clinic_2_labs": BASE_SPLIT + "clinic_2_labs.csv",
    "clinic_2_medication": BASE_SPLIT + "clinic_2_medication.csv",
    "clinic_2_nursing": BASE_SPLIT + "clinic_2_nursing.csv",

    # --- Clinic 3 ---
    "clinic_3_device": BASE_SPLIT + "clinic_3_device.csv",
    "clinic_3_device_1hz": BASE_SPLIT + "clinic_3_device_1hz.csv",
    "clinic_3_epa": BASE_SPLIT + "clinic_3_epaAC-Data.csv",
    "clinic_3_icd_ops": BASE_SPLIT + "clinic_3_icd_ops.csv",
    "clinic_3_labs": BASE_SPLIT + "clinic_3_labs.csv",
    "clinic_3_medication": BASE_SPLIT + "clinic_3_medication.csv",
    "clinic_3_nursing": BASE_SPLIT + "clinic_3_nursing.csv",

    # --- Clinic 4 ---
    "clinic_4_device": BASE_SPLIT + "clinic_4_device.csv",
    "clinic_4_device_1hz": BASE_SPLIT + "clinic_4_device_1hz.csv",
    "clinic_4_epa": BASE_SPLIT + "clinic_4_epaAC-Data.xlsx",
    "clinic_4_icd_ops": BASE_SPLIT + "clinic_4_icd_ops.csv",
    "clinic_4_labs": BASE_SPLIT + "clinic_4_labs.csv",
    "clinic_4_medication": BASE_SPLIT + "clinic_4_medication.csv",
    # PDF handled separately
    "clinic_4_nursing_pdf": BASE_SPLIT + "clinic_4_nursing.pdf",
}
