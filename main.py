# main.py
import os
import sys
sys.path.insert(0, "src")

from dotenv import load_dotenv
load_dotenv()  # reads .env file automatically

from ingestion.loader import load_all
from harmonizer import unify

print("Loading data...")
dataframes = load_all(group="standard", verbose=True)
unified = unify(dataframes, db_path="healthcare_unified.db")