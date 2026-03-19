# main.py
import os
import sys
sys.path.insert(0, "src")

os.environ["ANTHROPIC_API_KEY"] = "your-key-here"  # or set in terminal

from ingestion.loader import load_all
from harmonizer import unify

# 1. Load all files
print("Loading data...")
dataframes = load_all(group="standard", verbose=True)

# 2. Unify
unified = unify(dataframes, db_path="healthcare_unified.db")