# inspect_db.py
import sqlite3
import pandas as pd

conn = sqlite3.connect("healthcare_unified.db")

# show all tables
tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)
print("=== TABLES ===")
print(tables.to_string())

print("\n=== UNIFIED CASES PREVIEW ===")
unified = pd.read_sql("SELECT * FROM unified_cases", conn)
print(f"Shape: {unified.shape}")
print(unified.head(3).to_string())

print("\n=== TABLE SIZES ===")
for table in tables["name"]:
    count = pd.read_sql(f"SELECT COUNT(*) as rows FROM {table}", conn)
    cols  = pd.read_sql(f"SELECT * FROM {table} LIMIT 1", conn).shape[1]
    print(f"  {table:30s} → {count['rows'][0]:6} rows, {cols} cols")

conn.close()