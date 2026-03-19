from data_reading import read_file
from pathlib import Path

# Ordner, in dem dieses Python-Skript liegt
BASE_DIR = Path(__file__).parent

# Datei im gleichen Ordner
csv_file_path = BASE_DIR / "epaAC-Data-3.csv"
xlsx_file_path = BASE_DIR / "synth_device_motion_fall.xlsx"

df_csv = read_file(csv_file_path)
df_xlsx = read_file(xlsx_file_path)


print(df_csv.data.head()) # Zeigt die ersten Zeilen des DataFrames aus der CSV-Datei
# print(df_xlsx.data.head())  # Zeigt die ersten Zeilen des DataFrames aus der Excell