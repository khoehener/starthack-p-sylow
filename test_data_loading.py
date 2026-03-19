from data_sources import DATASETS
import pandas as pd

name = "epa_1"

url = DATASETS[name]
print("Teste URL:", url)

try:
    df = pd.read_csv(url, sep=";")  # ← WICHTIG!
    print("✅ Datei geladen!")
    print("Shape:", df.shape)
    print("Spalten:", df.columns.tolist())
    print("Erste Zeile:")
    print(df.head(1))
except Exception as e:
    print("❌ Fehler:", e)