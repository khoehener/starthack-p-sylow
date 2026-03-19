from data_reading_llm import read_file
import os
from anthropic import Anthropic
from dotenv import load_dotenv

# 1. Lädt die Variablen aus der .env-Datei in das System (os.environ)
load_dotenv() 

# 2. Den Key aus dem System auslesen
api_key = os.getenv("ANTHROPIC_API_KEY")

# 3. Den Client mit dem Key starten
client = Anthropic(api_key=api_key)

from data_reading_llm import read_file  # <- Name deiner .py Datei ohne .py

result = read_file("clinic_4_nursing.pdf")  # <- Name deiner PDF

print("Erfolgreich:", result.success)
print("Issues:", [(i.severity, i.message) for i in result.issues])
if result.data is not None:
    print(result.data)
