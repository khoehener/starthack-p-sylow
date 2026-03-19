from data_reading_llm import read_file
import os
os.environ["ANTHROPIC_API_KEY"] = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

from data_reading_llm import read_file  # <- Name deiner .py Datei ohne .py

result = read_file("clinic_4_nursing.pdf")  # <- Name deiner PDF

print("Erfolgreich:", result.success)
print("Issues:", [(i.severity, i.message) for i in result.issues])
if result.data is not None:
    print(result.data)
