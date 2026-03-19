from data_reading_llm import read_file
import os
os.environ["ANTHROPIC_API_KEY"] = "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"  # dein echter Key

from data_reading_llm import read_file  # <- Name deiner .py Datei ohne .py

result = read_file("clinic_4_nursing.pdf")  # <- Name deiner PDF

print("Erfolgreich:", result.success)
print("Issues:", [(i.severity, i.message) for i in result.issues])
if result.data is not None:
    print(result.data)
