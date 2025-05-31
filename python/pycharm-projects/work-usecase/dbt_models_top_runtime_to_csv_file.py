import re
import pandas as pd

from io import StringIO

log_path = r"C:\Users\DanielAguayo\Downloads\console_output_39.log"
csv_path = r"C:\Users\DanielAguayo\Downloads\long_running_models_raw_hourly.csv"

# Function to strip ANSI color codes (like [32m green etc.)
def strip_ansi_codes(text):
    ansi_escape = re.compile(r'\x1b\[[0-9;]*m')
    return ansi_escape.sub('', text)

# Read and clean the log content
with open(log_path, "r", encoding="utf-8") as file:
    log_content = file.read()

print("=== File Content Preview ===")
print(log_content[:1000])  # Preview first 1000 characters

# Strip out ANSI color codes
log_content = strip_ansi_codes(log_content)

# Regex pattern to match successful model builds
pattern = re.compile(
    r"created sql (?:table|view|incremental) model ([\w\.]+).*?SUCCESS \d in ([\d\.]+)s",
    re.IGNORECASE
)

matches = pattern.findall(log_content)
print(f"\nNumber of detailed model matches found: {len(matches)}")

# Filter models with duration > 200 seconds
long_models = [(model, float(duration)) for model, duration in matches if float(duration) > 100]
print(f"Number of models > 200s: {len(long_models)}")

# Create DataFrame and export to CSV
df = pd.DataFrame(long_models, columns=["Model", "Duration_seconds"])
df.to_csv(csv_path, index=False)
print(f"Exported to: {csv_path}")

