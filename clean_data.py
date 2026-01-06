import os
import re

DATA_DIR = "data"

# Patterns to keep
monthly_pattern = re.compile(r"weather_data_\d{4}_\d{2}_[A-Za-z]+\.parquet$")
weekly_pattern = re.compile(r"weather_data_\d{4}_W\d{2}\.parquet$")
device_pattern = re.compile(r"[A-Za-z0-9_\-]+\.parquet$")  # device files
latest_name = "latest.parquet"

for filename in os.listdir(DATA_DIR):
    path = os.path.join(DATA_DIR, filename)

    # Skip directories
    if os.path.isdir(path):
        continue

    # Keep monthly files
    if monthly_pattern.match(filename):
        continue

    # Keep latest.parquet
    if filename == latest_name:
        continue

    # Keep per-device parquet files (but NOT weekly)
    if filename.endswith(".parquet") and not weekly_pattern.match(filename):
        continue

    # Everything else gets deleted
    print(f"Deleting: {filename}")
    os.remove(path)

print("✓ Full reset complete")
