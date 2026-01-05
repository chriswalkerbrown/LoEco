# ============================================================================
# fetch_data.py — robust SSE, DST-safe, cron-safe
# ============================================================================

import os
import json
import requests
import pandas as pd
from datetime import datetime, timezone
from typing import Iterable

# ----------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------

URL = (
    "https://eu1.cloud.thethings.network/api/v3/"
    "as/applications/test-field-lora-meteoa/packages/storage/uplink_message"
)

TOKEN = os.environ.get("TTN_TOKEN")
LOOKBACK = "168h"  # last 7 days

DATA_DIR = "data"


# ----------------------------------------------------------------------------
# Safety checks (cron-safe)
# ----------------------------------------------------------------------------

if not TOKEN:
    raise EnvironmentError("TTN_TOKEN environment variable is not set")

os.makedirs(DATA_DIR, exist_ok=True)

# ----------------------------------------------------------------------------
# SSE parser
# ----------------------------------------------------------------------------

def sse_events(response: requests.Response) -> Iterable[str]:
    """
    Generator yielding complete SSE `data:` payloads as strings.
    Correctly handles multi-line events and direct JSON lines without 'data:' prefix.
    """
    buffer = []

    for raw_line in response.iter_lines(decode_unicode=True):
        if raw_line is None:
            continue

        line = raw_line.strip()

        # Blank line = end of event
        if line == "":
            if buffer:
                yield "\n".join(buffer)
                buffer = []
            continue

        # Ignore comments / keep-alives
        if line.startswith(":"):
            continue

        # If it's a data line, strip "data: " prefix
        if line.startswith("data:"):
            buffer.append(line[5:].lstrip())
        else:
            # Otherwise, assume it's a direct JSON line or part of a multi-line event
            # This handles cases where the API sends JSON without "data:" prefix
            buffer.append(line)

    # Flush any remaining buffer
    if buffer:
        yield "\n".join(buffer)

# ----------------------------------------------------------------------------
# Fetch data
# ----------------------------------------------------------------------------

headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "text/event-stream",
}

params = {"last": LOOKBACK}

rows = []

with requests.get(URL, headers=headers, params=params, stream=True, timeout=60) as r:
    r.raise_for_status()

    for event in sse_events(r):
        try:
            payload = json.loads(event)
        except json.JSONDecodeError:
            continue

        result = payload.get("result", payload)

        uplink = result.get("uplink_message")
        if not uplink:
            continue

        ts = result.get("received_at")
        ts_parsed = pd.to_datetime(ts, utc=True, errors="coerce")
        if pd.isna(ts_parsed):
            continue

        row = {
            "device_id": result.get("end_device_ids", {}).get("device_id"),
            "time": ts_parsed,
            "f_cnt": uplink.get("f_cnt"),
        }

        decoded_payload = uplink.get("decoded_payload", {})
        if isinstance(decoded_payload, dict):
            row.update(decoded_payload)

        rows.append(row)

# ----------------------------------------------------------------------------
# DataFrame construction
# ----------------------------------------------------------------------------

if not rows:
    raise ValueError("No data received from TTN API")

df = pd.DataFrame(rows)

df["time"] = pd.to_datetime(df["time"], utc=True)
df = (
    df.dropna(subset=["time", "device_id"])
      .sort_values("time")
      .set_index("time")
)

# ----------------------------------------------------------------------------
# Resampling (DST-safe because UTC)
# ----------------------------------------------------------------------------

df_resampled = (
    df
    .groupby("device_id")
    .resample("30min")
    .first()
    .drop(columns="device_id") # Drop the redundant 'device_id' column before resetting index
    .reset_index()
    .set_index("time")
)

# ----------------------------------------------------------------------------
# Interpolation
# ----------------------------------------------------------------------------

def interpolate_meteo(df: pd.DataFrame) -> pd.DataFrame:
    linear_keys = ("temp", "hum", "press", "wind", "rain")
    ffill_keys = ("bat", "status", "sensor")

    linear_vars = [
        c for c in df.columns
        if any(k in c.lower() for k in linear_keys)
    ]
    ffill_vars = [
        c for c in df.columns
        if any(k in c.lower() for k in ffill_keys)
    ]

    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError("DatetimeIndex required for time interpolation")

    out = df.copy()

    if linear_vars:
        out[linear_vars] = out[linear_vars].interpolate(
            method="time",
            limit_direction="both"
        )

    if ffill_vars:
        out[ffill_vars] = out[ffill_vars].ffill().bfill()

    return out

df_final = interpolate_meteo(df_resampled)

# ----------------------------------------------------------------------------
# Output (cron-safe, DST-safe)
# ----------------------------------------------------------------------------

now_utc = datetime.now(timezone.utc)
iso_year, iso_week, _ = now_utc.isocalendar()

weekly_name = f"weather_data_{iso_year}_W{iso_week:02d}.csv"
weekly_path = os.path.join(DATA_DIR, weekly_name)
latest_path = os.path.join(DATA_DIR, "latest.csv")

df_final.to_csv(weekly_path, index=True)
df_final.to_csv(latest_path, index=True)

print(f"Saved weekly file: {weekly_path}")
print(f"Saved latest file: {latest_path}")
# ----------------------------------------------------------------------------
# Save per-device CSVs
# ----------------------------------------------------------------------------

devices = {dev: df for dev, df in df_final.groupby("device_id")}

for dev, df_dev in devices.items():
    safe_name = dev.replace(" ", "_")
    device_path = os.path.join(DATA_DIR, f"{safe_name}.csv")
    df_dev.to_csv(device_path, index=True)
    print(f"Saved per-device file: {device_path}")
