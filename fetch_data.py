# ============================================================================
# fetch_data.py — Weather data fetcher with SSE support
# ============================================================================
# This script fetches weather data from The Things Network (TTN) via Server-Sent
# Events (SSE), processes and interpolates the data, then saves it to Parquet files.
# Designed to be cron-safe and DST-safe by using UTC timestamps throughout.
# ============================================================================

import os
import json
import requests
import pandas as pd
from datetime import datetime, timezone
from typing import Iterable

# ============================================================================
# CONFIGURATION
# ============================================================================

URL = (
    "https://eu1.cloud.thethings.network/api/v3/"
    "as/applications/test-field-lora-meteoa/packages/storage/uplink_message"
)

TOKEN = os.environ.get("TTN_TOKEN")
LOOKBACK = "168h"  # Fetch last 7 days of data

DATA_DIR = "data"

# ============================================================================
# SAFETY CHECKS
# ============================================================================

if not TOKEN:
    raise EnvironmentError("TTN_TOKEN environment variable is not set")

os.makedirs(DATA_DIR, exist_ok=True)

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def sse_events(response: requests.Response) -> Iterable[str]:
    """
    Parse Server-Sent Events (SSE) from a streaming HTTP response.
    
    Yields complete SSE data payloads as strings. Handles:
    - Multi-line events
    - Direct JSON lines without 'data:' prefix
    - Comment lines (ignored)
    - Keep-alive messages (ignored)
    
    Args:
        response: A streaming requests.Response object
        
    Yields:
        str: Complete SSE event data payload
    """
    buffer = []

    for raw_line in response.iter_lines(decode_unicode=True):
        if raw_line is None:
            continue

        line = raw_line.strip()

        # Blank line signals end of event
        if line == "":
            if buffer:
                yield "\n".join(buffer)
                buffer = []
            continue

        # Ignore comments and keep-alive messages
        if line.startswith(":"):
            continue

        # Strip "data: " prefix if present
        if line.startswith("data:"):
            buffer.append(line[5:].lstrip())
        else:
            # Handle direct JSON lines without prefix
            buffer.append(line)

    # Flush any remaining buffered data
    if buffer:
        yield "\n".join(buffer)


def interpolate_meteo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Interpolate missing weather data using appropriate methods.
    
    - Continuous measurements (temp, humidity, pressure, wind, rain): 
      Linear time-based interpolation
    - Discrete values (battery, status, sensor): 
      Forward fill then backward fill
    
    Args:
        df: DataFrame with DatetimeIndex and weather columns
        
    Returns:
        DataFrame with interpolated values
        
    Raises:
        TypeError: If df.index is not a DatetimeIndex
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError("DatetimeIndex required for time interpolation")

    # Define which columns get which interpolation method
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

    out = df.copy()

    # Linear interpolation for continuous measurements
    if linear_vars:
        out[linear_vars] = out[linear_vars].interpolate(
            method="time",
            limit_direction="both"
        )

    # Forward/backward fill for discrete values
    if ffill_vars:
        out[ffill_vars] = out[ffill_vars].ffill().bfill()

    return out


def append_only_new_rows(path: str, new_df: pd.DataFrame) -> None:
    """
    Append only rows newer than the last timestamp in an existing Parquet file.
    
    If the file doesn't exist, creates it. If it exists, only appends rows
    with timestamps newer than the most recent existing timestamp.
    
    Args:
        path: Path to Parquet file
        new_df: DataFrame with DatetimeIndex containing new data
    """
    if os.path.exists(path):
        old_df = pd.read_parquet(path)

        # Ensure datetime index
        old_df.index = pd.to_datetime(old_df.index)
        new_df.index = pd.to_datetime(new_df.index)

        # Find the latest timestamp already stored
        last_ts = old_df.index.max()

        # Keep only rows newer than last_ts
        new_only = new_df[new_df.index > last_ts]

        if new_only.empty:
            print(f"No new rows to append for {path}")
            return

        combined = pd.concat([old_df, new_only])
    else:
        combined = new_df

    # Sort by timestamp and save
    combined = combined.sort_index()
    combined.to_parquet(path, index=True)
    print(f"Updated: {path}")


# ============================================================================
# FETCH DATA FROM TTN API
# ============================================================================

headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "text/event-stream",
}

params = {"last": LOOKBACK}

rows = []

print("Fetching data from TTN API...")

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

        # Parse timestamp
        ts = result.get("received_at")
        ts_parsed = pd.to_datetime(ts, utc=True, errors="coerce")
        if pd.isna(ts_parsed):
            continue

        # Build row with basic metadata
        row = {
            "device_id": result.get("end_device_ids", {}).get("device_id"),
            "time": ts_parsed,
            "f_cnt": uplink.get("f_cnt"),
        }

        # Add decoded sensor payload
        decoded_payload = uplink.get("decoded_payload", {})
        if isinstance(decoded_payload, dict):
            row.update(decoded_payload)

        rows.append(row)

if not rows:
    raise ValueError("No data received from TTN API")

print(f"Fetched {len(rows)} data points")

# ============================================================================
# PROCESS DATA
# ============================================================================

print("Processing data...")

# Create DataFrame
df = pd.DataFrame(rows)
df["time"] = pd.to_datetime(df["time"], utc=True)
df = (
    df.dropna(subset=["time", "device_id"])
      .sort_values("time")
      .set_index("time")
)

# Resample to 30-minute intervals (DST-safe because UTC)
df_resampled = (
    df
    .groupby("device_id")
    .resample("30min")
    .first()
    .drop(columns="device_id")  # Remove redundant column
    .reset_index()
    .set_index("time")
)

# Interpolate missing values
df_final = interpolate_meteo(df_resampled)

print(f"Processed {len(df_final)} resampled data points")

# ============================================================================
# SAVE DATA TO PARQUET FILES
# ============================================================================

now_utc = datetime.now(timezone.utc)

# Monthly file (e.g., weather_data_2026_01_January.parquet)
year = now_utc.year
month = now_utc.month
month_name = now_utc.strftime("%B")
monthly_name = f"weather_data_{year}_{month:02d}_{month_name}.parquet"
monthly_path = os.path.join(DATA_DIR, monthly_name)

# Weekly file (e.g., weather_data_2026_W01.parquet)
iso_year, iso_week, _ = now_utc.isocalendar()
weekly_name = f"weather_data_{iso_year}_W{iso_week:02d}.parquet"
weekly_path = os.path.join(DATA_DIR, weekly_name)

# Latest file (always contains most recent data)
latest_path = os.path.join(DATA_DIR, "latest.parquet")

# Save aggregated files
print("\nSaving aggregated files...")
append_only_new_rows(monthly_path, df_final)
append_only_new_rows(weekly_path, df_final)
append_only_new_rows(latest_path, df_final)

# Save per-device files
print("\nSaving per-device files...")
devices = {dev: df for dev, df in df_final.groupby("device_id")}

for dev, df_dev in devices.items():
    safe_name = dev.replace(" ", "_")
    device_path = os.path.join(DATA_DIR, f"{safe_name}.parquet")
    append_only_new_rows(device_path, df_dev)

print("\n✓ All data saved successfully")
