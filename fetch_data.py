# ============================================================================
# fetch_data.py — Weather data fetcher with SSE support + Kalman smoothing
# ============================================================================

import os
import json
import requests
import pandas as pd
import numpy as np
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
    buffer = []
    for raw_line in response.iter_lines(decode_unicode=True):
        if raw_line is None:
            continue
        line = raw_line.strip()
        if line == "":
            if buffer:
                yield "\n".join(buffer)
                buffer = []
            continue
        if line.startswith(":"):
            continue
        if line.startswith("data:"):
            buffer.append(line[5:].lstrip())
        else:
            buffer.append(line)
    if buffer:
        yield "\n".join(buffer)


# ============================================================================
# OUTLIER DETECTION
# ============================================================================

def remove_outliers(series: pd.Series, n_sigma=3) -> pd.Series:
    """
    Remove statistical outliers beyond n_sigma standard deviations.
    Uses rolling statistics to handle non-stationary data.
    """
    if len(series.dropna()) < 10:
        return series
    
    # Use rolling window for non-stationary data
    rolling_mean = series.rolling(window=12, center=True, min_periods=3).mean()
    rolling_std = series.rolling(window=12, center=True, min_periods=3).std()
    
    # Fallback to global statistics for edges
    rolling_mean = rolling_mean.fillna(series.mean())
    rolling_std = rolling_std.fillna(series.std())
    
    # Mark outliers
    deviation = (series - rolling_mean).abs()
    threshold = n_sigma * rolling_std
    mask = deviation <= threshold
    
    # Replace outliers with NaN
    return series.where(mask)


# ============================================================================
# TIME-AWARE KALMAN FILTER
# ============================================================================

def kalman_smooth_series(series: pd.Series, Q_base=0.01, R=1.0) -> pd.Series:
    """
    Apply 1D Kalman filter with time-gap awareness.
    
    Parameters:
    -----------
    series : pd.Series with DatetimeIndex
        The time series to smooth
    Q_base : float
        Process noise per hour (how much we expect value to change)
    R : float
        Measurement noise (sensor accuracy)
    
    Returns:
    --------
    pd.Series
        Smoothed time series
    """
    x = series.copy().astype(float)
    
    # Handle duplicate indices by keeping first occurrence
    if x.index.duplicated().any():
        print(f"Warning: Duplicate indices found in {series.name}, keeping first occurrence")
        x = x[~x.index.duplicated(keep='first')]
    
    mask = x.notna()
    values = x[mask].values
    index = x[mask].index

    if len(values) < 3:
        return series  # Not enough data to filter

    # Initialize
    x_est = values[0]
    P = R  # Initial uncertainty = measurement noise
    out = []
    
    for i, z in enumerate(values):
        # Calculate time delta (in hours) for time-varying process noise
        if i > 0:
            dt = (index[i] - index[i-1]).total_seconds() / 3600
            dt = max(dt, 0.01)  # Avoid division by zero
        else:
            dt = 0.5  # Default 30 min for first point
        
        # Scale process noise by time gap
        Q = Q_base * dt
        
        # Prediction step
        x_pred = x_est
        P_pred = P + Q
        
        # Update step (Kalman gain)
        K = P_pred / (P_pred + R)
        x_est = x_pred + K * (z - x_pred)
        P = (1 - K) * P_pred
        
        out.append(x_est)
    
    # Sanity check: detect filter divergence
    if len(out) > 0:
        original_std = np.std(values)
        smoothed_std = np.std(out)
        
        if smoothed_std > 3 * original_std:
            print(f"Warning: Kalman filter unstable for {series.name}, using original data")
            return series
    
    # Create result series matching original structure
    result = series.copy()
    
    # Handle duplicate indices in original series
    if result.index.duplicated().any():
        result = result[~result.index.duplicated(keep='first')]
    
    # Put filtered values back
    for idx, val in zip(index, out):
        result.loc[idx] = val
    
    return result


# ============================================================================
# SENSOR-SPECIFIC PARAMETERS
# ============================================================================

SENSOR_PARAMS = {
    "tempc_sht": {"Q_base": 0.01, "R": 0.5},   # Dry bulb temp: slow changes
    "tempc_ds": {"Q_base": 0.02, "R": 0.8},    # Black bulb: faster response to sun
    "hum_sht": {"Q_base": 0.05, "R": 2.0},     # Humidity: more variable
    "press": {"Q_base": 0.001, "R": 0.1},      # Pressure: very stable
    "wind": {"Q_base": 0.5, "R": 5.0},         # Wind: highly variable
    "rain": {"Q_base": 0.1, "R": 0.5},         # Rain: step changes
}


def get_sensor_params(col_name: str) -> dict:
    """Get Kalman parameters for a specific sensor."""
    col_lower = col_name.lower()
    
    for key, params in SENSOR_PARAMS.items():
        if key in col_lower:
            return params
    
    # Default parameters
    return {"Q_base": 0.01, "R": 1.0}


# ============================================================================
# METEOROLOGICAL INTERPOLATION + KALMAN SMOOTHING
# ============================================================================

def interpolate_meteo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Complete meteorological data processing pipeline:
    1. Remove outliers
    2. Apply Kalman smoothing to raw data
    3. Interpolate remaining short gaps
    4. Forward-fill discrete variables
    
    Parameters:
    -----------
    df : pd.DataFrame with DatetimeIndex
        Weather data with columns like TempC_SHT, Hum_SHT, BatV, etc.
    
    Returns:
    --------
    pd.DataFrame
        Processed weather data
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError("DatetimeIndex required for time interpolation")
    
    # Classify variables
    linear_keys = ("temp", "hum", "press", "wind", "rain")
    ffill_keys = ("bat", "status", "sensor", "rssi", "snr", "f_cnt", "device")
    
    linear_vars = []
    ffill_vars = []
    
    for col in df.columns:
        col_lower = col.lower()
        
        if any(k in col_lower for k in linear_keys):
            linear_vars.append(col)
        elif any(k in col_lower for k in ffill_keys):
            ffill_vars.append(col)
    
    out = df.copy()
    
    # STEP 1: Remove outliers from continuous variables
    print("  → Removing outliers...")
    for col in linear_vars:
        out[col] = remove_outliers(out[col], n_sigma=3)
    
    # STEP 2: Apply Kalman smoothing to raw sensor data
    print("  → Applying Kalman smoothing...")
    for col in linear_vars:
        params = get_sensor_params(col)
        out[col] = kalman_smooth_series(out[col], **params)
    
    # STEP 3: Interpolate remaining short gaps (<= 2 hours)
    print("  → Interpolating gaps...")
    if linear_vars:
        out[linear_vars] = out[linear_vars].interpolate(
            method="time",
            limit=4,  # 4 × 30 min = 2 hours
            limit_direction="both"
        )
    
    # STEP 4: Forward/backward fill discrete variables
    print("  → Filling discrete variables...")
    if ffill_vars:
        out[ffill_vars] = out[ffill_vars].ffill().bfill()
    
    return out


# ============================================================================
# APPEND-ONLY PARQUET WRITER
# ============================================================================

def append_only_new_rows(path: str, new_df: pd.DataFrame) -> None:
    if os.path.exists(path):
        old_df = pd.read_parquet(path)
        old_df.index = pd.to_datetime(old_df.index)
        new_df.index = pd.to_datetime(new_df.index)
        last_ts = old_df.index.max()
        new_only = new_df[new_df.index > last_ts]
        if new_only.empty:
            print(f"No new rows to append for {path}")
            return
        combined = pd.concat([old_df, new_only])
    else:
        combined = new_df

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

if not rows:
    raise ValueError("No data received from TTN API")

print(f"Fetched {len(rows)} data points")

# ============================================================================
# PROCESS DATA
# ============================================================================

print("\nProcessing data...")

df = pd.DataFrame(rows)
df["time"] = pd.to_datetime(df["time"], utc=True)
df = (
    df.dropna(subset=["time", "device_id"])
      .sort_values("time")
      .set_index("time")
)

df_resampled = (
    df
    .groupby("device_id")
    .resample("30min")
    .first()
)

# Reset index fully to get both time and device_id as columns
df_resampled = df_resampled.reset_index()

# Set time as index
df_resampled = df_resampled.set_index("time")

# Remove any duplicate time indices (keep first occurrence per timestamp)
if df_resampled.index.duplicated().any():
    print(f"Warning: Found {df_resampled.index.duplicated().sum()} duplicate timestamps, keeping first")
    df_resampled = df_resampled[~df_resampled.index.duplicated(keep='first')]

print(f"Resampled to {len(df_resampled)} data points")
print("\nApplying Kalman filtering and interpolation...")

df_final = interpolate_meteo(df_resampled)

print(f"✓ Processed {len(df_final)} final data points")

# ============================================================================
# SAVE DATA TO PARQUET FILES
# ============================================================================

now_utc = datetime.now(timezone.utc)

year = now_utc.year
month = now_utc.month
month_name = now_utc.strftime("%B")
monthly_name = f"weather_data_{year}_{month:02d}_{month_name}.parquet"
monthly_path = os.path.join(DATA_DIR, monthly_name)

iso_year, iso_week, _ = now_utc.isocalendar()
weekly_name = f"weather_data_{iso_year}_W{iso_week:02d}.parquet"
weekly_path = os.path.join(DATA_DIR, weekly_name)

latest_path = os.path.join(DATA_DIR, "latest.parquet")

print("\nSaving aggregated files...")
append_only_new_rows(monthly_path, df_final)
append_only_new_rows(weekly_path, df_final)
append_only_new_rows(latest_path, df_final)

print("\nSaving per-device files...")
devices = {dev: df for dev, df in df_final.groupby("device_id")}

for dev, df_dev in devices.items():
    safe_name = dev.replace(" ", "_")
    device_path = os.path.join(DATA_DIR, f"{safe_name}.parquet")
    append_only_new_rows(device_path, df_dev)

print("\n✓ All data saved successfully")
