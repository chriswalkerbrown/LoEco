# providers/ttn_provider.py

import json
import requests
import pandas as pd
from datetime import datetime, timedelta
from providers.base_provider import BaseProvider


class TTNProvider(BaseProvider):
    """
    Fetches weather data from The Things Network (TTN) Storage Integration (SSE stream).
    Normalizes TTN payloads into the LoEco universal schema.
    """

    TTN_URL = (
        "https://eu1.cloud.thethings.network/api/v3/as/applications/"
        "{app_id}/packages/storage/uplink_message"
    )

    SCHEMA_MAP = {
        "timestamp": "timestamp",
        "temperature_c": "temperature_c",
        "humidity_pct": "humidity_pct",
        "dewpoint_c": "dewpoint_c",
        "pressure_hpa": "pressure_hpa",
        "wind_speed_ms": "wind_speed_ms",
        "wind_gust_ms": "wind_gust_ms",
        "wind_dir_deg": "wind_dir_deg",
        "rain_mm": "rain_mm",
        "rain_rate_mmhr": "rain_rate_mmhr",
        "solar_wm2": "solar_wm2",
        "uv_index": "uv_index",
        "battery_voltage_v": "battery_voltage_v",
        "signal_strength_dbm": "signal_strength_dbm",
    }

    def __init__(
        self,
        name,
        token,
        application_id,
        lookback,
        target_file,
        latitude=None,
        longitude=None,
        sensor_type=None,
        height_m=None,
        owner=None,
    ):
        super().__init__(name, target_file)

        self.token = token
        self.application_id = application_id
        self.lookback = lookback
        self.device_id = name  # device_id matches provider name

        self.latitude = latitude
        self.longitude = longitude
        self.sensor_type = sensor_type
        self.height_m = height_m
        self.owner = owner

    # ---------------------------------------------------------
    # Fetch TTN uplinks via SSE stream
    # ---------------------------------------------------------
    def fetch(self):
        url = self.TTN_URL.format(app_id=self.application_id)

        headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "text/event-stream",
        }

        params = {"last": self.lookback}

        rows = []

        with requests.get(url, headers=headers, params=params, stream=True) as r:
            r.raise_for_status()

            for line in r.iter_lines():
                if not line:
                    continue

                decoded = line.decode("utf-8").strip()

                if decoded.startswith("data: "):
                    decoded = decoded[6:]

                if not decoded.startswith("{"):
                    continue

                try:
                    payload = json.loads(decoded)
                    result = payload.get("result", payload)

                    uplink = result.get("uplink_message", {})
                    decoded_payload = uplink.get("decoded_payload", {})
                    ts = result.get("received_at")

                    # Parse timestamp safely
                    ts_clean = ts.split(".")[0] + "Z" if ts else None
                    ts_parsed = (
                        datetime.fromisoformat(ts_clean.replace("Z", "+00:00"))
                        if ts_clean
                        else None
                    )

                    if ts_parsed:
                        row = {"timestamp": ts_parsed}
                        row.update(decoded_payload)
                        rows.append(row)

                except Exception:
                    continue

        if not rows:
            print("[WARN] TTN returned no valid uplinks")
            return None

        # Return the latest uplink only
        rows = sorted(rows, key=lambda r: r["timestamp"])
        return rows[-1]

    # ---------------------------------------------------------
    # Normalize TTN uplink into LoEco schema
    # ---------------------------------------------------------
    def normalize(self, raw):
        if raw is None:
            return pd.DataFrame()

        df = pd.DataFrame([raw])

        return self.apply_schema(
            df=df,
            mapping=self.SCHEMA_MAP,
            provider="ttn",
            station=self.name,
            latitude=self.latitude,
            longitude=self.longitude,
            sensor_type=self.sensor_type,
            height_m=self.height_m,
            owner=self.owner,
        )
