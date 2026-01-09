# providers/ttn_provider.py

import pandas as pd
import requests
from datetime import datetime, timedelta
from providers.base_provider import BaseProvider


class TTNProvider(BaseProvider):
    """
    Fetches weather data from The Things Network (TTN).
    Normalizes TTN payloads into the LoEco universal schema.
    """

    TTN_API_URL = "https://eu1.cloud.thethings.network/api/v3/as/applications/{app_id}/devices/{device_id}/packages/storage/uplink_message"

    # Mapping TTN payload fields → LoEco schema fields
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
    # Fetch TTN uplinks
    # ---------------------------------------------------------
    def fetch(self):
        lookback_hours = int(self.lookback.replace("h", ""))
        start_time = datetime.utcnow() - timedelta(hours=lookback_hours)

        url = self.TTN_API_URL.format(
            app_id=self.application_id,
            device_id=self.device_id,
        )

        headers = {
            "Authorization": f"Bearer {self.token}",
        }

        params = {
            "after": start_time.isoformat() + "Z",
            "limit": 1,  # only need the latest uplink
        }

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()

        data = response.json()

        if not data:
            print("[WARN] TTN returned no uplinks")
            return None

        return data[0]  # latest uplink

    # ---------------------------------------------------------
    # Normalize TTN uplink into LoEco schema
    # ---------------------------------------------------------
    def normalize(self, raw):
        if raw is None:
            return pd.DataFrame()

        uplink = raw.get("uplink_message", {})
        payload = uplink.get("decoded_payload", {})
        rx_metadata = uplink.get("rx_metadata", [{}])[0]

        # Extract timestamp
        timestamp = uplink.get("received_at")

        # Build normalized row
        row = {
            "timestamp": timestamp,
            "temperature_c": payload.get("temperature"),
            "humidity_pct": payload.get("humidity"),
            "dewpoint_c": payload.get("dewpoint"),
            "pressure_hpa": payload.get("pressure"),
            "wind_speed_ms": payload.get("wind_speed"),
            "wind_gust_ms": payload.get("wind_gust"),
            "wind_dir_deg": payload.get("wind_direction"),
            "rain_mm": payload.get("rain"),
            "rain_rate_mmhr": payload.get("rain_rate"),
            "solar_wm2": payload.get("solar"),
            "uv_index": payload.get("uv"),
            "battery_voltage_v": payload.get("battery_voltage"),
            "signal_strength_dbm": rx_metadata.get("rssi"),
        }

        df = pd.DataFrame([row])

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
