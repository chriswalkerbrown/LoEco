# providers/ecowitt_provider.py

import pandas as pd
import requests
from providers.base_provider import BaseProvider


class EcowittProvider(BaseProvider):
    """
    Fetches weather data from the Ecowitt API.
    Normalizes Ecowitt JSON into the LoEco universal schema.
    """

    API_URL = "https://api.ecowitt.net/api/v3/device/real_time"

    SCHEMA_MAP = {
        "timestamp": "time",
        "temperature_c": "temp",
        "humidity_pct": "humidity",
        "pressure_hpa": "baromrelin",
        "wind_speed_ms": "windspeed",
        "wind_gust_ms": "windgust",
        "wind_dir_deg": "winddir",
        "rain_mm": "rainrate",
        "rain_rate_mmhr": "rainrate",
        "solar_wm2": "solarradiation",
        "uv_index": "uv",
        "soil_temperature_1_c": "soiltemp1",
        "soil_moisture_1_pct": "soilmoisture1",
        "pm2_5_ugm3": "pm25",
        "pm10_ugm3": "pm10",
        "co2_ppm": "co2",
        "battery_voltage_v": "battery",
        "signal_strength_dbm": "rssi",
    }

    def __init__(
        self,
        name,
        application_key,
        api_key,
        mac,
        target_file,
        latitude=None,
        longitude=None,
        sensor_type=None,
        height_m=None,
        owner=None,
    ):
        super().__init__(name, target_file)

        self.application_key = application_key
        self.api_key = api_key
        self.mac = mac

        self.latitude = latitude
        self.longitude = longitude
        self.sensor_type = sensor_type
        self.height_m = height_m
        self.owner = owner

    # ---------------------------------------------------------
    # REQUIRED ABSTRACT METHOD IMPLEMENTATION
    # ---------------------------------------------------------
    def fetch(self):
        params = {
            "application_key": self.application_key,
            "api_key": self.api_key,
            "mac": self.mac,
            "call_back": "all",
        }

        response = requests.get(self.API_URL, params=params)
        print("Ecowitt API response:", response.text)
        response.raise_for_status()
        return response.json()

    # ---------------------------------------------------------
    # REQUIRED ABSTRACT METHOD IMPLEMENTATION
    # ---------------------------------------------------------
    def normalize(self, raw):
        data = raw.get("data", {})

        if not data:
            print("[ERROR] Ecowitt returned no data:", raw)
            return pd.DataFrame()

        df = pd.json_normalize(data)

        if df.empty:
            print("[ERROR] Ecowitt normalization produced empty DataFrame:", data)
            return pd.DataFrame()

        return self.apply_schema(
            df=df,
            mapping=self.SCHEMA_MAP,
            provider="ecowitt",
            station=self.name,
            latitude=self.latitude,
            longitude=self.longitude,
            sensor_type=self.sensor_type,
            height_m=self.height_m,
            owner=self.owner,
        )
