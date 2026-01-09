# providers/ecowitt_provider.py

import pandas as pd
import requests
from datetime import datetime
from providers.base_provider import BaseProvider


class EcowittProvider(BaseProvider):
    """
    Fetches weather data from the Ecowitt Cloud API (v3).
    Normalizes nested Ecowitt JSON into the LoEco universal schema.
    """

    API_URL = "https://api.ecowitt.net/api/v3/device/real_time"

    SCHEMA_MAP = {
        "timestamp": "time",
        "temperature_c": "temp",
        "humidity_pct": "humidity",
        "pressure_hpa": "baromrelin",
        "dew_point_c": "dewpoint",
        "feels_like_c": "feelslike",
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
    # Helper: safely extract nested values
    # ---------------------------------------------------------
    def extract(self, section, field, default=None):
        """
        Extracts section[field]["value"] safely.
        Example: extract(indoor, "temperature") → float(value)
        """
        try:
            return float(section[field]["value"])
        except Exception:
            return default

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

        # Sections
        indoor = data.get("indoor", {})
        pressure = data.get("pressure", {})
        battery = data.get("battery", {})

        # Extract values
        temp_f = self.extract(indoor, "temperature")
        humidity = self.extract(indoor, "humidity")
        dew_f = self.extract(indoor, "dew_point")
        feels_f = self.extract(indoor, "feels_like")

        # Pressure (relative)
        pressure_rel = pressure.get("relative", {})
        pressure_inhg = None
        try:
            pressure_inhg = float(pressure_rel.get("value"))
        except Exception:
            pass

        # Battery (example: temperature sensor ch1)
        battery_section = battery.get("temperature_sensor_ch1", {})
        battery_v = None
        try:
            battery_v = float(battery_section.get("value"))
        except Exception:
            pass

        # Unit conversions
        def f_to_c(f):
            return (f - 32) * 5 / 9 if f is not None else None

        temp_c = f_to_c(temp_f)
        dew_c = f_to_c(dew_f)
        feels_c = f_to_c(feels_f)

        pressure_hpa = pressure_inhg * 33.8639 if pressure_inhg is not None else None

        # Build normalized row
        row = {
            "timestamp": datetime.utcnow().isoformat(),
            "temperature_c": temp_c,
            "humidity_pct": humidity,
            "pressure_hpa": pressure_hpa,
            "dew_point_c": dew_c,
            "feels_like_c": feels_c,
            "battery_voltage_v": battery_v,
            "signal_strength_dbm": None,  # Cloud API does not provide RSSI
        }

        df = pd.DataFrame([row])

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
