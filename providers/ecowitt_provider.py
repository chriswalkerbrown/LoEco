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
        "wind_speed_ms": "windspeed",
        "wind_gust_ms": "windgust",
        "wind_dir_deg": "winddir",
        "rain_rate_mmhr": "rainrate",
        "rain_daily_mm": "raindaily",
        "solar_wm2": "solarradiation",
        "uv_index": "uv",
        "battery_voltage_v": "battery",
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
        outdoor = data.get("outdoor", {})
        indoor = data.get("indoor", {})
        solar = data.get("solar_and_uvi", {})
        rainfall = data.get("rainfall", {})
        wind = data.get("wind", {})
        pressure = data.get("pressure", {})
        battery = data.get("battery", {})

        # -----------------------------
        # Outdoor temperature & humidity
        # -----------------------------
        temp_f = self.extract(outdoor, "temperature")
        humidity = self.extract(outdoor, "humidity")
        dew_f = self.extract(outdoor, "dew_point")
        feels_f = self.extract(outdoor, "feels_like")

        # -----------------------------
        # Solar & UV
        # -----------------------------
        solar_wm2 = None
        try:
            solar_wm2 = float(solar.get("solar", {}).get("value"))
        except Exception:
            pass

        uv_index = None
        try:
            uv_index = float(solar.get("uvi", {}).get("value"))
        except Exception:
            pass

        # -----------------------------
        # Rainfall (convert inches → mm)
        # -----------------------------
        def inches_to_mm(x):
            return x * 25.4 if x is not None else None

        rain_rate_in = None
        try:
            rain_rate_in = float(rainfall.get("rain_rate", {}).get("value"))
        except Exception:
            pass

        rain_daily_in = None
        try:
            rain_daily_in = float(rainfall.get("daily", {}).get("value"))
        except Exception:
            pass

        rain_rate_mmhr = inches_to_mm(rain_rate_in)
        rain_daily_mm = inches_to_mm(rain_daily_in)

        # -----------------------------
        # Wind (mph → m/s)
        # -----------------------------
        def mph_to_ms(x):
            return x * 0.44704 if x is not None else None

        wind_speed_mph = None
        wind_gust_mph = None
        wind_dir_deg = None

        try:
            wind_speed_mph = float(wind.get("wind_speed", {}).get("value"))
        except Exception:
            pass

        try:
            wind_gust_mph = float(wind.get("wind_gust", {}).get("value"))
        except Exception:
            pass

        try:
            wind_dir_deg = float(wind.get("wind_direction", {}).get("value"))
        except Exception:
            pass

        wind_speed_ms = mph_to_ms(wind_speed_mph)
        wind_gust_ms = mph_to_ms(wind_gust_mph)

        # -----------------------------
        # Pressure (inHg → hPa)
        # -----------------------------
        pressure_rel = pressure.get("relative", {})
        pressure_inhg = None
        try:
            pressure_inhg = float(pressure_rel.get("value"))
        except Exception:
            pass

        pressure_hpa = pressure_inhg * 33.8639 if pressure_inhg is not None else None

        # -----------------------------
        # Battery
        # -----------------------------
        battery_v = None
        try:
            battery_v = float(battery.get("sensor_array", {}).get("value"))
        except Exception:
            pass

        # -----------------------------
        # Unit conversions
        # -----------------------------
        def f_to_c(f):
            return (f - 32) * 5 / 9 if f is not None else None

        temp_c = f_to_c(temp_f)
        dew_c = f_to_c(dew_f)
        feels_c = f_to_c(feels_f)

        # -----------------------------
        # Build normalized row
        # -----------------------------
        row = {
            "timestamp": datetime.utcnow().isoformat(),
            "temperature_c": temp_c,
            "humidity_pct": humidity,
            "pressure_hpa": pressure_hpa,
            "dew_point_c": dew_c,
            "feels_like_c": feels_c,
            "wind_speed_ms": wind_speed_ms,
            "wind_gust_ms": wind_gust_ms,
            "wind_dir_deg": wind_dir_deg,
            "rain_rate_mmhr": rain_rate_mmhr,
            "rain_daily_mm": rain_daily_mm,
            "solar_wm2": solar_wm2,
            "uv_index": uv_index,
            "battery_voltage_v": battery_v,
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
