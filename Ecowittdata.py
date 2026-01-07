import requests
import csv
import json
import os
from datetime import datetime
from typing import Dict


class EcowittProvider:
    API_URL = "https://api.ecowitt.net/api/v3/device/real_time"

    def __init__(self, name: str, application_key: str, api_key: str, mac: str):
        self.name = name
        self.application_key = application_key
        self.api_key = api_key
        self.mac = mac

    def fetch(self) -> Dict:
        params = {
            "application_key": self.application_key,
            "api_key": self.api_key,
            "mac": self.mac,
            "call_back": "outdoor,indoor,pressure,wind,solar_and_uvi",
            "temp_unitid": 1,         # Celsius
            "pressure_unitid": 3,     # hPa
            "rainfall_unitid": 12,    # mm
            "wind_speed_unitid": 6    # km/h
        }

        response = requests.get(self.API_URL, params=params, timeout=10)
        response.raise_for_status()
        return response.json()

    def normalize(self, raw: Dict) -> Dict:
        """Convert Ecowitt API response to LoEco internal format"""

        data = raw.get("data", {})

        def v(section, key):
            return data.get(section, {}).get(key, {}).get("value")

        return {
            "timestamp": raw.get("time"),
            "outdoor_temperature": v("outdoor", "temperature"),
            "outdoor_feels_like": v("outdoor", "feels_like"),
            "outdoor_app_temp": v("outdoor", "app_temp"),
            "outdoor_dew_point": v("outdoor", "dew_point"),
            "outdoor_humidity": v("outdoor", "humidity"),
            "indoor_temperature": v("indoor", "temperature"),
            "indoor_humidity": v("indoor", "humidity"),
            "solar": v("solar_and_uvi", "solar"),
            "uvi": v("solar_and_uvi", "uvi"),
            "wind_speed": v("wind", "wind_speed"),
            "wind_gust": v("wind", "wind_gust"),
            "wind_direction": v("wind", "wind_direction"),
            "pressure_relative": v("pressure", "relative"),
            "pressure_absolute": v("pressure", "absolute"),
        }

    def write_csv(self, record: Dict):
        output_dir = os.path.join("data", self.name)
        os.makedirs(output_dir, exist_ok=True)

        filename = f"PWS_data_{self.name}_{datetime.utcnow().strftime('%Y_%m_%d')}.csv"
        path = os.path.join(output_dir, filename)

        header = list(record.keys())
        file_exists = os.path.isfile(path)

        with open(path, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=header)
            if not file_exists:
                writer.writeheader()
            writer.writerow(record)

    def run(self):
        raw = self.fetch()
        record = self.normalize(raw)
        self.write_csv(record)
        print(f"[Ecowitt:{self.name}] data written successfully")
