# providers/ttn_provider.py

import pandas as pd
from providers.base_provider import BaseProvider


class TTNProvider(BaseProvider):
    """
    Fetches weather data from The Things Network (TTN).
    Normalizes TTN payloads into the LoEco universal schema.
    """

    # Mapping TTN → LoEco schema
    # Adjust these keys to match your actual TTN payload fields
    SCHEMA_MAP = {
        "timestamp": "timestamp",

        "temperature_c": "temperature",
        "humidity_pct": "humidity",
        "dewpoint_c": "dewpoint",
        "pressure_hpa": "pressure",
        "wind_speed_ms": "wind_speed",
        "wind_gust_ms": "wind_gust",
        "wind_dir_deg": "wind_direction",
        "rain_mm": "rain",
        "rain_rate_mmhr": "rain_rate",
        "solar_wm2": "solar",
        "uv_index": "uv",
        "black_bulb_temperature_c": "black_bulb_temperature",

        # Extended sensors (adjust as needed)
        "soil_temperature_1_c": "soil_temp_1",
        "soil_moisture_1_pct": "soil_moisture_1",
        "pm2_5_ugm3": "pm25",
        "pm10_ugm3": "pm10",
        "co2_ppm": "co2",
        "battery_voltage_v": "battery_voltage",
        "signal_strength_dbm": "rssi",
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
    ):
        super().__init__(name, target_file)
        self.token = token
        self.application_id = application_id
        self.lookback = lookback

        # Metadata
        self.latitude = latitude
        self.longitude = longitude
        self.sensor_type = sensor_type
        self.height_m = height_m

    def fetch(self):
        """
        Fetch raw TTN data.
        Replace this stub with your actual TTN API logic.
        """
        raise NotImplementedError("TTN API fetch logic not implemented yet")

    def normalize(self, raw):
        df = pd.DataFrame(raw)

        return self.apply_schema(
            df=df,
            mapping=self.SCHEMA_MAP,
            provider="ttn",
            station=self.name,
            latitude=self.latitude,
            longitude=self.longitude,
            sensor_type=self.sensor_type,
            height_m=self.height_m,
        )
