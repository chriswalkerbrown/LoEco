# providers/ecowitt_provider.py

import pandas as pd
import requests
from providers.base_provider import BaseProvider

class EcowittProvider(BaseProvider):
    """
    Fetches weather data from the Ecowitt API.
    """

    API_URL = "https://api.ecowitt.net/api/v3/device/real_time"

    def __init__(self, name, application_key, api_key, mac, target_file):
        super().__init__(name, target_file)
        self.application_key = application_key
        self.api_key = api_key
        self.mac = mac

    def fetch(self):
        """
        Fetch raw Ecowitt data.
        """
        params = {
            "application_key": self.application_key,
            "api_key": self.api_key,
            "mac": self.mac,
            "call_back": "all"
        }

        response = requests.get(self.API_URL, params=params)
        response.raise_for_status()
        return response.json()

    def normalize(self, raw):
        """
        Convert Ecowitt JSON into a normalized dataframe.
        """
        # Ecowitt returns nested JSON; flatten it
        data = raw.get("data", {})
        df = pd.json_normalize(data)
        return df
