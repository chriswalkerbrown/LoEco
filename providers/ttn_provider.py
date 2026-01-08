# providers/ttn_provider.py

import pandas as pd
from providers.base_provider import BaseProvider

class TTNProvider(BaseProvider):
    """
    Fetches weather data from The Things Network (TTN).
    """

    def __init__(self, name, token, application_id, lookback, target_file):
        super().__init__(name, target_file)
        self.token = token
        self.application_id = application_id
        self.lookback = lookback

    def fetch(self):
        """
        Fetch raw TTN data.
        Replace this stub with your existing TTN API logic.
        """
        # TODO: Insert your TTN API call here
        raise NotImplementedError("TTN API fetch logic not implemented yet")

    def normalize(self, raw):
        """
        Convert TTN raw payloads into a normalized dataframe.
        """
        # TODO: Convert raw TTN payloads into a pandas DataFrame
        df = pd.DataFrame(raw)
        return df
