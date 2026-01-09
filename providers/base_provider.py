# providers/base_provider.py

import abc
import pandas as pd
from providers.schema import LOECO_SCHEMA


class BaseProvider(abc.ABC):
    """
    Abstract base class for all LoEco data providers.
    Ensures consistent interface and shared behavior.
    """

    def __init__(self, name: str, target_file: str):
        self.name = name
        self.target_file = target_file

    # ---------------------------------------------------------
    # Abstract methods providers must implement
    # ---------------------------------------------------------
    @abc.abstractmethod
    def fetch(self):
        """Fetch raw data from the provider API."""
        pass

    @abc.abstractmethod
    def normalize(self, raw):
        """Normalize provider-specific JSON into a DataFrame."""
        pass

    # ---------------------------------------------------------
    # Shared schema application
    # ---------------------------------------------------------
    def apply_schema(
        self,
        df,
        mapping,
        provider,
        station,
        latitude,
        longitude,
        sensor_type,
        height_m,
        owner,
    ):
        """
        Applies provider→LoEco mapping and ensures all LOECO_SCHEMA fields exist.
        """

        # 1. Keep only columns that appear in the mapping
        keep = [col for col in df.columns if col in mapping]
        df = df[keep].copy()

        # 2. Rename provider columns → LoEco universal names
        df = df.rename(columns=mapping)

        # 3. Add metadata
        df["schema_version"] = 1
        df["provider"] = provider
        df["station"] = station
        df["latitude"] = latitude
        df["longitude"] = longitude
        df["sensor_type"] = sensor_type
        df["height_m"] = height_m
        df["owner"] = owner

        # 4. Ensure ALL schema fields exist (fill missing with None)
        for col in LOECO_SCHEMA:
            if col not in df.columns:
                df[col] = None

        # 5. Reorder columns to match the universal schema
        df = df[LOECO_SCHEMA]

        return df

    # ---------------------------------------------------------
    # Shared run() method used by all providers
    # ---------------------------------------------------------
    def run(self):
        """Fetch → normalize → save to Parquet."""
        print(f"→ Fetching data for {self.name}...")
        raw = self.fetch()

        print(f"→ Normalizing data for {self.name}...")
        df = self.normalize(raw)

        if df is None or df.empty:
            print(f"[WARN] No data returned for {self.name}")
            return

        print(f"→ Saving data for {self.name}...")
        df.to_parquet(self.target_file, index=False)
        print(f"✓ Saved data → {self.target_file}")
