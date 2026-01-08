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

    @abc.abstractmethod
    def fetch(self):
        """Fetch raw data from the provider API."""
        pass

    @abc.abstractmethod
    def normalize(self, raw):
        """Convert provider-specific data into a normalized dataframe."""
        pass

    def apply_schema(
        self,
        df: pd.DataFrame,
        mapping: dict,
        provider: str,
        station: str,
        latitude: float = None,
        longitude: float = None,
        sensor_type: str = None,
        height_m: float = None,
        schema_version: int = 1,
    ) -> pd.DataFrame:
        """
        Normalize provider-specific columns into the LoEco schema.
        """

        out = {}

        # Fill all schema columns
        for loeco_col in LOECO_SCHEMA:

            # Metadata fields handled separately
            if loeco_col in (
                "provider",
                "station",
                "latitude",
                "longitude",
                "sensor_type",
                "height_m",
                "schema_version",
            ):
                continue

            provider_col = mapping.get(loeco_col)

            if provider_col and provider_col in df.columns:
                out[loeco_col] = df[provider_col]
            else:
                out[loeco_col] = pd.NA

        # ------------------------------------------------------------------
        # Fill metadata fields
        # ------------------------------------------------------------------
        out["provider"] = provider
        out["station"] = station
        out["latitude"] = latitude
        out["longitude"] = longitude
        out["sensor_type"] = sensor_type
        out["height_m"] = height_m
        out["schema_version"] = schema_version

        # ------------------------------------------------------------------
        # Warn on unknown provider columns
        # ------------------------------------------------------------------
        known_provider_cols = set(mapping.values())
        unknown_cols = [c for c in df.columns if c not in known_provider_cols]

        if unknown_cols:
            print(f"[WARN] {provider}/{station}: Unmapped provider columns: {unknown_cols}")

        return pd.DataFrame(out)

    def save(self, df: pd.DataFrame):
        df.to_parquet(self.target_file, index=False)
        print(f"✓ Saved data → {self.target_file}")

    def run(self):
        print(f"→ Fetching data for {self.name}...")
        raw = self.fetch()

        print(f"→ Normalizing data for {self.name}...")
        df = self.normalize(raw)

        print(f"→ Saving data for {self.name}...")
        self.save(df)
