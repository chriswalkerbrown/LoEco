# providers/base_provider.py

import abc

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

    def save(self, df):
        """Save normalized dataframe to Parquet."""
        df.to_parquet(self.target_file, index=False)
        print(f"✓ Saved data → {self.target_file}")

    def run(self):
        """Full provider pipeline."""
        print(f"→ Fetching data for {self.name}...")
        raw = self.fetch()

        print(f"→ Normalizing data for {self.name}...")
        df = self.normalize(raw)

        print(f"→ Saving data for {self.name}...")
        self.save(df)
