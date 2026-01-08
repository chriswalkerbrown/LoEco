# fetch_all_data.py

import json
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from providers.ttn_provider import TTNProvider
from providers.ecowitt_provider import EcowittProvider


# ----------------------------------------------------------------------
# Provider registry
# ----------------------------------------------------------------------
PROVIDER_CLASSES = {
    "ttn": TTNProvider,
    "ecowitt": EcowittProvider,
}


# ----------------------------------------------------------------------
# Load stations.json
# ----------------------------------------------------------------------
def load_config(path="stations.json"):
    with open(path, "r") as f:
        return json.load(f)


# ----------------------------------------------------------------------
# Build target filename for monthly parquet storage
# ----------------------------------------------------------------------
def build_target_file(provider_type, provider_name):
    now = datetime.utcnow()
    year = now.strftime("%Y")
    month = now.strftime("%m")

    folder = Path("data") / year / month
    folder.mkdir(parents=True, exist_ok=True)

    filename = f"{provider_type}__{provider_name}.parquet"
    return folder / filename


# ----------------------------------------------------------------------
# Worker function for each provider
# ----------------------------------------------------------------------
def run_provider(entry):
    provider_type = entry.get("type")
    provider_name = entry.get("name")
    enabled = entry.get("enabled", False)

    if not enabled:
        return f"Skipped disabled provider: {provider_name}"

    if provider_type not in PROVIDER_CLASSES:
        return f"[ERROR] Unknown provider type: {provider_type}"

    ProviderClass = PROVIDER_CLASSES[provider_type]

    # Metadata
    latitude = entry.get("latitude")
    longitude = entry.get("longitude")
    sensor_type = entry.get("sensor_type")
    height_m = entry.get("height_m")
    owner = entry.get("owner")

    # Provider-specific config
    cfg = entry.get("config", {})

    # Output file
    target_file = build_target_file(provider_type, provider_name)

    print(f"\n=== Running provider: {provider_name} ({provider_type}) ===")
    print(f"→ Output file: {target_file}")

    try:
        provider = ProviderClass(
            name=provider_name,
            target_file=str(target_file),
            latitude=latitude,
            longitude=longitude,
            sensor_type=sensor_type,
            height_m=height_m,
            owner=owner,
            **cfg,
        )

        provider.run()
        return f"✓ Completed: {provider_name}"

    except Exception as e:
        return f"[ERROR] Provider {provider_name} failed: {e}"


# ----------------------------------------------------------------------
# Main orchestrator (parallel execution)
# ----------------------------------------------------------------------
def main():
    config = load_config()
    providers = config.get("providers", [])

    print(f"Starting parallel execution for {len(providers)} providers...")

    results = []

    # Run providers in parallel threads
    with ThreadPoolExecutor(max_workers=len(providers)) as executor:
        future_to_provider = {
            executor.submit(run_provider, entry): entry.get("name")
            for entry in providers
        }

        for future in as_completed(future_to_provider):
            provider_name = future_to_provider[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                results.append(f"[ERROR] Unexpected failure in {provider_name}: {e}")

    print("\n=== Summary ===")
    for r in results:
        print(r)


# ----------------------------------------------------------------------
# Entry point
# ----------------------------------------------------------------------
if __name__ == "__main__":
    main()
