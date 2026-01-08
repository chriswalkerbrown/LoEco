#!/usr/bin/env python3
"""
LoEco Multi-Provider Data Fetcher
Orchestrates data collection from multiple weather station providers
"""
import os
import sys
import json
from typing import Dict

from providers.ttn_provider import TTNProvider
from providers.ecowitt_provider import EcowittProvider


def load_config(config_path: str = "config/stations.json") -> Dict:
    """Load provider configuration from JSON file"""
    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"Config file not found: {config_path}\n"
            "Please create config/stations.json with your provider settings."
        )
    
    with open(config_path, "r") as f:
        config = json.load(f)
    
    # Substitute environment variables
    config_str = json.dumps(config)
    for key, value in os.environ.items():
        config_str = config_str.replace(f"${{{key}}}", value)
    
    return json.loads(config_str)


def create_provider(provider_config: Dict):
    """
    Factory function to instantiate provider based on type.
    Adds a universal output filename: {type}__{name}.parquet
    """
    provider_type = provider_config["type"]
    name = provider_config["name"]
    config = provider_config["config"]

    # Universal naming convention
    output_filename = f"{provider_type}__{name}.parquet"
    output_path = os.path.join("data", output_filename)

    if provider_type == "ttn":
        return TTNProvider(
            name=name,
            token=config["token"],
            application_id=config["application_id"],
            lookback=config.get("lookback", "168h"),
            target_file=output_path
        )
    
    elif provider_type == "ecowitt":
        return EcowittProvider(
            name=name,
            application_key=config["application_key"],
            api_key=config["api_key"],
            mac=config["mac"],
            target_file=output_path
        )
    
    else:
        raise ValueError(f"Unknown provider type: {provider_type}")


def main():
    """Run all enabled providers"""
    print("=" * 70)
    print("LoEco Multi-Provider Data Fetcher")
    print("=" * 70)
    
    try:
        config = load_config()
    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        sys.exit(1)
    
    providers = config.get("providers", [])
    enabled_providers = [p for p in providers if p.get("enabled", True)]
    
    if not enabled_providers:
        print("No enabled providers found in config.")
        sys.exit(0)
    
    print(f"\nFound {len(enabled_providers)} enabled provider(s):\n")
    
    success_count = 0
    error_count = 0
    
    for provider_config in enabled_providers:
        name = provider_config["name"]
        provider_type = provider_config["type"]
        
        print(f"\n{'─' * 70}")
        print(f"Provider: {name} ({provider_type})")
        print(f"{'─' * 70}")
        
        try:
            provider = create_provider(provider_config)
            provider.run()
            success_count += 1
            
        except Exception as e:
            print(f"ERROR in {name}: {e}")
            error_count += 1
            import traceback
            traceback.print_exc()
    
    # Summary
    print(f"\n{'=' * 70}")
    print(f"Summary: {success_count} successful, {error_count} failed")
    print(f"{'=' * 70}\n")
    
    sys.exit(0 if error_count == 0 else 1)


if __name__ == "__main__":
    main()
