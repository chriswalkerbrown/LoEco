import json
import os

def load_secrets(path=".secrets.json"):
    if not os.path.exists(path):
        print(f"[WARN] Secrets file not found: {path}")
        return {}

    with open(path, "r") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            print("[ERROR] Could not parse .secrets.json")
            return {}
