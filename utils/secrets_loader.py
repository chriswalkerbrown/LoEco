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
def inject_secrets(config: dict, secrets: dict):
    """
    Recursively replace ${VAR} placeholders in a config dict
    using values from secrets.
    """
    resolved = {}

    for key, value in config.items():
        if isinstance(value, dict):
            resolved[key] = inject_secrets(value, secrets)

        elif isinstance(value, str) and value.startswith("${") and value.endswith("}"):
            secret_key = value[2:-1]  # strip ${ and }
            resolved[key] = secrets.get(secret_key)
            if resolved[key] is None:
                print(f"[WARN] Secret '{secret_key}' not found in .secrets.json")

        else:
            resolved[key] = value

    return resolved
