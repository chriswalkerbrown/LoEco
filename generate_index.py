#!/usr/bin/env python3
# ============================================================================
# generate_index.py — Build a professional dashboard index.html
# ============================================================================
import os
import json
from datetime import datetime, timezone

DATA_DIR = "data"
OUTPUT_FILE = "index.html"
CONFIG_FILE = "config/stations.json"

# ----------------------------------------------------------------------------
# HTML Template
# ----------------------------------------------------------------------------
HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Weather Data Dashboard</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 20px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            padding: 40px;
        }
        h1 {
            font-size: 2.5em;
            margin-bottom: 10px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        .timestamp {
            color: #666;
            font-size: 0.9em;
            margin-bottom: 40px;
            padding-bottom: 20px;
            border-bottom: 2px solid #f0f0f0;
        }
        .section { margin: 40px 0; }
        h2 {
            font-size: 1.8em;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 3px solid #667eea;
        }
        h3 {
            font-size: 1.3em;
            margin: 20px 0 10px;
            color: #444;
        }
        .cards {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(250px, 1fr));
            gap: 20px;
            margin-top: 10px;
        }
        .card {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            border-radius: 12px;
            padding: 25px;
            transition: transform 0.3s ease, box-shadow 0.3s ease;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }
        .card:hover {
            transform: translateY(-5px);
            box-shadow: 0 12px 24px rgba(0,0,0,0.2);
        }
        .card a {
            color: white;
            text-decoration: none;
            font-weight: 600;
            font-size: 1.1em;
            display: block;
            text-align: center;
        }
        .empty-state {
            text-align: center;
            padding: 40px;
            color: #999;
            font-style: italic;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🌤️ Weather Data Dashboard</h1>
        <p class="timestamp">Last updated: {{timestamp}}</p>

        <div class="section">
            <h2>📊 Interactive Plots</h2>
            {{plot_sections}}
        </div>

        <div class="section">
            <h2>📁 Data Files</h2>
            {{data_sections}}
        </div>
    </div>
</body>
</html>"""

# ----------------------------------------------------------------------------
def make_card(filename, label):
    return f'<div class="card"><a href="data/{filename}" target="_blank">{label}</a></div>'

# ----------------------------------------------------------------------------
def load_station_config():
    if not os.path.exists(CONFIG_FILE):
        return {}

    with open(CONFIG_FILE, "r") as f:
        cfg = json.load(f)

    lookup = {}
    for p in cfg.get("providers", []):
        provider_type = p["type"]
        name = p["name"]
        key = f"{provider_type}__{name}"
        lookup[key] = {
            "provider": provider_type,
            "name": name,
            "label": f"{provider_type.upper()} — {name.replace('_', ' ').title()}"
        }
    return lookup

# ----------------------------------------------------------------------------
def group_files_by_station(files, station_lookup):
    groups = {}
    for f in files:
        base = f.replace(".parquet", "").replace("plot_", "").replace(".html", "")
        station_key = base.split("_")[0] if "__" not in base else base.split("__")[0] + "__" + base.split("__")[1]

        if station_key not in groups:
            groups[station_key] = []
        groups[station_key].append(f)
    return groups

# ----------------------------------------------------------------------------
def main():
    station_lookup = load_station_config()

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    all_files = os.listdir(DATA_DIR) if os.path.exists(DATA_DIR) else []

    plot_files = [f for f in all_files if f.startswith("plot_") and f.endswith(".html")]
    data_files = [f for f in all_files if f.endswith(".parquet")]

    plot_groups = group_files_by_station(plot_files, station_lookup)
    data_groups = group_files_by_station(data_files, station_lookup)

    # Build plot sections
    plot_sections = ""
    for station_key, files in sorted(plot_groups.items()):
        label = station_lookup.get(station_key, {}).get("label", station_key)
        plot_sections += f"<h3>{label}</h3><div class='cards'>"
        for f in sorted(files):
            card_label = f.replace("plot_", "").replace(".html", "").replace("_", " ").title()
            plot_sections += make_card(f, card_label)
        plot_sections += "</div>"

    if not plot_sections:
        plot_sections = '<div class="empty-state">No plots available yet.</div>'

