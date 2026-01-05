# ============================================================================
# generate_index.py — Dashboard + CSV index + per-device pages + device selector
# ============================================================================

import os
import csv
import html
from datetime import datetime, timezone
from collections import defaultdict

DATA_DIR = "data"
DEVICES_DIR = "devices"

os.makedirs(DEVICES_DIR, exist_ok=True)

# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------

def utc_now_str():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

def file_size_kb(path):
    return f"{os.path.getsize(path) / 1024:.1f} KB"

def csv_preview(path):
    record_count = 0
    last_ts = None
    devices = set()

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            record_count += 1
            last_ts = row.get("time", last_ts)
            if "device_id" in row and row["device_id"]:
                devices.add(row["device_id"])

    return last_ts or "—", record_count, devices

def safe_device_name(device_id):
    return device_id.replace(" ", "_")

# ----------------------------------------------------------------------------
# Discover CSV files
# ----------------------------------------------------------------------------

if not os.path.isdir(DATA_DIR):
    raise FileNotFoundError("data directory does not exist")

csv_files = [f for f in os.listdir(DATA_DIR) if f.endswith(".csv")]

if not csv_files:
    raise RuntimeError("No CSV files found in data directory")

def sort_key(name):
    if name == "latest.csv":
        return (0, 0)
    return (1, name)

csv_files.sort(key=sort_key)

# ----------------------------------------------------------------------------
# Collect metadata
# ----------------------------------------------------------------------------

file_meta = {}
device_to_files = defaultdict(list)

for filename in csv_files:
    path = os.path.join(DATA_DIR, filename)
    last_ts, count, devices = csv_preview(path)

    file_meta[filename] = {
        "last_ts": last_ts,
        "count": count,
        "size": file_size_kb(path),
        "devices": devices,
    }

    for d in devices:
        device_to_files[d].append(filename)

# ----------------------------------------------------------------------------
# HTML templates (dark theme)
# ----------------------------------------------------------------------------

def html_header(title):
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{html.escape(title)}</title>
<style>
body {{
    font-family: system-ui, sans-serif;
    max-width: 1000px;
    margin: 40px auto;
    background: #111;
    color: #eee;
}}
.container {{
    background: #1a1a1a;
    padding: 30px;
    border-radius: 8px;
    border: 1px solid #333;
}}
h1, h2 {{
    color: #66ccff;
}}
a {{
    color: #66aaff;
    text-decoration: none;
}}
.card {{
    background: #222;
    padding: 20px;
    margin-bottom: 25px;
    border-radius: 8px;
    border: 1px solid #333;
}}
img {{
    max-width: 100%;
    border-radius: 6px;
    border: 1px solid #333;
    margin-bottom: 20px;
}}
table {{
    width: 100%;
    border-collapse: collapse;
}}
th, td {{
    padding: 10px;
    border-bottom: 1px solid #333;
}}
th {{
    background: #222;
}}
.badge {{
    background: #28a745;
    color: white;
    padding: 3px 6px;
    border-radius: 3px;
    font-size: 12px;
}}
.footer {{
    margin-top: 30px;
    color: #aaa;
    font-size: 14px;
    text-align: center;
}}
select {{
    background: #222;
    color: #eee;
    border: 1px solid #444;
    padding: 5px 8px;
    border-radius: 4px;
}}
</style>
<script>
function goToDevice(url) {{
    if (url) {{
        window.location.href = url;
    }}
}}
</script>
</head>
<body>
<div class="container">
"""

def html_footer():
    return f"""
<div class="footer">
Generated {utc_now_str()}
</div>
</div>
</body>
</html>
"""

# ----------------------------------------------------------------------------
# Generate index.html (dashboard + CSV table + device selector)
# ----------------------------------------------------------------------------

rows = []
for fname in csv_files:
    meta = file_meta[fname]
    badge = '<span class="badge">LATEST</span>' if fname == "latest.csv" else ""
    rows.append(f"""
<tr>
<td>{html.escape(fname)} {badge}</td>
<td>{meta['last_ts']}</td>
<td>{meta['count']}</td>
<td>{meta['size']}</td>
<td><a href="data/{html.escape(fname)}" download>Download</a></td>
</tr>
""")

# Device selector options
device_options = ["<option value=''>Select a device…</option>"]
for device in sorted(device_to_files.keys()):
    safe = safe_device_name(device)
    device_options.append(
        f"<option value='devices/{safe}.html'>{html.escape(device)}</option>"
    )
device_select_html = (
    "<select onchange=\"goToDevice(this.value)\">"
    + "".join(device_options) +
    "</select>"
)

index_html = (
    html_header("LoRaWAN Weather Station Data") +
    "<h1>LoRaWAN Weather Station Dashboard</h1>"

    # Device selector UI
    "<div class='card'>"
    "<h2>Devices</h2>"
    "<p>Select a device to view its data and plots:</p>"
    f"{device_select_html}"
    "<p><a href='devices/index.html'>Or browse all devices</a></p>"
    "</div>"

    # Global dashboard plots
    "<div class='card'>"
    "<h2>Latest Global Plots</h2>"
    "<p>Automatically updated every 30 minutes.</p>"
    "<img src='data/plot_temp_humidity.png' alt='Temperature and Humidity'>"
    "<img src='data/plot_battery.png' alt='Battery Voltage'>"
    "<img src='data/plot_dewpoint.png' alt='Dew Point'>"
    "</div>"

    # CSV table
    "<div class='card'>"
    "<h2>Data Files</h2>"
    "<table>"
    "<tr><th>File</th><th>Last timestamp</th><th>Records</th><th>Size</th><th></th></tr>"
    + "".join(rows) +
    "</table>"
    "<p><a href='devices/index.html'>Browse per-device downloads</a></p>"
    "</div>"
    + html_footer()
)

with open("index.html.tmp", "w", encoding="utf-8") as f:
    f.write(index_html)
os.replace("index.html.tmp", "index.html")

# ----------------------------------------------------------------------------
# Generate per-device pages (with interactive + static plots)
# ----------------------------------------------------------------------------

device_links = []

for device, files in sorted(device_to_files.items()):
    device_safe = html.escape(device)
    safe = safe_device_name(device)
    rows = []

    for fname in files:
        meta = file_meta[fname]
        rows.append(f"""
<tr>
<td>{html.escape(fname)}</td>
<td>{meta['last_ts']}</td>
<td>{meta['count']}</td>
<td>{meta['size']}</td>
<td><a href="../data/{html.escape(fname)}" download>Download</a></td>
</tr>
""")

    page = (
        html_header(f"Device {device_safe}") +
        f"<h1>Device: {device_safe}</h1>"

        # Interactive plots
        "<div class='card'>"
        "<h2>Interactive Plots</h2>"
        f"<iframe src=\"../data/{safe}_temp_humidity.html\" width=\"100%\" height=\"400\"></iframe>"
        f"<iframe src=\"../data/{safe}_battery.html\" width=\"100%\" height=\"300\"></iframe>"
        f"<iframe src=\"../data/{safe}_dewpoint.html\" width=\"100%\" height=\"300\"></iframe>"
        "</div>"

        # Static PNG plots
        "<div class='card'>"
        "<h2>Static Plots (PNG)</h2>"
        f"<img src=\"../data/{safe}_temp_humidity.png\" alt=\"Temperature and Humidity\">"
        f"<img src=\"../data/{safe}_battery.png\" alt=\"Battery Voltage\">"
        f"<img src=\"../data/{safe}_dewpoint.png\" alt=\"Dew Point\">"
        "</div>"

        # CSV table
        "<div class='card'>"
        "<h2>Data Files for this Device</h2>"
        "<table>"
        "<tr><th>File</th><th>Last timestamp</th><th>Records</th><th>Size</th><th></th></tr>"
        + "".join(rows) +
        "</table>"
        "<p><a href='../index.html'>← Back to overview</a></p>"
        "</div>"
        + html_footer()
    )

    path = os.path.join(DEVICES_DIR, f"{safe}.html")
    with open(path + ".tmp", "w", encoding="utf-8") as f:
        f.write(page)
    os.replace(path + ".tmp", path)

    device_links.append(f"<li><a href='{safe}.html'>{device_safe}</a></li>")

# ----------------------------------------------------------------------------
# Generate devices/index.html
# ----------------------------------------------------------------------------

devices_index = (
    html_header("Devices") +
    "<h1>Devices</h1><ul>"
    + "\n".join(device_links) +
    "</ul><p><a href='../index.html'>← Back to overview</a></p>"
    + html_footer()
)

with open(os.path.join(DEVICES_DIR, "index.html.tmp"), "w", encoding="utf-8") as f:
    f.write(devices_index)
os.replace(
    os.path.join(DEVICES_DIR, "index.html.tmp"),
    os.path.join(DEVICES_DIR, "index.html")
)

print("Static HTML pages generated successfully.")

