# ============================================================================
# generate_index.py — Build a professional dashboard index.html
# ============================================================================

import os
from datetime import datetime, timezone

DATA_DIR = "data"
OUTPUT_FILE = "index.html"

# ----------------------------------------------------------------------------
# FULL HTML TEMPLATE (embedded)
# ----------------------------------------------------------------------------

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>LoEco Weather Dashboard</title>
<meta name="viewport" content="width=device-width, initial-scale=1">

<style>
    body {
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
        margin: 0;
        background: #f5f7fa;
        color: #333;
    }

    header {
        background: #1f2937;
        color: white;
        padding: 20px 30px;
        text-align: center;
    }

    h1 {
        margin: 0;
        font-size: 2rem;
        font-weight: 600;
    }

    .timestamp {
        margin-top: 5px;
        font-size: 0.9rem;
        opacity: 0.8;
    }

    .container {
        max-width: 1200px;
        margin: 30px auto;
        padding: 0 20px;
    }

    .section-title {
        font-size: 1.4rem;
        margin: 30px 0 10px;
        font-weight: 600;
        color: #111827;
    }

    .card-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
        gap: 20px;
    }

    .card {
        background: white;
        border-radius: 10px;
        padding: 15px;
        box-shadow: 0 2px 6px rgba(0,0,0,0.08);
        transition: transform 0.15s ease;
    }

    .card:hover {
        transform: translateY(-3px);
    }

    .card a {
        text-decoration: none;
        color: #2563eb;
        font-weight: 500;
    }

    footer {
        text-align: center;
        padding: 20px;
        margin-top: 40px;
        color: #6b7280;
        font-size: 0.9rem;
    }
</style>
</head>

<body>

<header>
    <h1>LoEco Weather Dashboard</h1>
    <div class="timestamp">Last updated: {{timestamp}}</div>
</header>

<div class="container">

    <div class="section-title">Interactive Plots</div>
    <div class="card-grid">
        {{plot_cards}}
    </div>

    <div class="section-title">Download Data</div>
    <div class="card-grid">
        {{data_cards}}
    </div>

</div>

<footer>
    LoEco Weather Monitoring — Powered by TTN & GitHub Actions
</footer>

</body>
</html>
"""

# ----------------------------------------------------------------------------

def make_card(filename, label=None):
    """Return an HTML card for a file."""
    label = label or filename
    return f"""
    <div class="card">
        <a href="data/{filename}" target="_blank">{label}</a>
    </div>
    """

# ----------------------------------------------------------------------------

def main():
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    plot_files = [
        f for f in os.listdir(DATA_DIR)
        if f.endswith(".html") and f.startswith("plot_")
    ]

    plot_cards = "\n".join(
        make_card(
            f,
            f.replace("plot_", "")
             .replace(".html", "")
             .replace("_", " ")
             .title()
        )
        for f in sorted(plot_files)
    )

    data_files = [
        f for f in os.listdir(DATA_DIR)
        if f.endswith(".parquet")
    ]

    data_cards = "\n".join(make_card(f) for f in sorted(data_files))

    html = (
        HTML_TEMPLATE
        .replace("{{timestamp}}", timestamp)
        .replace("{{plot_cards}}", plot_cards)
        .replace("{{data_cards}}", data_cards)
    )

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"✓ Updated {OUTPUT_FILE}")

# ----------------------------------------------------------------------------

if __name__ == "__main__":
    main()

