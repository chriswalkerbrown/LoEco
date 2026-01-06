# ============================================================================
# generate_index.py — Build a professional dashboard index.html
# ============================================================================

import os
from datetime import datetime, timezone

DATA_DIR = "data"
OUTPUT_FILE = "index.html"

TEMPLATE = "index_template.html"  # We'll embed the template below instead

# ----------------------------------------------------------------------------

HTML_TEMPLATE =  """<html> ... </html>"""


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
    # Timestamp
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # Collect plot files
    plot_files = [
        f for f in os.listdir(DATA_DIR)
        if f.endswith(".html") and f.startswith("plot_")
    ]

    plot_cards = "\n".join(make_card(f, f.replace("plot_", "").replace(".html", "").replace("_", " ").title())
                           for f in sorted(plot_files))

    # Collect data files
    data_files = [
        f for f in os.listdir(DATA_DIR)
        if f.endswith(".parquet")
    ]

    data_cards = "\n".join(make_card(f) for f in sorted(data_files))

    # Build final HTML
    html = HTML_TEMPLATE.replace("{{timestamp}}", timestamp)
    html = html.replace("{{plot_cards}}", plot_cards)
    html = html.replace("{{data_cards}}", data_cards)

    # Write output
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"✓ Updated {OUTPUT_FILE}")

# ----------------------------------------------------------------------------

if __name__ == "__main__":
    main()
