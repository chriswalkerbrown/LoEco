# ============================================================================
# generate_index.py — Build a professional dashboard index.html
# ============================================================================
import os
from datetime import datetime, timezone

DATA_DIR = "data"
OUTPUT_FILE = "index.html"

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
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
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
            color: #333;
            font-size: 2.5em;
            margin-bottom: 10px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }
        
        .timestamp {
            color: #666;
            font-size: 0.9em;
            margin-bottom: 40px;
            padding-bottom: 20px;
            border-bottom: 2px solid #f0f0f0;
        }
        
        .section {
            margin: 40px 0;
        }
        
        h2 {
            color: #444;
            font-size: 1.8em;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 3px solid #667eea;
        }
        
        .cards {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(250px, 1fr));
            gap: 20px;
            margin-top: 20px;
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
        
        @media (max-width: 768px) {
            .container {
                padding: 20px;
            }
            
            h1 {
                font-size: 2em;
            }
            
            h2 {
                font-size: 1.5em;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🌤️ Weather Data Dashboard</h1>
        <p class="timestamp">Last updated: {{timestamp}}</p>
        
        <div class="section">
            <h2>📊 Interactive Plots</h2>
            <div class="cards">
                {{plot_cards}}
            </div>
        </div>
        
        <div class="section">
            <h2>📁 Data Files</h2>
            <div class="cards">
                {{data_cards}}
            </div>
        </div>
    </div>
</body>
</html>"""

# ----------------------------------------------------------------------------
def make_card(filename, label=None):
    """Return an HTML card for a file."""
    label = label or filename
    return f'<div class="card"><a href="data/{filename}" target="_blank">{label}</a></div>'

# ----------------------------------------------------------------------------
def main():
    # Check if data directory exists
    if not os.path.exists(DATA_DIR):
        print(f"Warning: {DATA_DIR} directory not found!")
        os.makedirs(DATA_DIR, exist_ok=True)
    
    # Timestamp
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    
    # Debugging: show what we're finding
    print(f"Looking in directory: {os.path.abspath(DATA_DIR)}")
    all_files = os.listdir(DATA_DIR) if os.path.exists(DATA_DIR) else []
    print(f"All files found: {all_files}")
    
    # Collect plot files
    plot_files = [
        f for f in all_files
        if f.endswith(".html") and f.startswith("plot_")
    ]
    print(f"Plot files: {plot_files}")
    
    if plot_files:
        plot_cards = "\n".join(
            make_card(f, f.replace("plot_", "").replace(".html", "").replace("_", " ").title())
            for f in sorted(plot_files)
        )
    else:
        plot_cards = '<div class="empty-state">No plots available yet. Run generate_plot.py first.</div>'
    
    # Collect data files
    data_files = [
        f for f in all_files
        if f.endswith(".parquet")
    ]
    print(f"Data files: {data_files}")
    
    if data_files:
        data_cards = "\n".join(make_card(f) for f in sorted(data_files))
    else:
        data_cards = '<div class="empty-state">No data files available yet. Run fetch_data.py first.</div>'
    
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
