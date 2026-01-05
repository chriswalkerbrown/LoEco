# ============================================================================
# generate_plot.py — PNG + interactive Plotly plots (global + per-device)
# ============================================================================

import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import plotly.graph_objects as go
import plotly.io as pio

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

# ----------------------------------------------------------------------------
# Dew point calculation
# ----------------------------------------------------------------------------

def dew_point(temp_c, rh):
    """
    Magnus formula for dew point (°C).
    temp_c: array-like of temperatures in °C
    rh: array-like of relative humidity in %
    """
    a = 17.62
    b = 243.12
    rh_clipped = np.clip(rh, 1, 100)  # avoid log(0)
    gamma = (a * temp_c / (b + temp_c)) + np.log(rh_clipped / 100.0)
    return (b * gamma) / (a - gamma)

# ----------------------------------------------------------------------------
# Matplotlib PNG plots
# ----------------------------------------------------------------------------

def plot_temp_humidity_png(df, outfile):
    if not {"TempC_DS", "TempC_SHT", "Hum_SHT"}.issubset(df.columns):
        print(f"Skipping temp/humidity PNG for {outfile}: missing columns")
        return

    fig, ax1 = plt.subplots(figsize=(12, 6), facecolor="#111")

    ax1.plot(df.index, df["TempC_DS"], color="#4da6ff", lw=2, label="Black Bulb (°C)")
    ax1.plot(df.index, df["TempC_SHT"], color="#ffa64d", lw=2, label="Dry Bulb (°C)")

    ax1.set_xlabel("Time (UTC)", color="white")
    ax1.set_ylabel("Temperature (°C)", color="white")
    ax1.tick_params(axis="both", colors="white")

    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%b %d %H:%M"))
    ax1.xaxis.set_major_locator(mdates.AutoDateLocator())
    fig.autofmt_xdate()

    ax2 = ax1.twinx()
    ax2.fill_between(df.index, df["Hum_SHT"], color="#66ff66", alpha=0.25, step="mid")
    ax2.set_ylabel("Relative Humidity (%)", color="#66ff66")
    ax2.tick_params(axis="y", colors="#66ff66")

    lines1, labels1 = ax1.get_legend_handles_labels()
    ax1.legend(lines1, labels1, loc="upper left", facecolor="#222", edgecolor="#444")

    ax1.grid(True, linestyle="--", alpha=0.3)
    fig.tight_layout()
    fig.savefig(outfile, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved PNG: {outfile}")


def plot_battery_png(df, outfile):
    if "BatV" not in df.columns:
        print(f"Skipping battery PNG for {outfile}: missing BatV")
        return

    fig, ax = plt.subplots(figsize=(12, 4), facecolor="#111")

    ax.plot(df.index, df["BatV"], color="#ffff66", lw=2)
    ax.set_title("Battery Voltage", color="white")
    ax.set_ylabel("Voltage (V)", color="white")
    ax.set_xlabel("Time (UTC)", color="white")
    ax.tick_params(axis="both", colors="white")

    ax.grid(True, linestyle="--", alpha=0.3)
    fig.tight_layout()
    fig.savefig(outfile, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved PNG: {outfile}")


def plot_dewpoint_png(df, outfile):
    if not {"TempC_SHT", "DewPoint"}.issubset(df.columns):
        print(f"Skipping dewpoint PNG for {outfile}: missing columns")
        return

    fig, ax = plt.subplots(figsize=(12, 4), facecolor="#111")

    ax.plot(df.index, df["TempC_SHT"], color="#ffa64d", lw=2, label="Dry Bulb (°C)")
    ax.plot(df.index, df["DewPoint"], color="#66ccff", lw=2, label="Dew Point (°C)")

    ax.set_title("Dew Point vs Dry Bulb", color="white")
    ax.set_ylabel("Temperature (°C)", color="white")
    ax.set_xlabel("Time (UTC)", color="white")
    ax.tick_params(axis="both", colors="white")

    ax.legend(facecolor="#222", edgecolor="#444")
    ax.grid(True, linestyle="--", alpha=0.3)
    fig.tight_layout()
    fig.savefig(outfile, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved PNG: {outfile}")

# ----------------------------------------------------------------------------
# Plotly interactive HTML plots
# ----------------------------------------------------------------------------

def plotly_temp_humidity_html(df, outfile):
    if not {"TempC_DS", "TempC_SHT", "Hum_SHT"}.issubset(df.columns):
        print(f"Skipping Plotly temp/humidity for {outfile}: missing columns")
        return

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df.index, y=df["TempC_DS"],
        mode="lines", name="Black Bulb (°C)"
    ))
    fig.add_trace(go.Scatter(
        x=df.index, y=df["TempC_SHT"],
        mode="lines", name="Dry Bulb (°C)"
    ))
    fig.add_trace(go.Scatter(
        x=df.index, y=df["Hum_SHT"],
        mode="lines", name="Humidity (%)",
        yaxis="y2"
    ))

    fig.update_layout(
        template="plotly_dark",
        yaxis=dict(title="Temperature (°C)"),
        yaxis2=dict(
            title="Humidity (%)",
            overlaying="y",
            side="right"
        ),
        title="Temperature & Humidity (Interactive)",
        margin=dict(l=40, r=40, t=40, b=40)
    )

    pio.write_html(fig, outfile, include_plotlyjs="cdn", full_html=True)
    print(f"Saved Plotly HTML: {outfile}")


def plotly_battery_html(df, outfile):
    if "BatV" not in df.columns:
        print(f"Skipping Plotly battery for {outfile}: missing BatV")
        return

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df.index, y=df["BatV"],
        mode="lines", name="Battery Voltage (V)"
    ))

    fig.update_layout(
        template="plotly_dark",
        yaxis=dict(title="Voltage (V)"),
        title="Battery Voltage (Interactive)",
        margin=dict(l=40, r=40, t=40, b=40)
    )

    pio.write_html(fig, outfile, include_plotlyjs="cdn", full_html=True)
    print(f"Saved Plotly HTML: {outfile}")


def plotly_dewpoint_html(df, outfile):
    if not {"TempC_SHT", "DewPoint"}.issubset(df.columns):
        print(f"Skipping Plotly dewpoint for {outfile}: missing columns")
        return

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df.index, y=df["TempC_SHT"],
        mode="lines", name="Dry Bulb (°C)"
    ))
    fig.add_trace(go.Scatter(
        x=df.index, y=df["DewPoint"],
        mode="lines", name="Dew Point (°C)"
    ))

    fig.update_layout(
        template="plotly_dark",
        yaxis=dict(title="Temperature (°C)"),
        title="Dew Point vs Dry Bulb (Interactive)",
        margin=dict(l=40, r=40, t=40, b=40)
    )

    pio.write_html(fig, outfile, include_plotlyjs="cdn", full_html=True)
    print(f"Saved Plotly HTML: {outfile}")

# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------

if __name__ == "__main__":
    latest_path = os.path.join(DATA_DIR, "latest.csv")
    if not os.path.exists(latest_path):
        raise FileNotFoundError(f"{latest_path} not found. Run fetch_data.py first.")

    df = pd.read_csv(latest_path, parse_dates=["time"], index_col="time")

    # Compute dew point where possible
    if {"TempC_SHT", "Hum_SHT"}.issubset(df.columns):
        df["DewPoint"] = dew_point(df["TempC_SHT"].astype(float),
                                   df["Hum_SHT"].astype(float))

    # Global plots (all devices combined)
    plot_temp_humidity_png(df, os.path.join(DATA_DIR, "plot_temp_humidity.png"))
    plot_battery_png(df, os.path.join(DATA_DIR, "plot_battery.png"))
    if "DewPoint" in df.columns:
        plot_dewpoint_png(df, os.path.join(DATA_DIR, "plot_dewpoint.png"))

    # Per-device plots
    if "device_id" in df.columns:
        for dev, df_dev in df.groupby("device_id"):
            safe = dev.replace(" ", "_")

            # Ensure dew point for this subset
            if {"TempC_SHT", "Hum_SHT"}.issubset(df_dev.columns):
                df_dev = df_dev.copy()
                df_dev["DewPoint"] = dew_point(df_dev["TempC_SHT"].astype(float),
                                               df_dev["Hum_SHT"].astype(float))

            # PNGs
            plot_temp_humidity_png(df_dev, os.path.join(DATA_DIR, f"{safe}_temp_humidity.png"))
            plot_battery_png(df_dev, os.path.join(DATA_DIR, f"{safe}_battery.png"))
            if "DewPoint" in df_dev.columns:
                plot_dewpoint_png(df_dev, os.path.join(DATA_DIR, f"{safe}_dewpoint.png"))

            # Plotly HTMLs
            plotly_temp_humidity_html(df_dev, os.path.join(DATA_DIR, f"{safe}_temp_humidity.html"))
            plotly_battery_html(df_dev, os.path.join(DATA_DIR, f"{safe}_battery.html"))
            if "DewPoint" in df_dev.columns:
                plotly_dewpoint_html(df_dev, os.path.join(DATA_DIR, f"{safe}_dewpoint.html"))

    print("All plots generated.")
