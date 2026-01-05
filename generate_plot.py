import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import os

# Ensure output directory exists
os.makedirs("data", exist_ok=True)

# ---------------------------
# Dew point calculation
# ---------------------------
def dew_point(temp_c, rh):
    """
    Magnus formula for dew point (°C)
    """
    a = 17.62
    b = 243.12
    gamma = (a * temp_c / (b + temp_c)) + np.log(rh / 100.0)
    return (b * gamma) / (a - gamma)

# ---------------------------
# Plot 1: Temperature + Humidity
# ---------------------------
def plot_temp_humidity(df):
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

    # Legend
    lines1, labels1 = ax1.get_legend_handles_labels()
    ax1.legend(lines1, labels1, loc="upper left", facecolor="#222", edgecolor="#444")

    ax1.grid(True, linestyle="--", alpha=0.3)
    fig.tight_layout()
    fig.savefig("data/plot_temp_humidity.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

# ---------------------------
# Plot 2: Battery Voltage
# ---------------------------
def plot_battery(df):
    fig, ax = plt.subplots(figsize=(12, 4), facecolor="#111")

    ax.plot(df.index, df["BatV"], color="#ffff66", lw=2)
    ax.set_title("Battery Voltage", color="white")
    ax.set_ylabel("Voltage (V)", color="white")
    ax.set_xlabel("Time (UTC)", color="white")
    ax.tick_params(axis="both", colors="white")

    ax.grid(True, linestyle="--", alpha=0.3)
    fig.tight_layout()
    fig.savefig("data/plot_battery.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

# ---------------------------
# Plot 3: Dew Point vs Dry Bulb
# ---------------------------
def plot_dewpoint(df):
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
    fig.savefig("data/plot_dewpoint.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

# ---------------------------
# Main
# ---------------------------
df = pd.read_csv("data/latest.csv", parse_dates=["time"], index_col="time")

# Compute dew point
df["DewPoint"] = dew_point(df["TempC_SHT"], df["Hum_SHT"])

plot_temp_humidity(df)
plot_battery(df)
plot_dewpoint(df)

print("Plots generated.")
