# providers/schema.py

"""
LoEco Universal Schema Definition
---------------------------------
This schema defines the full set of normalized observation fields used across
all providers (TTN, Ecowitt, future sensors). Every Parquet file produced by
LoEco must contain ALL of these columns, even if many are null.

This ensures:
- Stable long-term storage
- Provider-agnostic downstream processing
- Zero-cost schema evolution
- Efficient Parquet compression (null-heavy columns compress extremely well)
"""

LOECO_SCHEMA = [

    # ----------------------------------------------------------------------
    # Metadata
    # ----------------------------------------------------------------------
    "schema_version",
    "timestamp",
    "provider",
    "station",
    "latitude",
    "longitude",
    "sensor_type",
    "height_m",

    # ----------------------------------------------------------------------
    # Primary weather - Temperature & Humidity
    # ----------------------------------------------------------------------
    "temperature_c",
    "humidity_pct",
    "dewpoint_c",
    "wet_bulb_temperature_c",
    "feels_like_c",
    "heat_index_c",
    "wind_chill_c",
    "thw_index_c",
    "thsw_index_c",
    "black_bulb_temperature_c",

    # ----------------------------------------------------------------------
    # Primary weather - Pressure
    # ----------------------------------------------------------------------
    "pressure_hpa",
    "vapor_pressure_hpa",

    # ----------------------------------------------------------------------
    # Primary weather - Wind
    # ----------------------------------------------------------------------
    "wind_speed_ms",
    "wind_gust_ms",
    "wind_dir_deg",
    "wind_speed_avg_10min_ms",
    "wind_dir_avg_10min_deg",

    # ----------------------------------------------------------------------
    # Primary weather - Precipitation
    # ----------------------------------------------------------------------
    "rain_mm",
    "rain_rate_mmhr",
    "rain_daily_mm",
    "rain_event_mm",
    "precipitation_type",
    "precipitation_type_code",

    # ----------------------------------------------------------------------
    # Primary weather - Solar & UV
    # ----------------------------------------------------------------------
    "solar_wm2",
    "solar_radiation_perceived_wm2",
    "uv_index",
    "lux",

    # ----------------------------------------------------------------------
    # Primary weather - Visibility & ET
    # ----------------------------------------------------------------------
    "visibility_m",
    "et_mm",
    "et_daily_mm",

    # ----------------------------------------------------------------------
    # Indoor conditions
    # ----------------------------------------------------------------------
    "temperature_indoor_c",
    "humidity_indoor_pct",
    "pressure_indoor_hpa",
    "co2_indoor_ppm",

    # ----------------------------------------------------------------------
    # Extra temperatures
    # ----------------------------------------------------------------------
    "temperature_1_c",
    "temperature_2_c",
    "temperature_3_c",
    "temperature_4_c",
    "temperature_5_c",
    "water_temperature_c",

    # ----------------------------------------------------------------------
    # Extra humidity
    # ----------------------------------------------------------------------
    "humidity_1_pct",
    "humidity_2_pct",
    "humidity_3_pct",
    "humidity_4_pct",
    "humidity_5_pct",

    # ----------------------------------------------------------------------
    # Soil temperature
    # ----------------------------------------------------------------------
    "soil_temperature_1_c",
    "soil_temperature_2_c",
    "soil_temperature_3_c",
    "soil_temperature_4_c",
    "soil_temperature_5_c",

    # ----------------------------------------------------------------------
    # Soil moisture
    # ----------------------------------------------------------------------
    "soil_moisture_1_pct",
    "soil_moisture_2_pct",
    "soil_moisture_3_pct",
    "soil_moisture_4_pct",
    "soil_moisture_5_pct",

    # ----------------------------------------------------------------------
    # Soil advanced (professional sensors)
    # ----------------------------------------------------------------------
    "soil_ec_dsm",
    "soil_water_potential_kpa",
    "soil_salinity_ppt",

    # ----------------------------------------------------------------------
    # Leaf wetness
    # ----------------------------------------------------------------------
    "leaf_wetness_1",
    "leaf_wetness_2",
    "leaf_wetness_3",
    "leaf_wetness_4",
    "leaf_wetness_5",

    # ----------------------------------------------------------------------
    # Air quality - Particulate matter
    # ----------------------------------------------------------------------
    "pm1_0_ugm3",
    "pm2_5_ugm3",
    "pm4_0_ugm3",
    "pm10_ugm3",

    # ----------------------------------------------------------------------
    # Air quality - Gases
    # ----------------------------------------------------------------------
    "co2_ppm",
    "co_ppm",
    "no2_ppb",
    "o3_ppb",
    "voc_index",
    "formaldehyde_mgm3",

    # ----------------------------------------------------------------------
    # Air quality - Index
    # ----------------------------------------------------------------------
    "aqi",

    # ----------------------------------------------------------------------
    # Lightning
    # ----------------------------------------------------------------------
    "lightning_strike_count",
    "lightning_distance_km",

    # ----------------------------------------------------------------------
    # Power & signal
    # ----------------------------------------------------------------------
    "battery_voltage_v",
    "battery_pct",
    "solar_panel_voltage_v",
    "signal_strength_dbm",
    "reception_quality_pct",

    # ----------------------------------------------------------------------
    # Status & diagnostics
    # ----------------------------------------------------------------------
    "sensor_status",
    "uptime_seconds",
    "transmission_failures",

    # ----------------------------------------------------------------------
    # Reserved future observation classes (20 fields)
    # ----------------------------------------------------------------------
    "obs_future_01",
    "obs_future_02",
    "obs_future_03",
    "obs_future_04",
    "obs_future_05",
    "obs_future_06",
    "obs_future_07",
    "obs_future_08",
    "obs_future_09",
    "obs_future_10",
    "obs_future_11",
    "obs_future_12",
    "obs_future_13",
    "obs_future_14",
    "obs_future_15",
    "obs_future_16",
    "obs_future_17",
    "obs_future_18",
    "obs_future_19",
    "obs_future_20",
]
