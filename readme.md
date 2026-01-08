loeco/
├── providers/
│   ├── __init__.py
│   ├── base_provider.py       # Abstract base class
│   ├── ttn_provider.py         # Your existing TTN LoRaWAN provider
│   └── ecowitt_provider.py     # New Ecowitt provider
├── config/
│   └── stations.json           # Provider configuration
├── data/
│   ├── loeco_ttn/              # TTN data (existing structure maintained)
│   │   ├── latest.parquet
│   │   ├── weather_data_2026_01_January.parquet
│   │   └── ...
│   └── ecowitt_home/           # Ecowitt data (separate folder)
│       └── latest.parquet
├── fetch_all_data.py           # New orchestrator (replaces fetch_data.py)
├── fetch_data.py               # Legacy script (keep for backwards compat)
├── generate_plot.py            # Updated to support multiple providers
├── generate_index.py
├── requirements.txt
└── Dockerfile

