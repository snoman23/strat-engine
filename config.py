# config.py

# ---------- MODE ----------
# DEV_MODE affects how many tickers we scan and how aggressive we are with I/O.
DEV_MODE = False
DEV_TICKERS_LIMIT = 200  # only used when DEV_MODE=True

# Universe mode: "DEV" or "FULL"
UNIVERSE_MODE = "FULL"

# Minimum market cap filter
MIN_MARKET_CAP = 10_000_000  # $10M

# ---------- DATA FEEDS ----------
# Feeds we load from Yahoo (then we resample locally only when needed).
YF_BASE_FEEDS = {
    "1d": {"period": "max"},
    "60m": {"period": "730d"},
}

# ---------- CACHE ----------
# Cache TTLs (seconds) for downloaded OHLC
CACHE_TTL = {
    "1d": 12 * 3600,     # 12 hours
    "60m": 2 * 3600,     # 2 hours
    "30m": 1 * 3600,
    "15m": 30 * 60,
    "10m": 20 * 60,
    "5m": 15 * 60,
}

# Universe cache paths
UNIVERSE_CACHE_DIR = "cache/universe"
UNIVERSE_SYMBOLS_PATH = "cache/universe/symbols.csv"
UNIVERSE_MARKETCAP_PATH = "cache/universe/marketcap.csv"

# Scanner output paths (what Streamlit reads)
RESULTS_DIR = "cache/results"
LATEST_CSV_PATH = "cache/results/latest.csv"
LATEST_JSON_PATH = "cache/results/latest.json"
