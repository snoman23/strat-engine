# config.py

# =========================
# ENV / MODE
# =========================
# Set to False for production (Streamlit Cloud / GitHub Actions)
DEV_MODE = False

# If DEV_MODE True, limit the number of tickers scanned
DEV_TICKERS_LIMIT = 200

# =========================
# UNIVERSE SETTINGS
# =========================
# You asked to cap at 1B instead of 10M
MIN_MARKET_CAP = 1_000_000_000  # $1B

# "Top 1000 by market cap" priority bucket
PRIORITY_TOP_STOCKS = 1000

# Per-run performance control (important for GitHub Actions + Streamlit)
# If you scan too many tickers, the workflow will take forever / timeout.
MAX_TICKERS_PER_RUN = 400

# Always include this many from the Top 1000 bucket per run (rest is rotating expansion)
PRIORITY_PER_RUN = 250

# How many "new" tickers to rotate in per run from the remaining universe
ROTATION_PER_RUN = MAX_TICKERS_PER_RUN - PRIORITY_PER_RUN

# =========================
# YF BASE FEEDS
# =========================
# Keep this tight so runs don't explode in time.
# Your engine resamples from 60m and 1d for higher timeframes.
DEV_YF_BASE_FEEDS = {
    "1d": {"period": "max"},
    "60m": {"period": "60d"},
}

# For prod, you can keep the same feeds unless you add lower timeframes later.
YF_BASE_FEEDS = {
    "1d": {"period": "max"},
    "60m": {"period": "730d"},
}

# =========================
# CACHE / TTL
# =========================
UNIVERSE_CACHE_TTL_SEC = 24 * 3600  # 24 hours

CACHE_TTL = {
    "1d": 12 * 3600,     # 12 hours
    "60m": 2 * 3600,     # 2 hours
}

# Snapshot file (latest results)
SNAPSHOT_PATH = "cache/snapshots/latest.json"
RESULTS_CSV_PATH = "cache/results/latest.csv"
RESULTS_JSON_PATH = "cache/results/latest.json"
