# config.py

# =========================
# ENV / MODE
# =========================
# False for production (GitHub Actions + Streamlit Cloud)
DEV_MODE = False

# If DEV_MODE True, limit number of tickers scanned
DEV_TICKERS_LIMIT = 200

# =========================
# UNIVERSE SETTINGS
# =========================
MIN_MARKET_CAP = 1_000_000_000  # $1B
PRIORITY_TOP_STOCKS = 1000

# Per-run performance control (GitHub Actions runtime)
MAX_TICKERS_PER_RUN = 400
PRIORITY_PER_RUN = 250
ROTATION_PER_RUN = MAX_TICKERS_PER_RUN - PRIORITY_PER_RUN

# =========================
# YAHOO FINANCE BASE FEEDS
# =========================
# IMPORTANT:
# - 1d can be "max"
# - 60m is fragile beyond ~60–90 days via yfinance.
#   "730d" often errors even though Yahoo advertises 730-day range.
#
# We keep 60m tight to make the scanner reliable and fast.
DEV_YF_BASE_FEEDS = {
    "1d": {"period": "max"},
    "60m": {"period": "60d"},
}

YF_BASE_FEEDS = {
    "1d": {"period": "max"},
    "60m": {"period": "60d"},
}

# =========================
# CACHING / TTL
# =========================
UNIVERSE_CACHE_TTL_SEC = 24 * 3600  # 24 hours

CACHE_TTL = {
    "1d": 12 * 3600,   # 12 hours
    "60m": 60 * 60,    # 1 hour (scanner runs hourly)
}

# =========================
# SNAPSHOT OUTPUTS
# =========================
SNAPSHOT_PATH = "cache/snapshots/latest.json"
RESULTS_CSV_PATH = "cache/results/latest.csv"
RESULTS_JSON_PATH = "cache/results/latest.json"

# =========================
# SAFETY LIMITS (prevents “one ticker took 20 minutes”)
# =========================
# These are used by loaders/main (you’ll wire them in once; safe defaults here).
REQUEST_TIMEOUT_SEC = 20          # per Yahoo request
MAX_SECONDS_PER_TICKER = 12       # per ticker overall
MAX_FAILED_TICKERS_PER_RUN = 40   # stop early if Yahoo is down
