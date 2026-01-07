# config.py

# =========================
# ENV / MODE
# =========================
DEV_MODE = False
DEV_TICKERS_LIMIT = 200

# =========================
# UNIVERSE SETTINGS
# =========================
MIN_MARKET_CAP = 1_000_000_000  # $1B
PRIORITY_TOP_STOCKS = 1000

MAX_TICKERS_PER_RUN = 400
PRIORITY_PER_RUN = 250
ROTATION_PER_RUN = MAX_TICKERS_PER_RUN - PRIORITY_PER_RUN

# Always include these ETFs every run
CORE_ETFS = [
    "SPY", "QQQ", "IWM", "DIA",
    "SMH", "XLK", "XLF", "XLE",
    "XLY", "XLP", "XLU", "XLV", "XLI",
    "ARKK", "TLT", "GLD",
]

# =========================
# YF BASE FEEDS
# =========================
DEV_YF_BASE_FEEDS = {
    "1d": {"period": "max"},
    "60m": {"period": "60d"},
}

YF_BASE_FEEDS = {
    "1d": {"period": "max"},
    "60m": {"period": "730d"},
}

# =========================
# NETWORK / TIMEOUTS
# =========================
# yfinance can hang on some tickers; keep a reasonable cap for GitHub Actions
REQUEST_TIMEOUT_SEC = 20

# =========================
# CACHE / TTL
# =========================
UNIVERSE_CACHE_TTL_SEC = 24 * 3600  # 24 hours

CACHE_TTL = {
    "1d": 12 * 3600,
    "60m": 2 * 3600,
}

# =========================
# OUTPUT PATHS
# =========================
SNAPSHOT_PATH = "cache/snapshots/latest.json"
RESULTS_CSV_PATH = "cache/results/latest.csv"
RESULTS_JSON_PATH = "cache/results/latest.json"
