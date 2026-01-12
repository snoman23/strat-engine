# config.py

DEV_MODE = False
DEV_TICKERS_LIMIT = 200

MIN_MARKET_CAP = 1_000_000_000
PRIORITY_TOP_STOCKS = 1000

MAX_TICKERS_PER_RUN = 400
PRIORITY_PER_RUN = 250
ROTATION_PER_RUN = MAX_TICKERS_PER_RUN - PRIORITY_PER_RUN

CORE_ETFS = [
    "SPY", "QQQ", "IWM", "DIA",
    "SMH", "XLK", "XLF", "XLE",
    "XLY", "XLP", "XLU", "XLV", "XLI",
    "ARKK", "TLT", "GLD",
]

# FEEDS
DEV_YF_BASE_FEEDS = {
    "1d": {"period": "max"},
    "60m": {"period": "60d"},
}

# IMPORTANT: keep intraday tight (prevents 730-day Yahoo errors)
YF_BASE_FEEDS = {
    "1d": {"period": "max"},
    "60m": {"period": "60d"},
}

REQUEST_TIMEOUT_SEC = 20

UNIVERSE_CACHE_TTL_SEC = 24 * 3600

CACHE_TTL = {
    "1d": 12 * 3600,
    "60m": 60 * 60,   # 1 hour (workflow runs hourly)
}

SNAPSHOT_PATH = "cache/snapshots/latest.json"
RESULTS_CSV_PATH = "cache/results/latest.csv"
RESULTS_JSON_PATH = "cache/results/latest.json"
