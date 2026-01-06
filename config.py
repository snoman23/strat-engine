# config.py

DEV_MODE = True
DEV_TICKERS_LIMIT = 10

MIN_MARKET_CAP = 10_000_000

# Which feeds to load in DEV (fast)
DEV_YF_BASE_FEEDS = {
    "1d": {"period": "max"},
    "60m": {"period": "730d"},
}

# Caching TTL (seconds)
CACHE_TTL = {
    "1d": 12 * 3600,     # 12 hours
    "60m": 2 * 3600,     # 2 hours
    "30m": 1 * 3600,
    "15m": 30 * 60,
    "5m": 15 * 60,
}

# Snapshot file (latest results)
SNAPSHOT_PATH = "cache/snapshots/latest.json"
