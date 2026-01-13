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

# =========================
# CORE ETFs (always include)
# =========================
CORE_ETFS = [
    "SPY", "QQQ", "IWM", "DIA",
    "SMH", "XLK", "XLF", "XLE",
    "XLY", "XLP", "XLU", "XLV", "XLI",
    "ARKK", "TLT", "GLD",
]

# =========================
# ETF → Sector mapping (used for filters + heatmap)
# This is a practical taxonomy for your scanner (fast & free).
# =========================
ETF_SECTOR_MAP = {
    "SPY": "Broad Market",
    "QQQ": "Technology (Large Cap Growth)",
    "IWM": "Small Caps",
    "DIA": "Large Caps",

    "SMH": "Semiconductors",
    "XLK": "Technology",
    "XLF": "Financials",
    "XLE": "Energy",
    "XLY": "Consumer Discretionary",
    "XLP": "Consumer Staples",
    "XLU": "Utilities",
    "XLV": "Health Care",
    "XLI": "Industrials",

    "ARKK": "Innovation / High Beta",
    "TLT": "Rates / Bonds",
    "GLD": "Gold / Commodities",
}

# =========================
# YF BASE FEEDS
# IMPORTANT: keep intraday tight (prevents 730-day Yahoo errors)
# =========================
DEV_YF_BASE_FEEDS = {
    "1d": {"period": "max"},
    "60m": {"period": "60d"},
}

YF_BASE_FEEDS = {
    "1d": {"period": "max"},
    "60m": {"period": "60d"},
}

# =========================
# NETWORK / TIMEOUTS
# =========================
REQUEST_TIMEOUT_SEC = 20

# =========================
# CACHE / TTL
# =========================
UNIVERSE_CACHE_TTL_SEC = 24 * 3600  # 24 hours

CACHE_TTL = {
    "1d": 12 * 3600,
    "60m": 60 * 60,  # 1 hour
}

# =========================
# OUTPUT PATHS
# =========================
SNAPSHOT_PATH = "cache/snapshots/latest.json"
RESULTS_CSV_PATH = "cache/results/latest.csv"
RESULTS_JSON_PATH = "cache/results/latest.json"
