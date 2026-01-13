# config.py

DEV_MODE = False
DEV_TICKERS_LIMIT = 200

MIN_MARKET_CAP = 1_000_000_000  # $1B
PRIORITY_TOP_STOCKS = 1000

MAX_TICKERS_PER_RUN = 400
PRIORITY_PER_RUN = 250
ROTATION_PER_RUN = MAX_TICKERS_PER_RUN - PRIORITY_PER_RUN

# Always include these ETFs every run
CORE_ETFS = [
    "SPY", "QQQ", "IWM", "DIA",
    "XLK", "XLF", "XLE", "XLY", "XLP", "XLU", "XLV", "XLI",
    "SMH", "ARKK", "TLT", "GLD",
]

# Intraday windows: keep tight (prevents Yahoo 730-day errors)
DEV_YF_BASE_FEEDS = {"1d": {"period": "max"}, "60m": {"period": "60d"}}
YF_BASE_FEEDS = {"1d": {"period": "max"}, "60m": {"period": "60d"}}

REQUEST_TIMEOUT_SEC = 20

UNIVERSE_CACHE_TTL_SEC = 24 * 3600
CACHE_TTL = {"1d": 12 * 3600, "60m": 60 * 60}

SNAPSHOT_PATH = "cache/snapshots/latest.json"
RESULTS_CSV_PATH = "cache/results/latest.csv"
RESULTS_JSON_PATH = "cache/results/latest.json"

# =========================
# YOUR 11 SECTORS (GICS)
# =========================
SECTORS_11 = [
    "Communication Services",
    "Consumer Discretionary",
    "Consumer Staples",
    "Energy",
    "Financials",
    "Health Care",
    "Industrials",
    "Information Technology",
    "Materials",
    "Real Estate",
    "Utilities",
]

# 2–3 top ETFs to show next to sector name in Sectors tab
SECTOR_TOP_ETFS = {
    "Communication Services": ["XLC", "VOX", "IYZ"],
    "Consumer Discretionary": ["XLY", "VCR", "FDIS"],
    "Consumer Staples": ["XLP", "VDC", "FSTA"],
    "Energy": ["XLE", "VDE", "FENY"],
    "Financials": ["XLF", "VFH", "FNCL"],
    "Health Care": ["XLV", "VHT", "FHLC"],
    "Industrials": ["XLI", "VIS", "FIDU"],
    "Information Technology": ["XLK", "VGT", "FTEC"],
    "Materials": ["XLB", "VAW", "FMAT"],
    "Real Estate": ["XLRE", "VNQ", "IYR"],
    "Utilities": ["XLU", "VPU", "FUTY"],
}

# Optional: map “specialty” ETFs into one of the 11 sectors for filtering only
# (ETFs themselves don't need sector, but helps when users filter by sector + ETF)
ETF_TO_SECTOR_11 = {
    "SMH": "Information Technology",   # semis are a sub-industry inside IT
    "QQQ": "Information Technology",   # dominated by tech/growth; for filtering convenience
    "SPY": "Communication Services",   # not used for sector tagging stocks; only optional for ETF row
    "ARKK": "Information Technology",  # innovation-heavy; assign to IT for filtering convenience
    "TLT": "Financials",               # bonds/rates proxy; doesn't fit 11 well — keep but harmless
    "GLD": "Materials",                # gold/commodities proxy
}
