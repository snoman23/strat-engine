# universe/loader.py

from typing import List

# =========================
# CONFIG
# =========================

DEV_TICKERS = [
    "SPY", "QQQ", "IWM",
    "AAPL", "MSFT", "NVDA",
    "TSLA", "AMZN", "META",
    "ARKK"
]

# Modes:
# DEV  -> small fixed list (fast, for development)
# FULL -> dynamic universe (later)
UNIVERSE_MODE = "DEV"   # change to "FULL" later


# =========================
# PUBLIC INTERFACE
# =========================

def load_universe(min_market_cap: int = 10_000_000) -> List[str]:
    """
    Returns list of tickers to scan.
    DEV mode: fixed small list
    FULL mode: dynamic universe (implemented later)
    """

    if UNIVERSE_MODE == "DEV":
        return load_dev_universe()

    elif UNIVERSE_MODE == "FULL":
        return load_full_universe(min_market_cap)

    else:
        raise ValueError(f"Unknown UNIVERSE_MODE: {UNIVERSE_MODE}")


# =========================
# DEV MODE
# =========================

def load_dev_universe() -> List[str]:
    """
    Small, fast universe for development & debugging.
    """
    print("[Universe] DEV mode active")
    return DEV_TICKERS


# =========================
# FULL MODE (STUB FOR NOW)
# =========================

def load_full_universe(min_market_cap: int) -> List[str]:
    """
    Placeholder for full universe logic.
    Implemented later.
    """
    raise NotImplementedError(
        "FULL universe not implemented yet. "
        "Switch UNIVERSE_MODE to 'DEV' while developing."
    )
