# universe/loader.py

from __future__ import annotations

import json
import os
import re
import time
from typing import List

import pandas as pd

# -------------------------
# SAFE CONFIG IMPORTS
# -------------------------
# IMPORTANT:
# If any config key is missing or invalid (like DEV_MODE=false),
# we still want universe to load with defaults instead of crashing the whole scan.
try:
    import config as _cfg  # type: ignore
except Exception:
    _cfg = None  # noqa


def _cfg_get(name: str, default):
    if _cfg is None:
        return default
    return getattr(_cfg, name, default)


# Defaults (match what you’ve been asking for)
DEV_MODE = bool(_cfg_get("DEV_MODE", True))
DEV_TICKERS_LIMIT = int(_cfg_get("DEV_TICKERS_LIMIT", 10))

# You requested to cap it at $1B now (instead of $10M)
MIN_MARKET_CAP = int(_cfg_get("MIN_MARKET_CAP", 1_000_000_000))

# “Top 1000 by cap” priority pool (we still rotate beyond this later)
PRIORITY_TOP_STOCKS = int(_cfg_get("PRIORITY_TOP_STOCKS", 1000))

# Per-run scanning limits
MAX_TICKERS_PER_RUN = int(_cfg_get("MAX_TICKERS_PER_RUN", 400))
PRIORITY_PER_RUN = int(_cfg_get("PRIORITY_PER_RUN", 250))
ROTATION_PER_RUN = int(_cfg_get("ROTATION_PER_RUN", 150))

# Cache TTL for universe tables/state
UNIVERSE_CACHE_TTL_SEC = int(_cfg_get("UNIVERSE_CACHE_TTL_SEC", 24 * 3600))

# -------------------------
# SOURCES
# -------------------------
STOCKS_URL = "https://stockanalysis.com/list/biggest-companies/"
ETFS_URL = "https://stockanalysis.com/etf/"

# -------------------------
# CACHE PATHS
# -------------------------
CACHE_DIR = "cache/universe"
CACHE_STOCKS = os.path.join(CACHE_DIR, "stocks_biggest.csv")
CACHE_ETFS = os.path.join(CACHE_DIR, "etfs_all.csv")
CACHE_STATE = os.path.join(CACHE_DIR, "state.json")


def _ensure_dirs() -> None:
    os.makedirs(CACHE_DIR, exist_ok=True)


def _is_fresh(path: str, ttl_sec: int) -> bool:
    if not os.path.exists(path):
        return False
    age = time.time() - os.path.getmtime(path)
    return age <= ttl_sec


def _normalize_symbol(sym: str) -> str:
    """
    Normalize symbols for Yahoo Finance / yfinance.

    Notes:
    - StockAnalysis sometimes shows $TICKER (e.g., $ARM) → strip '$'
    - BRK.B / BF.B / etc must become BRK-B for Yahoo
    - Keep only A-Z 0-9 and '-' to avoid weird symbols
    """
    s = str(sym).strip().upper()
    if not s:
        return ""
    if s.startswith("$"):
        s = s[1:]
    s = s.replace(".", "-")
    s = re.sub(r"[^A-Z0-9\-]", "", s)
    return s


def _parse_market_cap_to_int(value) -> int | None:
    """
    Converts market cap strings like '4.55T', '911.31B', '245.78M' into integer dollars.
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s or s == "-" or s.lower() == "nan":
        return None

    mult = 1
    if s.endswith("T"):
        mult = 1_000_000_000_000
        s = s[:-1]
    elif s.endswith("B"):
        mult = 1_000_000_000
        s = s[:-1]
    elif s.endswith("M"):
        mult = 1_000_000
        s = s[:-1]
    elif s.endswith("K"):
        mult = 1_000
        s = s[:-1]

    try:
        return int(float(s.replace(",", "")) * mult)
    except Exception:
        return None


def _fetch_table(url: str) -> pd.DataFrame:
    # Uses pandas.read_html (requires lxml installed — you already added it)
    tables = pd.read_html(url)
    if not tables:
        return pd.DataFrame()
    return tables[0].copy()


def _load_stocks_biggest(force_refresh: bool = False) -> pd.DataFrame:
    _ensure_dirs()

    if not force_refresh and _is_fresh(CACHE_STOCKS, UNIVERSE_CACHE_TTL_SEC):
        try:
            return pd.read_csv(CACHE_STOCKS)
        except Exception:
            pass

    df = _fetch_table(STOCKS_URL)
    if df.empty:
        raise RuntimeError("Could not load stocks table from StockAnalysis.")

    df.columns = [str(c).strip() for c in df.columns]
    if "Symbol" not in df.columns or "Market Cap" not in df.columns:
        raise RuntimeError(f"Unexpected columns in stocks table: {list(df.columns)}")

    df["market_cap_int"] = df["Market Cap"].apply(_parse_market_cap_to_int)
    df = df.dropna(subset=["Symbol", "market_cap_int"]).copy()
    df["Symbol"] = df["Symbol"].astype(str).str.strip()

    try:
        df.to_csv(CACHE_STOCKS, index=False)
    except Exception:
        pass

    return df


def _load_etfs_all(force_refresh: bool = False) -> pd.DataFrame:
    _ensure_dirs()

    if not force_refresh and _is_fresh(CACHE_ETFS, UNIVERSE_CACHE_TTL_SEC):
        try:
            return pd.read_csv(CACHE_ETFS)
        except Exception:
            pass

    df = _fetch_table(ETFS_URL)
    if df.empty:
        raise RuntimeError("Could not load ETFs table from StockAnalysis.")

    df.columns = [str(c).strip() for c in df.columns]
    if "Symbol" not in df.columns:
        raise RuntimeError(f"Unexpected columns in ETFs table: {list(df.columns)}")

    df["Symbol"] = df["Symbol"].astype(str).str.strip()
    df = df.dropna(subset=["Symbol"]).copy()

    try:
        df.to_csv(CACHE_ETFS, index=False)
    except Exception:
        pass

    return df


def _read_state() -> dict:
    _ensure_dirs()
    if not os.path.exists(CACHE_STATE):
        return {"offset": 0}
    try:
        with open(CACHE_STATE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"offset": 0}


def _write_state(state: dict) -> None:
    _ensure_dirs()
    with open(CACHE_STATE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def _dedupe_keep_order(items: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in items:
        if not x:
            continue
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def load_universe(min_market_cap: int = MIN_MARKET_CAP) -> List[str]:
    """
    Returns tickers to scan:
      - Priority: top PRIORITY_TOP_STOCKS stocks by market cap, filtered by >= min_market_cap
      - Rotation: remaining eligible stocks + all ETFs, rotating each run
    """
    stocks_df = _load_stocks_biggest()
    etfs_df = _load_etfs_all()

    # market cap filter
    stocks_df = stocks_df[stocks_df["market_cap_int"] >= int(min_market_cap)].copy()

    # Priority pool (top by cap)
    priority_raw = stocks_df["Symbol"].head(PRIORITY_TOP_STOCKS).tolist()
    priority = [_normalize_symbol(x) for x in priority_raw]
    priority = [x for x in priority if x]

    # Rotation pool = remaining stocks + ETFs
    remaining_raw = stocks_df["Symbol"].iloc[PRIORITY_TOP_STOCKS:].tolist()
    remaining = [_normalize_symbol(x) for x in remaining_raw]
    remaining = [x for x in remaining if x]

    etfs_raw = etfs_df["Symbol"].tolist()
    etfs = [_normalize_symbol(x) for x in etfs_raw]
    etfs = [x for x in etfs if x]

    expansion_pool = _dedupe_keep_order(remaining + etfs)

    # Priority batch
    take_priority = min(PRIORITY_PER_RUN, len(priority))
    priority_batch = priority[:take_priority]

    # Rotation batch
    state = _read_state()
    offset = int(state.get("offset", 0)) if expansion_pool else 0

    take_rotation = min(ROTATION_PER_RUN, len(expansion_pool))
    if take_rotation > 0 and expansion_pool:
        start = offset % len(expansion_pool)
        end = start + take_rotation
        if end <= len(expansion_pool):
            rotation_batch = expansion_pool[start:end]
        else:
            rotation_batch = expansion_pool[start:] + expansion_pool[: end - len(expansion_pool)]
        offset = (start + take_rotation) % len(expansion_pool)
    else:
        rotation_batch = []

    state["offset"] = offset
    _write_state(state)

    universe = _dedupe_keep_order(priority_batch + rotation_batch)

    # DEV trims
    if DEV_MODE:
        universe = universe[:DEV_TICKERS_LIMIT]

    # hard cap per run
    universe = universe[:MAX_TICKERS_PER_RUN]

    print(f"[Universe] Loaded: priority={len(priority_batch)}, rotation={len(rotation_batch)}, total={len(universe)}")
    print(f"[Universe] Stock cap filter: >= ${int(min_market_cap):,}")
    return universe
