# universe/loader.py

from __future__ import annotations

import json
import os
import time
from typing import List

import pandas as pd

from config import (
    DEV_MODE,
    DEV_TICKERS_LIMIT,
    MIN_MARKET_CAP,
    PRIORITY_TOP_STOCKS,
    MAX_TICKERS_PER_RUN,
    PRIORITY_PER_RUN,
    ROTATION_PER_RUN,
    UNIVERSE_CACHE_TTL_SEC,
)

# StockAnalysis sources
STOCKS_URL = "https://stockanalysis.com/list/biggest-companies/"
ETFS_URL = "https://stockanalysis.com/etf/"

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
    Normalize symbols for Yahoo Finance compatibility.
    - StockAnalysis may return BRK.B / BF.B etc
    - Yahoo Finance expects BRK-B / BF-B
    """
    s = str(sym).strip().upper()
    s = s.replace(".", "-")
    return s


def _parse_market_cap_to_int(value) -> int | None:
    """
    Converts market cap strings like '4.55T', '911.31B', '245.78M' into integer dollars.
    Returns None if missing/unparseable.
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
    """
    Uses pandas.read_html to parse the first main table.
    Requires lxml installed (added to requirements.txt).
    """
    tables = pd.read_html(url)
    if not tables:
        return pd.DataFrame()
    return tables[0].copy()


def _load_stocks_biggest(force_refresh: bool = False) -> pd.DataFrame:
    _ensure_dirs()

    if not force_refresh and _is_fresh(CACHE_STOCKS, UNIVERSE_CACHE_TTL_SEC):
        return pd.read_csv(CACHE_STOCKS)

    df = _fetch_table(STOCKS_URL)
    if df.empty:
        raise RuntimeError("Could not load stocks table from StockAnalysis.")

    df.columns = [str(c).strip() for c in df.columns]

    if "Symbol" not in df.columns or "Market Cap" not in df.columns:
        raise RuntimeError(f"Unexpected columns in stocks table: {list(df.columns)}")

    df["market_cap_int"] = df["Market Cap"].apply(_parse_market_cap_to_int)
    df = df.dropna(subset=["Symbol", "market_cap_int"]).copy()

    df["Symbol"] = df["Symbol"].astype(str).str.strip()
    df.to_csv(CACHE_STOCKS, index=False)
    return df


def _load_etfs_all(force_refresh: bool = False) -> pd.DataFrame:
    _ensure_dirs()

    if not force_refresh and _is_fresh(CACHE_ETFS, UNIVERSE_CACHE_TTL_SEC):
        return pd.read_csv(CACHE_ETFS)

    df = _fetch_table(ETFS_URL)
    if df.empty:
        raise RuntimeError("Could not load ETFs table from StockAnalysis.")

    df.columns = [str(c).strip() for c in df.columns]

    if "Symbol" not in df.columns:
        raise RuntimeError(f"Unexpected columns in ETFs table: {list(df.columns)}")

    df["Symbol"] = df["Symbol"].astype(str).str.strip()
    df = df.dropna(subset=["Symbol"]).copy()

    df.to_csv(CACHE_ETFS, index=False)
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
    AUTO Universe:
      - Top PRIORITY_TOP_STOCKS US stocks by market cap (filtered by >= min_market_cap)
      - + ALL ETFs
      - Returns a per-run batch:
          * PRIORITY_PER_RUN from the top bucket
          * ROTATION_PER_RUN rotating from (remaining stocks + all ETFs)
      - Rotation offset persisted in cache/universe/state.json

    This prevents alphabet bias and keeps runs manageable.
    """

    stocks_df = _load_stocks_biggest()
    etfs_df = _load_etfs_all()

    # Filter stocks by market cap threshold
    stocks_df = stocks_df[stocks_df["market_cap_int"] >= int(min_market_cap)].copy()

    # Priority bucket: top N by cap
    priority_raw = stocks_df["Symbol"].head(PRIORITY_TOP_STOCKS).tolist()
    priority = [_normalize_symbol(x) for x in priority_raw]

    # Expansion pool: remaining eligible stocks + ALL ETFs
    remaining_raw = stocks_df["Symbol"].iloc[PRIORITY_TOP_STOCKS:].tolist()
    remaining_stocks = [_normalize_symbol(x) for x in remaining_raw]

    etfs_raw = etfs_df["Symbol"].tolist()
    all_etfs = [_normalize_symbol(x) for x in etfs_raw]

    expansion_pool = _dedupe_keep_order(remaining_stocks + all_etfs)

    # Priority batch (always include some big names)
    take_priority = min(PRIORITY_PER_RUN, len(priority))
    priority_batch = priority[:take_priority]

    # Rotation batch
    state = _read_state()
    offset = int(state.get("offset", 0)) if expansion_pool else 0

    take_rotation = min(ROTATION_PER_RUN, len(expansion_pool))
    if take_rotation > 0:
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

    # DEV mode limit
    if DEV_MODE:
        universe = universe[:DEV_TICKERS_LIMIT]

    # Hard safety cap
    universe = universe[:MAX_TICKERS_PER_RUN]

    print(f"[Universe] Loaded: priority={len(priority_batch)}, rotation={len(rotation_batch)}, total={len(universe)}")
    print(f"[Universe] Stock cap filter: >= ${int(min_market_cap):,}")
    return universe

