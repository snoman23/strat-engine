# universe/loader.py

from __future__ import annotations

import json
import os
import re
import time
from typing import List, Dict

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
    CORE_ETFS,
)

STOCKS_URL = "https://stockanalysis.com/list/biggest-companies/"
ETFS_URL = "https://stockanalysis.com/etf/"

CACHE_DIR = "cache/universe"
CACHE_STOCKS = os.path.join(CACHE_DIR, "stocks_biggest.csv")
CACHE_ETFS = os.path.join(CACHE_DIR, "etfs_all.csv")
CACHE_STATE = os.path.join(CACHE_DIR, "state.json")

# ETF membership cache
CACHE_CORE_HOLDINGS = os.path.join(CACHE_DIR, "core_etf_holdings.csv")

# ✅ NEW: GICS 11 sector map cache (from SPDR sector ETF holdings)
CACHE_SECTOR_MAP = os.path.join(CACHE_DIR, "sector_map.csv")

# SPDR sector ETFs -> your 11 sectors
SECTOR_ETF_MAP = {
    "XLC": "Communication Services",
    "XLY": "Consumer Discretionary",
    "XLP": "Consumer Staples",
    "XLE": "Energy",
    "XLF": "Financials",
    "XLV": "Health Care",
    "XLI": "Industrials",
    "XLK": "Information Technology",
    "XLB": "Materials",
    "XLRE": "Real Estate",
    "XLU": "Utilities",
}


def _ensure_dirs() -> None:
    os.makedirs(CACHE_DIR, exist_ok=True)


def _is_fresh(path: str, ttl_sec: int) -> bool:
    if not os.path.exists(path):
        return False
    return (time.time() - os.path.getmtime(path)) <= ttl_sec


def _normalize_symbol(sym: str) -> str:
    s = str(sym).strip().upper()
    if s.startswith("$"):
        s = s[1:]
    s = s.replace(".", "-")
    s = re.sub(r"[^A-Z0-9\-]", "", s)
    return s


def _parse_market_cap_to_int(value) -> int | None:
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


def _fetch_holdings_symbols(etf: str) -> List[str]:
    """
    Fetch holdings list from StockAnalysis:
      https://stockanalysis.com/etf/{etf}/holdings/
    """
    url = f"https://stockanalysis.com/etf/{etf.lower()}/holdings/"
    try:
        tables = pd.read_html(url)
        if not tables:
            return []
        best = None
        for t in tables:
            cols = [str(c).strip() for c in t.columns]
            if "Symbol" in cols:
                best = t
                break
        if best is None:
            return []
        best.columns = [str(c).strip() for c in best.columns]
        syms = best["Symbol"].astype(str).tolist()
        syms = [_normalize_symbol(s) for s in syms]
        return [s for s in syms if s]
    except Exception:
        return []


def ensure_core_holdings_cache(force_refresh: bool = False) -> None:
    """
    cache/universe/core_etf_holdings.csv
    columns: ticker, etfs (pipe-delimited), etf_count
    """
    _ensure_dirs()
    if not force_refresh and _is_fresh(CACHE_CORE_HOLDINGS, UNIVERSE_CACHE_TTL_SEC):
        return

    membership: Dict[str, List[str]] = {}
    for etf in CORE_ETFS:
        etf_n = _normalize_symbol(etf)
        if not etf_n:
            continue
        holdings = _fetch_holdings_symbols(etf_n)
        for sym in holdings:
            membership.setdefault(sym, []).append(etf_n)

    rows = []
    for sym, etfs in membership.items():
        etfs = sorted(set(etfs))
        rows.append({"ticker": sym, "etfs": "|".join(etfs), "etf_count": len(etfs)})

    pd.DataFrame(rows).to_csv(CACHE_CORE_HOLDINGS, index=False)


def ensure_sector_map_cache(force_refresh: bool = False) -> None:
    """
    cache/universe/sector_map.csv
    columns: ticker, sector (one of your 11)
    Built from SPDR sector ETF holdings (XLC, XLK, etc.).
    """
    _ensure_dirs()
    if not force_refresh and _is_fresh(CACHE_SECTOR_MAP, UNIVERSE_CACHE_TTL_SEC):
        return

    rows = []
    for etf, sector in SECTOR_ETF_MAP.items():
        holdings = _fetch_holdings_symbols(etf)
        for sym in holdings:
            rows.append({"ticker": sym, "sector": sector})

    # If a ticker shows up in multiple (rare), keep first
    out = pd.DataFrame(rows).drop_duplicates(subset=["ticker"], keep="first")
    out.to_csv(CACHE_SECTOR_MAP, index=False)


def load_universe(min_market_cap: int = MIN_MARKET_CAP) -> List[str]:
    """
    Returns tickers to scan:
      - Always include CORE_ETFS every run
      - Priority: top PRIORITY_TOP_STOCKS stocks by market cap, filtered by >= min_market_cap
      - Rotation: remaining eligible stocks + all ETFs, rotating each run

    Also ensures sector_map + core_etf_holdings caches exist for Streamlit.
    """
    ensure_core_holdings_cache(force_refresh=False)
    ensure_sector_map_cache(force_refresh=False)

    stocks_df = _load_stocks_biggest()
    etfs_df = _load_etfs_all()

    stocks_df = stocks_df[stocks_df["market_cap_int"] >= int(min_market_cap)].copy()

    priority_raw = stocks_df["Symbol"].head(PRIORITY_TOP_STOCKS).tolist()
    priority = [_normalize_symbol(x) for x in priority_raw]
    priority = [x for x in priority if x]

    remaining_raw = stocks_df["Symbol"].iloc[PRIORITY_TOP_STOCKS:].tolist()
    remaining = [_normalize_symbol(x) for x in remaining_raw]
    remaining = [x for x in remaining if x]

    etfs_raw = etfs_df["Symbol"].tolist()
    etfs = [_normalize_symbol(x) for x in etfs_raw]
    etfs = [x for x in etfs if x]

    core_etfs = [_normalize_symbol(x) for x in CORE_ETFS]
    core_etfs = [x for x in core_etfs if x]

    expansion_pool = _dedupe_keep_order(remaining + etfs)

    take_priority = min(PRIORITY_PER_RUN, len(priority))
    priority_batch = priority[:take_priority]

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

    universe = _dedupe_keep_order(core_etfs + priority_batch + rotation_batch)

    if DEV_MODE:
        universe = universe[:DEV_TICKERS_LIMIT]

    universe = universe[:MAX_TICKERS_PER_RUN]

    print(
        f"[Universe] Loaded: core_etfs={len(core_etfs)}, priority={len(priority_batch)}, "
        f"rotation={len(rotation_batch)}, total={len(universe)}"
    )
    print(f"[Universe] Stock cap filter: >= ${int(min_market_cap):,}")
    return universe
