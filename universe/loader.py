# universe/loader.py

from __future__ import annotations

import os
import time
import csv
import re
from typing import List, Dict, Optional

import pandas as pd
import requests
import yfinance as yf

from config import (
    DEV_MODE,
    DEV_TICKERS_LIMIT,
    MIN_MARKET_CAP,
    UNIVERSE_MODE,
    UNIVERSE_CACHE_DIR,
    UNIVERSE_SYMBOLS_PATH,
    UNIVERSE_MARKETCAP_PATH,
)

# -------------------------
# DEV TICKERS (fast)
# -------------------------
DEV_TICKERS = [
    "SPY", "QQQ", "IWM",
    "AAPL", "MSFT", "NVDA",
    "TSLA", "AMZN", "META",
    "ARKK"
]

# -------------------------
# Full universe sources
# -------------------------
# Free symbol lists:
# - NASDAQ listed
# - NYSE listed
# - AMEX listed
# These are widely mirrored on the web. We cache locally and only refresh periodically.
SYMBOL_SOURCES = [
    # These endpoints are commonly used for listing files. If one fails, the others may still work.
    "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt",
    "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt",
]

# Refresh symbol list every 7 days (in seconds)
SYMBOLS_TTL = 7 * 24 * 3600

# Refresh marketcap cache every 7 days (in seconds)
MARKETCAP_TTL = 7 * 24 * 3600

# Yahoo rate safety (seconds between marketcap calls)
MCAP_SLEEP = 0.12  # keep it gentle to avoid bans


# -------------------------
# Public API
# -------------------------
def load_universe(min_market_cap: int = MIN_MARKET_CAP) -> List[str]:
    if UNIVERSE_MODE.upper() == "DEV":
        print("[Universe] DEV mode active")
        return DEV_TICKERS

    if UNIVERSE_MODE.upper() != "FULL":
        raise ValueError(f"Unknown UNIVERSE_MODE: {UNIVERSE_MODE}")

    print("[Universe] FULL mode active (cached)")
    symbols = load_or_refresh_symbols()
    tickers = filter_by_market_cap(symbols, min_market_cap=min_market_cap)

    if DEV_MODE:
        print("[Universe] DEV_MODE=True, limiting tickers for speed")
        return tickers[:DEV_TICKERS_LIMIT]

    return tickers


# -------------------------
# Helpers
# -------------------------
def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _is_fresh(path: str, ttl: int) -> bool:
    if not os.path.exists(path):
        return False
    age = time.time() - os.path.getmtime(path)
    return age < ttl


def _clean_symbol(sym: str) -> Optional[str]:
    if sym is None:
        return None
    s = sym.strip().upper()

    # Remove weird symbols / test issues
    # Keep ETF tickers too (SPY, QQQ, etc.)
    if not s:
        return None

    # NasdaqTrader lists can contain symbols like "BRK.A" (dot), or "BF.B"
    # yfinance typically supports "BRK-B" not "BRK.B"
    # We’ll map dots to hyphens for Yahoo compatibility.
    s = s.replace(".", "-")

    # Filter out symbols with spaces or non-ticker junk
    if re.search(r"\s", s):
        return None

    # Exclude obvious non-equity test symbols
    if s in ("SYMBOL", "FILE", "TEST"):
        return None

    return s


# -------------------------
# Symbol list (cached)
# -------------------------
def load_or_refresh_symbols() -> List[str]:
    _ensure_dir(UNIVERSE_CACHE_DIR)

    if _is_fresh(UNIVERSE_SYMBOLS_PATH, SYMBOLS_TTL):
        return _read_symbols_csv(UNIVERSE_SYMBOLS_PATH)

    symbols: List[str] = []

    # Download and parse nasdaqlisted + otherlisted
    for url in SYMBOL_SOURCES:
        try:
            txt = requests.get(url, timeout=20).text
            symbols.extend(_parse_nasdaqtrader_listing(txt))
        except Exception as e:
            print(f"[Universe] Warning: failed to load {url}: {e}")

    symbols = sorted(set([s for s in (_clean_symbol(x) for x in symbols) if s]))

    # Write cache
    with open(UNIVERSE_SYMBOLS_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["symbol"])
        for s in symbols:
            w.writerow([s])

    print(f"[Universe] Cached {len(symbols)} symbols -> {UNIVERSE_SYMBOLS_PATH}")
    return symbols


def _parse_nasdaqtrader_listing(text: str) -> List[str]:
    """
    nasdaqlisted.txt format:
      Symbol|Security Name|Market Category|...|Test Issue|...|Financial Status|...
      File Creation Time: ...
    otherlisted.txt format:
      ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|...
      File Creation Time: ...
    We'll parse the first column for symbols and ignore metadata lines.
    """
    out: List[str] = []
    lines = text.splitlines()
    for line in lines:
        if not line or line.startswith("File Creation Time:"):
            continue
        if "|" not in line:
            continue
        parts = line.split("|")
        sym = parts[0].strip()
        if sym and sym.upper() not in ("SYMBOL", "ACT SYMBOL"):
            out.append(sym)
    return out


def _read_symbols_csv(path: str) -> List[str]:
    df = pd.read_csv(path)
    if "symbol" not in df.columns:
        return []
    return [str(x).strip().upper() for x in df["symbol"].dropna().tolist()]


# -------------------------
# Market cap filter (cached)
# -------------------------
def filter_by_market_cap(symbols: List[str], min_market_cap: int) -> List[str]:
    _ensure_dir(UNIVERSE_CACHE_DIR)

    # Load existing marketcap cache if fresh-ish
    cache: Dict[str, float] = {}
    if os.path.exists(UNIVERSE_MARKETCAP_PATH):
        cache = _read_marketcap_cache(UNIVERSE_MARKETCAP_PATH)

    # If cache is old, we still use it but we’ll refresh missing entries as needed.
    cache_age_ok = _is_fresh(UNIVERSE_MARKETCAP_PATH, MARKETCAP_TTL)

    # Determine which symbols need marketcap lookup
    need_lookup = [s for s in symbols if s not in cache]
    if not cache_age_ok:
        # Optional: if cache is stale, we can refresh a slice each run instead of all at once
        # to avoid timeouts. We'll still prioritize missing entries first.
        pass

    # Look up market caps for missing symbols (throttled)
    looked_up = 0
    for sym in need_lookup:
        mc = _fetch_market_cap(sym)
        if mc is not None:
            cache[sym] = mc
        else:
            cache[sym] = -1  # mark as unknown/unavailable
        looked_up += 1
        time.sleep(MCAP_SLEEP)

        # Safety guard: don't look up infinite symbols in one GitHub run.
        # You can raise this later once stable.
        if looked_up >= 1200 and not DEV_MODE:
            print("[Universe] Hit lookup guard (1200). Using cached market caps for the rest this run.")
            break

    _write_marketcap_cache(UNIVERSE_MARKETCAP_PATH, cache)

    # Filter final list
    tickers = [s for s in symbols if cache.get(s, -1) >= min_market_cap]

    print(f"[Universe] Universe after market cap filter >= {min_market_cap:,}: {len(tickers)} tickers")
    return tickers


def _fetch_market_cap(symbol: str) -> Optional[float]:
    try:
        t = yf.Ticker(symbol)
        info = getattr(t, "fast_info", None)
        if info and isinstance(info, dict):
            mc = info.get("market_cap")
            if mc is not None:
                return float(mc)

        # fallback to .info (slower)
        inf = t.info
        mc2 = inf.get("marketCap")
        if mc2 is not None:
            return float(mc2)

        return None
    except Exception:
        return None


def _read_marketcap_cache(path: str) -> Dict[str, float]:
    out: Dict[str, float] = {}
    try:
        df = pd.read_csv(path)
        if "symbol" not in df.columns or "market_cap" not in df.columns:
            return out
        for _, r in df.iterrows():
            out[str(r["symbol"]).strip().upper()] = float(r["market_cap"])
    except Exception:
        return out
    return out


def _write_marketcap_cache(path: str, cache: Dict[str, float]) -> None:
    df = pd.DataFrame(
        [{"symbol": k, "market_cap": v} for k, v in cache.items()]
    ).sort_values("symbol")
    df.to_csv(path, index=False)
