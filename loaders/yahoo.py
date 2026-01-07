# loaders/yahoo.py

import os
import time
from typing import Optional, List

import pandas as pd
import yfinance as yf

from config import CACHE_TTL, REQUEST_TIMEOUT_SEC

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_DIR = os.path.join(PROJECT_ROOT, "cache", "ohlc")


# -----------------------------
# Helpers: cache
# -----------------------------
def _ensure_cache_dir() -> None:
    os.makedirs(CACHE_DIR, exist_ok=True)


def _cache_path(ticker: str, interval: str) -> str:
    safe_ticker = (
        str(ticker)
        .replace("/", "_")
        .replace("^", "")
        .replace("=", "_")
        .replace(" ", "")
    )
    safe_interval = str(interval).replace(" ", "")
    return os.path.join(CACHE_DIR, f"{safe_ticker}_{safe_interval}.csv")


def _is_cache_fresh(path: str, max_age_seconds: int) -> bool:
    if not os.path.exists(path):
        return False
    age = time.time() - os.path.getmtime(path)
    return age <= max_age_seconds


def _read_cache(path: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(path)
        if df is None or df.empty:
            return pd.DataFrame()
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = df.dropna(subset=["timestamp"])
        return df
    except Exception:
        return pd.DataFrame()


# -----------------------------
# Helpers: download + normalize
# -----------------------------
_INTRADAY_INTERVALS = {"1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h"}


def _normalize_download(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize yfinance download() output into:
      timestamp, open, high, low, close, volume
    """
    if df is None or len(df) == 0:
        return pd.DataFrame()

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]

    df = df.rename(
        columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        }
    ).reset_index()

    ts_col = None
    if "Date" in df.columns:
        ts_col = "Date"
    elif "Datetime" in df.columns:
        ts_col = "Datetime"
    else:
        ts_col = df.columns[0]

    df = df.rename(columns={ts_col: "timestamp"})

    keep = ["timestamp", "open", "high", "low", "close", "volume"]
    df = df[[c for c in keep if c in df.columns]].copy()

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])

    for c in ["open", "high", "low", "close", "volume"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["open", "high", "low", "close"])
    return df


def _download_once(ticker: str, interval: str, period: str) -> pd.DataFrame:
    """
    Single yfinance download with a hard request timeout to prevent hangs in Actions.
    """
    raw = yf.download(
        tickers=ticker,
        interval=interval,
        period=period,
        auto_adjust=False,
        prepost=False,
        progress=False,
        threads=False,
        timeout=REQUEST_TIMEOUT_SEC,
    )
    return _normalize_download(raw)


def _download_with_fallback(ticker: str, interval: str, period: str) -> pd.DataFrame:
    """
    For intraday, Yahoo can reject long windows (ARM/new IPOs).
    Try a fallback chain from bigger -> smaller.
    """
    interval_norm = interval.strip()

    if interval_norm not in _INTRADAY_INTERVALS:
        return _download_once(ticker, interval_norm, period)

    fallback_periods: List[str] = []
    if period:
        fallback_periods.append(period)

    for p in ["730d", "365d", "180d", "60d", "30d", "7d"]:
        if p not in fallback_periods:
            fallback_periods.append(p)

    for p in fallback_periods:
        df = _download_once(ticker, interval_norm, p)
        if df is not None and not df.empty:
            return df

    return pd.DataFrame()


# -----------------------------
# Public API
# -----------------------------
def load_ohlc(
    ticker: str,
    interval: str = "1d",
    period: str = "max",
    max_age_seconds: Optional[int] = None,
) -> pd.DataFrame:
    """
    Loads OHLCV from Yahoo via yfinance, with disk caching.

    Returns columns:
      timestamp, open, high, low, close, volume

    Behavior:
      - Serves fresh cache if available
      - Otherwise downloads from Yahoo
      - Intraday intervals fall back to shorter periods if Yahoo rejects the range
      - If Yahoo fails, returns stale cache if available
    """
    _ensure_cache_dir()

    interval = interval.strip()
    if max_age_seconds is None:
        max_age_seconds = int(CACHE_TTL.get(interval, 2 * 3600))

    path = _cache_path(ticker, interval)

    # 1) Serve cache if fresh
    if _is_cache_fresh(path, max_age_seconds):
        cached = _read_cache(path)
        if not cached.empty:
            return cached

    # 2) Fetch from Yahoo with fallback (intraday-safe)
    data = _download_with_fallback(ticker=ticker, interval=interval, period=period)

    if data is None or data.empty:
        # 3) If Yahoo fails, return stale cache if available
        if os.path.exists(path):
            cached = _read_cache(path)
            if not cached.empty:
                return cached
        return pd.DataFrame()

    # 4) Write cache
    try:
        data.to_csv(path, index=False)
    except Exception:
        pass

    return data
