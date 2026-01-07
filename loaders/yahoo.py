# loaders/yahoo.py

import os
import re
import time
import threading
from typing import Optional, List, Tuple

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


def _normalize_ticker_for_yahoo(ticker: str) -> str:
    """
    Normalize symbols for Yahoo Finance:
      - remove leading '$' (example: $ARM -> ARM)
      - BRK.B -> BRK-B
      - keep only A-Z 0-9 and '-' (removes weird characters)
    """
    s = str(ticker).strip().upper()
    if s.startswith("$"):
        s = s[1:]
    s = s.replace(".", "-")
    s = re.sub(r"[^A-Z0-9\-]", "", s)
    return s


def _cache_path(ticker: str, interval: str) -> str:
    safe_ticker = _normalize_ticker_for_yahoo(ticker).replace("/", "_").replace("^", "")
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
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=False)
        df = df.dropna(subset=["timestamp"])
        return df
    except Exception:
        return pd.DataFrame()


# -----------------------------
# Helpers: normalize dataframe
# -----------------------------
def _normalize_download(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize yfinance output into:
      timestamp, open, high, low, close, volume
    """
    if df is None or len(df) == 0:
        return pd.DataFrame()

    # yfinance sometimes returns MultiIndex columns
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

    # Timestamp column can be Date or Datetime
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

    # Sort and dedupe
    df = df.sort_values("timestamp").drop_duplicates(subset=["timestamp"]).reset_index(drop=True)
    return df


def _infer_resolution_seconds(df: pd.DataFrame) -> int:
    """
    Estimate bar size in seconds from median timestamp diff.
    Returns 0 if not enough data.
    """
    if df is None or df.empty or "timestamp" not in df.columns or len(df) < 3:
        return 0
    ts = pd.to_datetime(df["timestamp"], errors="coerce")
    ts = ts.dropna().sort_values()
    if len(ts) < 3:
        return 0
    diffs = ts.diff().dropna().dt.total_seconds()
    if diffs.empty:
        return 0
    return int(diffs.median())


def _expected_max_resolution_seconds(interval: str) -> int:
    """
    For intraday intervals, enforce that returned data is not coarser than expected.
    """
    interval = interval.strip().lower()
    if interval in ("60m", "1h"):
        return 3600 * 2  # allow some irregularity
    if interval == "30m":
        return 1800 * 2
    if interval == "15m":
        return 900 * 2
    if interval == "5m":
        return 300 * 2
    if interval == "1d":
        return 86400 * 2
    return 0


# -----------------------------
# Helpers: download with timeout
# -----------------------------
def _download_yf_with_timeout(ticker: str, interval: str, period: str, timeout_sec: int) -> pd.DataFrame:
    """
    Run yf.download in a thread and hard-timeout.
    If it times out or errors, returns empty DF.
    """
    out: dict = {"df": None, "err": None}

    def _worker():
        try:
            raw = yf.download(
                tickers=ticker,
                interval=interval,
                period=period,
                auto_adjust=False,
                prepost=False,
                progress=False,
                threads=False,
            )
            out["df"] = raw
        except Exception as e:
            out["err"] = e
            out["df"] = None

    t = threading.Thread(target=_worker, daemon=True)
    t.start()
    t.join(timeout=timeout_sec)

    if t.is_alive():
        # Timed out — return empty to avoid stalling whole scanner.
        return pd.DataFrame()

    df = out.get("df")
    if df is None:
        return pd.DataFrame()

    return _normalize_download(df)


_INTRADAY_INTERVALS = {"1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h"}


def _download_with_fallback(ticker: str, interval: str, period: str) -> pd.DataFrame:
    """
    Keep fallback small and fast.
    DO NOT try 730d for intraday (causes errors and slowness).
    """
    interval_norm = interval.strip().lower()

    # Non-intraday: one shot
    if interval_norm not in _INTRADAY_INTERVALS:
        return _download_yf_with_timeout(ticker, interval_norm, period, REQUEST_TIMEOUT_SEC)

    # Intraday fallback chain (fast + safe)
    # Start with caller period if it looks reasonable, but never exceed 60d here.
    fallback_periods: List[str] = []

    # If caller asked something huge (like 730d), override
    if period and period.strip().lower() in ("max", "730d", "365d", "180d"):
        fallback_periods.append("60d")
    elif period:
        fallback_periods.append(period)

    for p in ["60d", "30d", "7d"]:
        if p not in fallback_periods:
            fallback_periods.append(p)

    for p in fallback_periods:
        df = _download_yf_with_timeout(ticker, interval_norm, p, REQUEST_TIMEOUT_SEC)
        if df is None or df.empty:
            continue

        # Validate that Yahoo actually returned the interval we asked for
        max_expected = _expected_max_resolution_seconds(interval_norm)
        if max_expected > 0:
            res = _infer_resolution_seconds(df)
            if res == 0:
                continue
            if res > max_expected:
                # Yahoo gave coarser data (e.g., 4H or 1D), treat as unusable
                continue

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
      - Otherwise downloads from Yahoo (hard timeout)
      - Intraday uses short safe fallback periods (60d/30d/7d)
      - If Yahoo fails, returns stale cache if available
    """
    _ensure_cache_dir()

    interval = interval.strip()
    yf_ticker = _normalize_ticker_for_yahoo(ticker)

    if max_age_seconds is None:
        max_age_seconds = int(CACHE_TTL.get(interval, 2 * 3600))

    path = _cache_path(yf_ticker, interval)

    # 1) Serve cache if fresh
    if _is_cache_fresh(path, max_age_seconds):
        cached = _read_cache(path)
        if not cached.empty:
            return cached

    # 2) Fetch from Yahoo with timeout + safe fallback
    data = _download_with_fallback(ticker=yf_ticker, interval=interval, period=period)

    if data is None or data.empty:
        # 3) If Yahoo fails, return stale cache if available (self-sustaining)
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
