# loaders/yahoo.py

import os
import time
from typing import Optional, List

import pandas as pd
import yfinance as yf

from config import CACHE_TTL, REQUEST_TIMEOUT_SEC

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_DIR = os.path.join(PROJECT_ROOT, "cache", "ohlc")


def _ensure_cache_dir() -> None:
    os.makedirs(CACHE_DIR, exist_ok=True)


def _cache_path(ticker: str, interval: str) -> str:
    safe_ticker = (
        str(ticker)
        .replace("/", "_")
        .replace("^", "")
        .replace("=", "_")
        .replace(" ", "")
        .replace(".", "-")
    )
    safe_interval = str(interval).replace(" ", "")
    return os.path.join(CACHE_DIR, f"{safe_ticker}_{safe_interval}.csv")


def _is_cache_fresh(path: str, max_age_seconds: int) -> bool:
    if not os.path.exists(path):
        return False
    return (time.time() - os.path.getmtime(path)) <= max_age_seconds


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


_INTRADAY_INTERVALS = {"1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h"}


def _normalize_download(df: pd.DataFrame) -> pd.DataFrame:
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
    df = df.sort_values("timestamp").drop_duplicates(subset=["timestamp"]).reset_index(drop=True)
    return df


def _download_once(ticker: str, interval: str, period: str) -> pd.DataFrame:
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
    interval_norm = interval.strip()

    # Non-intraday: one attempt
    if interval_norm not in _INTRADAY_INTERVALS:
        return _download_once(ticker, interval_norm, period)

    # Intraday: NEVER try 730d; it breaks for many tickers
    fallback_periods: List[str] = []

    # Respect caller if already safe; otherwise override to 60d
    safe_period = (period or "").strip().lower()
    if safe_period in ("7d", "30d", "60d", "90d"):
        fallback_periods.append(period)
    else:
        fallback_periods.append("60d")

    for p in ["60d", "30d", "7d"]:
        if p not in fallback_periods:
            fallback_periods.append(p)

    for p in fallback_periods:
        df = _download_once(ticker, interval_norm, p)
        if df is not None and not df.empty:
            return df

    return pd.DataFrame()


def load_ohlc(
    ticker: str,
    interval: str = "1d",
    period: str = "max",
    max_age_seconds: Optional[int] = None,
) -> pd.DataFrame:
    _ensure_cache_dir()

    interval = interval.strip()
    if max_age_seconds is None:
        max_age_seconds = int(CACHE_TTL.get(interval, 2 * 3600))

    path = _cache_path(ticker, interval)

    # 1) fresh cache
    if _is_cache_fresh(path, max_age_seconds):
        cached = _read_cache(path)
        if not cached.empty:
            return cached

    # 2) Yahoo fetch
    data = _download_with_fallback(ticker=ticker, interval=interval, period=period)

    if data is None or data.empty:
        # 3) fallback to stale cache
        if os.path.exists(path):
            cached = _read_cache(path)
            if not cached.empty:
                return cached
        return pd.DataFrame()

    # 4) write cache
    try:
        data.to_csv(path, index=False)
    except Exception:
        pass

    return data
