# loaders/yahoo.py

import os
import time
from typing import Optional

import pandas as pd
import yfinance as yf

from config import CACHE_TTL

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_DIR = os.path.join(PROJECT_ROOT, "cache", "ohlc")


def _ensure_cache_dir() -> None:
    os.makedirs(CACHE_DIR, exist_ok=True)


def _cache_path(ticker: str, interval: str) -> str:
    safe_ticker = ticker.replace("/", "_").replace("^", "")
    safe_interval = interval.replace(" ", "")
    return os.path.join(CACHE_DIR, f"{safe_ticker}_{safe_interval}.csv")


def _is_cache_fresh(path: str, max_age_seconds: int) -> bool:
    if not os.path.exists(path):
        return False
    age = time.time() - os.path.getmtime(path)
    return age <= max_age_seconds


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
    """
    _ensure_cache_dir()

    interval = interval.strip()
    if max_age_seconds is None:
        max_age_seconds = CACHE_TTL.get(interval, 2 * 3600)

    path = _cache_path(ticker, interval)

    # 1) Serve cache if fresh
    if _is_cache_fresh(path, max_age_seconds):
        try:
            df = pd.read_csv(path)
            if not df.empty:
                df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
                df = df.dropna(subset=["timestamp"])
                return df
        except Exception:
            pass  # fall through

    # 2) Fetch from Yahoo
    data = yf.download(
        tickers=ticker,
        interval=interval,
        period=period,
        auto_adjust=False,
        prepost=False,
        progress=False,
        threads=False,
    )

    if data is None or len(data) == 0:
        # 3) If Yahoo fails, return stale cache if available (self-sustaining)
        if os.path.exists(path):
            try:
                df = pd.read_csv(path)
                df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
                df = df.dropna(subset=["timestamp"])
                return df
            except Exception:
                return pd.DataFrame()
        return pd.DataFrame()

    # Handle multiindex columns
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = [c[0] for c in data.columns]

    data = data.rename(
        columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        }
    ).reset_index()

    ts_col = "Date" if "Date" in data.columns else ("Datetime" if "Datetime" in data.columns else None)
    if ts_col is None:
        ts_col = data.columns[0]

    data = data.rename(columns={ts_col: "timestamp"})

    keep = ["timestamp", "open", "high", "low", "close", "volume"]
    data = data[[c for c in keep if c in data.columns]].copy()

    data["timestamp"] = pd.to_datetime(data["timestamp"], errors="coerce")
    data = data.dropna(subset=["timestamp"])

    # 4) Write cache
    try:
        data.to_csv(path, index=False)
    except Exception:
        pass

    return data
