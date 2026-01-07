# loaders/yahoo.py

import os
import time
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError

import pandas as pd
import yfinance as yf

from config import CACHE_TTL

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_DIR = os.path.join(PROJECT_ROOT, "cache", "ohlc")

DOWNLOAD_TIMEOUT_SEC = 20

SAFE_INTRADAY_PERIOD_BY_INTERVAL = {
    "60m": "60d",
    "1h": "60d",
}


def _ensure_cache_dir() -> None:
    os.makedirs(CACHE_DIR, exist_ok=True)


def _sanitize_ticker(ticker: str) -> str:
    t = str(ticker).strip().upper()
    if t.startswith("$"):
        t = t[1:]
    t = t.replace(".", "-")
    return t


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

    ts_col = "Date" if "Date" in df.columns else ("Datetime" if "Datetime" in df.columns else df.columns[0])
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


def _download_blocking(ticker: str, interval: str, period: str) -> pd.DataFrame:
    raw = yf.download(
        tickers=ticker,
        interval=interval,
        period=period,
        auto_adjust=False,
        prepost=False,
        progress=False,
        threads=False,
    )
    return _normalize_download(raw)


def _download_with_timeout(ticker: str, interval: str, period: str) -> pd.DataFrame:
    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(_download_blocking, ticker, interval, period)
        try:
            return fut.result(timeout=DOWNLOAD_TIMEOUT_SEC)
        except FuturesTimeoutError:
            return pd.DataFrame()
        except Exception:
            return pd.DataFrame()


def load_ohlc(
    ticker: str,
    interval: str = "1d",
    period: str = "max",
    max_age_seconds: Optional[int] = None,
) -> pd.DataFrame:
    _ensure_cache_dir()

    ticker = _sanitize_ticker(ticker)
    interval = interval.strip()

    if max_age_seconds is None:
        max_age_seconds = int(CACHE_TTL.get(interval, 2 * 3600))

    path = _cache_path(ticker, interval)

    if _is_cache_fresh(path, max_age_seconds):
        cached = _read_cache(path)
        if not cached.empty:
            return cached

    period_to_use = SAFE_INTRADAY_PERIOD_BY_INTERVAL.get(interval.lower(), period)

    data = _download_with_timeout(ticker=ticker, interval=interval, period=period_to_use)

    if data is None or data.empty:
        if os.path.exists(path):
            cached = _read_cache(path)
            if not cached.empty:
                return cached
        return pd.DataFrame()

    try:
        data.to_csv(path, index=False)
    except Exception:
        pass

    return data
