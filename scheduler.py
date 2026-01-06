# scheduler.py

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Dict, Optional, Tuple, List

import pandas as pd
from zoneinfo import ZoneInfo

from loaders.yahoo import load_ohlc
from timeframes.resample import resample_timeframe

ET = ZoneInfo("America/New_York")

# We’ll use a reference ticker to detect candle closes.
# SPY is ideal because it’s liquid and updates reliably.
REFERENCE_TICKER = "SPY"

# Market "close" time per your requirement
CLOSE_HOUR = 16
CLOSE_MINUTE = 30  # 4:30pm ET

META_DIR = os.path.join("cache", "meta")
LAST_RUN_PATH = os.path.join(META_DIR, "last_run.json")


@dataclass
class TFState:
    tf: str
    last_closed_ts: Optional[pd.Timestamp]


def _ensure_dirs():
    os.makedirs(META_DIR, exist_ok=True)


def _load_last_run() -> Dict[str, str]:
    _ensure_dirs()
    if not os.path.exists(LAST_RUN_PATH):
        return {}
    try:
        with open(LAST_RUN_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_last_run(state: Dict[str, str]) -> None:
    _ensure_dirs()
    with open(LAST_RUN_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, sort_keys=True)


def _to_ts(x) -> Optional[pd.Timestamp]:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    try:
        return pd.to_datetime(x)
    except Exception:
        return None


def _close_dt_for_period_end(ts: pd.Timestamp) -> pd.Timestamp:
    """
    ts is period-end label (often midnight).
    We consider period closed at 4:30pm ET on that date (per your requirement).
    """
    d = ts.date()
    return pd.Timestamp(year=d.year, month=d.month, day=d.day, hour=CLOSE_HOUR, minute=CLOSE_MINUTE, tz=ET)


def _last_closed_idx_intraday(df: pd.DataFrame, tf: str) -> int:
    """
    For intraday bars: last row is closed if now >= last_ts + duration.
    If not, use -2.
    """
    df = df.sort_values("timestamp").reset_index(drop=True)
    if len(df) < 3:
        return -2

    now = pd.Timestamp.now(tz=ET)

    # Convert last timestamp to ET for comparison
    last_ts = pd.to_datetime(df.iloc[-1]["timestamp"])
    if last_ts.tzinfo is None:
        last_ts = last_ts.tz_localize(ET)
    else:
        last_ts = last_ts.tz_convert(ET)

    dur_map = {
        "5M": pd.Timedelta(minutes=5),
        "10M": pd.Timedelta(minutes=10),
        "15M": pd.Timedelta(minutes=15),
        "30M": pd.Timedelta(minutes=30),
        "1H": pd.Timedelta(hours=1),
        "2H": pd.Timedelta(hours=2),
        "3H": pd.Timedelta(hours=3),
        "4H": pd.Timedelta(hours=4),
    }

    dur = dur_map[tf]
    if now < (last_ts + dur):
        return -2
    return -1


def _last_closed_idx_higher(df: pd.DataFrame, tf: str) -> int:
    """
    For D/W/M/Q/Y with label-right end dates:
    The last row represents the current period END date label.
    It's closed only if now >= (period_end_date @ 4:30pm ET).
    Otherwise it’s in-progress and we use the previous row (-2).
    """
    df = df.sort_values("timestamp").reset_index(drop=True)
    if len(df) < 3:
        return -2

    now = pd.Timestamp.now(tz=ET)

    last_ts = pd.to_datetime(df.iloc[-1]["timestamp"])
    if last_ts.tzinfo is None:
        last_ts = last_ts.tz_localize(ET)
    else:
        last_ts = last_ts.tz_convert(ET)

    close_dt = _close_dt_for_period_end(last_ts)
    if now < close_dt:
        return -2
    return -1


def _compute_last_closed_ts(frames: Dict[str, pd.DataFrame], tf: str) -> Optional[pd.Timestamp]:
    df = frames.get(tf)
    if df is None or df.empty or "timestamp" not in df.columns or len(df) < 3:
        return None

    tfu = tf.upper()
    if tfu in ("5M", "10M", "15M", "30M", "1H", "2H", "3H", "4H"):
        idx = _last_closed_idx_intraday(df, tfu)
    else:
        idx = _last_closed_idx_higher(df, tfu)

    ts = pd.to_datetime(df.iloc[idx]["timestamp"])
    if ts.tzinfo is None:
        ts = ts.tz_localize(ET)
    else:
        ts = ts.tz_convert(ET)
    return ts


def _build_reference_frames() -> Dict[str, pd.DataFrame]:
    """
    Minimal data load for scheduling decisions:
    - 1d for D/W/M/Q/Y
    - 60m for 1H/2H/3H/4H
    - 5m for 5M/10M/15M/30M
    """
    df_1d = load_ohlc(REFERENCE_TICKER, interval="1d", period="1y")
    df_60 = load_ohlc(REFERENCE_TICKER, interval="60m", period="30d")
    df_5m = load_ohlc(REFERENCE_TICKER, interval="5m", period="30d")

    frames: Dict[str, pd.DataFrame] = {}

    if df_1d is not None and not df_1d.empty:
        frames["D"] = df_1d
        frames["W"] = resample_timeframe(df_1d, "W")
        frames["M"] = resample_timeframe(df_1d, "M")
        frames["Q"] = resample_timeframe(df_1d, "Q")
        frames["Y"] = resample_timeframe(df_1d, "Y")

    if df_60 is not None and not df_60.empty:
        frames["1H"] = df_60
        frames["2H"] = resample_timeframe(df_60, "2H")
        frames["3H"] = resample_timeframe(df_60, "3H")
        frames["4H"] = resample_timeframe(df_60, "4H")

    if df_5m is not None and not df_5m.empty:
        frames["5M"] = df_5m
        frames["10M"] = resample_timeframe(df_5m, "10M")
        frames["15M"] = resample_timeframe(df_5m, "15M")
        frames["30M"] = resample_timeframe(df_5m, "30M")

    return frames


def should_run_for_any_timeframe(target_tfs: List[str]) -> Tuple[bool, Dict[str, str]]:
    """
    Returns:
      (should_run, debug_dict)
    If any timeframe has a newer last-closed bar than what we recorded, we should run.
    """
    last_run = _load_last_run()
    frames = _build_reference_frames()

    debug = {}
    any_new = False

    for tf in target_tfs:
        last_closed_ts = _compute_last_closed_ts(frames, tf)
        if last_closed_ts is None:
            debug[tf] = "no_data"
            continue

        key = tf.upper()
        last_closed_str = last_closed_ts.isoformat()

        prev_recorded = last_run.get(key)
        debug[key] = f"last_closed={last_closed_str} recorded={prev_recorded}"

        if prev_recorded != last_closed_str:
            any_new = True

    return any_new, debug


def record_timeframes_run(target_tfs: List[str]) -> None:
    """
    After a successful scan, record the current last-closed timestamp per TF.
    """
    last_run = _load_last_run()
    frames = _build_reference_frames()

    for tf in target_tfs:
        ts = _compute_last_closed_ts(frames, tf)
        if ts is None:
            continue
        last_run[tf.upper()] = ts.isoformat()

    _save_last_run(last_run)
