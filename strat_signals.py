# strat_signals.py

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional
import pandas as pd
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")


@dataclass
class StratSignal:
    tf: str
    kind: str  # "NEXT" or "TRIGGERED" (main.py filters TRIGGERED out)
    pattern: str                 # last 2 closed candles, e.g. "1-2U"
    setup: str                   # what we are planning to trade next
    direction: Optional[str]     # "bull" | "bear" | None

    actionable: str
    entry: Optional[float]
    stop: Optional[float]
    note: str

    prev_closed_ts: pd.Timestamp
    prev_strat: str
    prev_open: float
    prev_high: float
    prev_low: float
    prev_close: float

    last_closed_ts: pd.Timestamp
    last_strat: str
    last_open: float
    last_high: float
    last_low: float
    last_close: float


def _to_ny(ts) -> pd.Timestamp:
    t = pd.to_datetime(ts, errors="coerce")
    if pd.isna(t):
        return pd.Timestamp.now(tz=NY)
    if t.tzinfo is None:
        return t.tz_localize(NY)
    return t.tz_convert(NY)


def _market_close_dt(date_ts: pd.Timestamp) -> pd.Timestamp:
    d = _to_ny(date_ts).date()
    return pd.Timestamp(year=d.year, month=d.month, day=d.day, hour=16, minute=30, tz=NY)


def last_closed_index(tf: str, df_tf: pd.DataFrame) -> int:
    """
    Closed-bar logic:
    - 1H: Yahoo 60m timestamps behave like bar START -> closed if now >= ts + 1 hour
    - 2H/3H/4H: resampled with label='right' -> timestamp is bar END -> closed if now >= ts
    - W/M/Q/Y: label often in the future for current open period -> use prior bar
    """
    if df_tf is None or df_tf.empty or "timestamp" not in df_tf.columns:
        return -1

    tf = tf.strip().upper()
    now = pd.Timestamp.now(tz=NY)
    ts_last = _to_ny(df_tf.iloc[-1]["timestamp"])

    if tf in ("W", "M", "Q", "Y"):
        if ts_last > now:
            return -2
        if now < _market_close_dt(ts_last):
            return -2
        return -1

    if tf == "D":
        if ts_last.date() == now.date() and now < _market_close_dt(ts_last):
            return -2
        return -1

    if tf == "1H":
        bar_end = ts_last + pd.Timedelta(hours=1)
        if now < bar_end:
            return -2
        return -1

    if tf in ("2H", "3H", "4H"):
        if now < ts_last:
            return -2
        return -1

    return -1


def _fmt2(x: float) -> float:
    try:
        return float(round(float(x), 2))
    except Exception:
        return x


def analyze_last_closed_setups(df_tf: pd.DataFrame, tf: str) -> List[StratSignal]:
    if df_tf is None or df_tf.empty or len(df_tf) < 3:
        return []

    df_tf = df_tf.sort_values("timestamp").reset_index(drop=True)

    last_idx = last_closed_index(tf, df_tf)
    prev_idx = last_idx - 1
    if len(df_tf) < abs(prev_idx):
        return []

    prev = df_tf.iloc[prev_idx]
    last = df_tf.iloc[last_idx]

    prev_s = str(prev.get("strat"))
    last_s = str(last.get("strat"))

    prev_ts = pd.to_datetime(prev["timestamp"])
    last_ts = pd.to_datetime(last["timestamp"])

    prev_o = float(prev["open"]);  prev_h = float(prev["high"]);  prev_l = float(prev["low"]);  prev_c = float(prev["close"])
    last_o = float(last["open"]);  last_h = float(last["high"]);  last_l = float(last["low"]);  last_c = float(last["close"])

    signals: List[StratSignal] = []

    # -----------------------------
    # INSIDE BAR (last = 1) => play 1-2 (break either way)
    # -----------------------------
    if last_s == "1":
        # Break UP
        signals.append(
            StratSignal(
                tf=tf,
                kind="NEXT",
                pattern=f"{prev_s}-1",
                setup="1-2 BREAK_UP",
                direction="bull",
                actionable=f"ALERT if price > {_fmt2(last_h)} (inside break UP); stop < {_fmt2(last_l)}",
                entry=_fmt2(last_h),
                stop=_fmt2(last_l),
                note="Inside bar break UP (1-2)",
                prev_closed_ts=prev_ts, prev_strat=prev_s,
                prev_open=_fmt2(prev_o), prev_high=_fmt2(prev_h), prev_low=_fmt2(prev_l), prev_close=_fmt2(prev_c),
                last_closed_ts=last_ts, last_strat=last_s,
                last_open=_fmt2(last_o), last_high=_fmt2(last_h), last_low=_fmt2(last_l), last_close=_fmt2(last_c),
            )
        )
        # Break DOWN
        signals.append(
            StratSignal(
                tf=tf,
                kind="NEXT",
                pattern=f"{prev_s}-1",
                setup="1-2 BREAK_DOWN",
                direction="bear",
                actionable=f"ALERT if price < {_fmt2(last_l)} (inside break DOWN); stop > {_fmt2(last_h)}",
                entry=_fmt2(last_l),
                stop=_fmt2(last_h),
                note="Inside bar break DOWN (1-2)",
                prev_closed_ts=prev_ts, prev_strat=prev_s,
                prev_open=_fmt2(prev_o), prev_high=_fmt2(prev_h), prev_low=_fmt2(prev_l), prev_close=_fmt2(prev_c),
                last_closed_ts=last_ts, last_strat=last_s,
                last_open=_fmt2(last_o), last_high=_fmt2(last_h), last_low=_fmt2(last_l), last_close=_fmt2(last_c),
            )
        )

    # -----------------------------
    # OUTSIDE BAR (last = 3) => play 3-2 (break either way)
    # -----------------------------
    if last_s == "3":
        signals.append(
            StratSignal(
                tf=tf,
                kind="NEXT",
                pattern=f"{prev_s}-3",
                setup="3-2 BREAK_UP",
                direction="bull",
                actionable=f"ALERT if price > {_fmt2(last_h)} (outside break UP); stop < {_fmt2(last_l)}",
                entry=_fmt2(last_h),
                stop=_fmt2(last_l),
                note="Outside bar break UP (3-2)",
                prev_closed_ts=prev_ts, prev_strat=prev_s,
                prev_open=_fmt2(prev_o), prev_high=_fmt2(prev_h), prev_low=_fmt2(prev_l), prev_close=_fmt2(prev_c),
                last_closed_ts=last_ts, last_strat=last_s,
                last_open=_fmt2(last_o), last_high=_fmt2(last_h), last_low=_fmt2(last_l), last_close=_fmt2(last_c),
            )
        )
        signals.append(
            StratSignal(
                tf=tf,
                kind="NEXT",
                pattern=f"{prev_s}-3",
                setup="3-2 BREAK_DOWN",
                direction="bear",
                actionable=f"ALERT if price < {_fmt2(last_l)} (outside break DOWN); stop > {_fmt2(last_h)}",
                entry=_fmt2(last_l),
                stop=_fmt2(last_h),
                note="Outside bar break DOWN (3-2)",
                prev_closed_ts=prev_ts, prev_strat=prev_s,
                prev_open=_fmt2(prev_o), prev_high=_fmt2(prev_h), prev_low=_fmt2(prev_l), prev_close=_fmt2(prev_c),
                last_closed_ts=last_ts, last_strat=last_s,
                last_open=_fmt2(last_o), last_high=_fmt2(last_h), last_low=_fmt2(last_l), last_close=_fmt2(last_c),
            )
        )

    # -----------------------------
    # REVSTRAT WATCH: after (1 or 3) then (2U/2D) => watch for 2 reversal next
    # (1-2-2 or 3-2-2 concept)
    # -----------------------------
    if prev_s in ("1", "3") and last_s in ("2U", "2D"):
        if last_s == "2U":
            # after 1-2U or 3-2U: watch for 2D reversal (bear)
            signals.append(
                StratSignal(
                    tf=tf,
                    kind="NEXT",
                    pattern=f"{prev_s}-2U",
                    setup=f"REVSTRAT {prev_s}-2-2 (watch 2D)",
                    direction="bear",
                    actionable=f"ALERT if price < {_fmt2(last_l)} (RevStrat bear); stop > {_fmt2(last_h)}",
                    entry=_fmt2(last_l),
                    stop=_fmt2(last_h),
                    note=f"RevStrat after {prev_s}-2U: watch for 2D reversal ({prev_s}-2U-2D)",
                    prev_closed_ts=prev_ts, prev_strat=prev_s,
                    prev_open=_fmt2(prev_o), prev_high=_fmt2(prev_h), prev_low=_fmt2(prev_l), prev_close=_fmt2(prev_c),
                    last_closed_ts=last_ts, last_strat=last_s,
                    last_open=_fmt2(last_o), last_high=_fmt2(last_h), last_low=_fmt2(last_l), last_close=_fmt2(last_c),
                )
            )
        if last_s == "2D":
            # after 1-2D or 3-2D: watch for 2U reversal (bull)
            signals.append(
                StratSignal(
                    tf=tf,
                    kind="NEXT",
                    pattern=f"{prev_s}-2D",
                    setup=f"REVSTRAT {prev_s}-2-2 (watch 2U)",
                    direction="bull",
                    actionable=f"ALERT if price > {_fmt2(last_h)} (RevStrat bull); stop < {_fmt2(last_l)}",
                    entry=_fmt2(last_h),
                    stop=_fmt2(last_l),
                    note=f"RevStrat after {prev_s}-2D: watch for 2U reversal ({prev_s}-2D-2U)",
                    prev_closed_ts=prev_ts, prev_strat=prev_s,
                    prev_open=_fmt2(prev_o), prev_high=_fmt2(prev_h), prev_low=_fmt2(prev_l), prev_close=_fmt2(prev_c),
                    last_closed_ts=last_ts, last_strat=last_s,
                    last_open=_fmt2(last_o), last_high=_fmt2(last_h), last_low=_fmt2(last_l), last_close=_fmt2(last_c),
                )
            )

    return signals
