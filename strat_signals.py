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
    pattern: str
    setup: str
    direction: Optional[str]  # "bull" | "bear" | None

    actionable: str
    entry: Optional[float]
    stop: Optional[float]
    note: str

    prev_closed_ts: pd.Timestamp
    prev_strat: str
    prev_high: float
    prev_low: float

    last_closed_ts: pd.Timestamp
    last_strat: str
    last_high: float
    last_low: float


def _to_ny(ts) -> pd.Timestamp:
    t = pd.to_datetime(ts, errors="coerce")
    if pd.isna(t):
        return pd.Timestamp.now(tz=NY)
    if t.tzinfo is None:
        # interpret naive as NY
        return t.tz_localize(NY)
    return t.tz_convert(NY)


def _market_close_dt(date_ts: pd.Timestamp) -> pd.Timestamp:
    """
    Use 4:30pm ET as our "day close" reference (your earlier preference).
    """
    d = _to_ny(date_ts).date()
    return pd.Timestamp(year=d.year, month=d.month, day=d.day, hour=16, minute=30, tz=NY)


def last_closed_index(tf: str, df_tf: pd.DataFrame) -> int:
    """
    Returns negative index of last CLOSED bar for given timeframe.

    Key fix:
    - 1H data from yfinance is typically timestamped by BAR START time.
      So a bar is CLOSED if now >= timestamp + 1 hour.
    - 2H/3H/4H are resampled by us with label='right' (timestamp is BAR END),
      so a bar is CLOSED if now >= timestamp.
    - W/M/Q/Y resampled label right -> timestamp often in the future for current open period.
    """
    if df_tf is None or df_tf.empty or "timestamp" not in df_tf.columns:
        return -1

    tf = tf.strip().upper()
    now = pd.Timestamp.now(tz=NY)

    # last row timestamp in NY
    ts_last = _to_ny(df_tf.iloc[-1]["timestamp"])

    # -----------------------------
    # Higher TFs labeled at period end
    # -----------------------------
    if tf in ("W", "M", "Q", "Y"):
        # If label is in the future, it's definitely open
        if ts_last > now:
            return -2

        # Also treat it open until 4:30pm of its label date
        if now < _market_close_dt(ts_last):
            return -2

        return -1

    # -----------------------------
    # Daily: today's bar open until 4:30pm ET
    # -----------------------------
    if tf == "D":
        if ts_last.date() == now.date() and now < _market_close_dt(ts_last):
            return -2
        return -1

    # -----------------------------
    # 1H: timestamp is BAR START (Yahoo chart convention)
    # bar end = start + 1 hour
    # -----------------------------
    if tf == "1H":
        bar_end = ts_last + pd.Timedelta(hours=1)
        if now < bar_end:
            return -2
        return -1

    # -----------------------------
    # 2H/3H/4H: these are resampled by us with label='right' => timestamp is BAR END
    # so open if now < ts_last
    # -----------------------------
    if tf in ("2H", "3H", "4H"):
        if now < ts_last:
            return -2
        return -1

    # Default
    return -1


def _fmt2(x: float) -> float:
    try:
        return float(round(float(x), 2))
    except Exception:
        return x


def analyze_last_closed_setups(df_tf: pd.DataFrame, tf: str) -> List[StratSignal]:
    """
    Generates NEXT plans using the last 2 CLOSED candles:
      prev_closed, last_closed
    """
    if df_tf is None or df_tf.empty or len(df_tf) < 3:
        return []

    df_tf = df_tf.sort_values("timestamp").reset_index(drop=True)

    last_idx = last_closed_index(tf, df_tf)
    prev_idx = last_idx - 1

    # guard
    if len(df_tf) < abs(prev_idx):
        return []

    prev = df_tf.iloc[prev_idx]
    last = df_tf.iloc[last_idx]

    prev_s = str(prev.get("strat"))
    last_s = str(last.get("strat"))

    prev_high = float(prev["high"])
    prev_low = float(prev["low"])
    last_high = float(last["high"])
    last_low = float(last["low"])

    prev_ts = pd.to_datetime(prev["timestamp"])
    last_ts = pd.to_datetime(last["timestamp"])

    signals: List[StratSignal] = []

    # -----------------------------
    # INSIDE BAR (last = 1): plan both breaks
    # -----------------------------
    if last_s == "1":
        signals.append(
            StratSignal(
                tf=tf,
                kind="NEXT",
                pattern=f"{prev_s}-1",
                setup="INSIDE_BREAK_UP",
                direction="bull",
                actionable=f"ALERT if price > {_fmt2(last_high)} (inside break UP); stop < {_fmt2(last_low)}",
                entry=_fmt2(last_high),
                stop=_fmt2(last_low),
                note="Inside break UP",
                prev_closed_ts=prev_ts,
                prev_strat=prev_s,
                prev_high=_fmt2(prev_high),
                prev_low=_fmt2(prev_low),
                last_closed_ts=last_ts,
                last_strat=last_s,
                last_high=_fmt2(last_high),
                last_low=_fmt2(last_low),
            )
        )
        signals.append(
            StratSignal(
                tf=tf,
                kind="NEXT",
                pattern=f"{prev_s}-1",
                setup="INSIDE_BREAK_DOWN",
                direction="bear",
                actionable=f"ALERT if price < {_fmt2(last_low)} (inside break DOWN); stop > {_fmt2(last_high)}",
                entry=_fmt2(last_low),
                stop=_fmt2(last_high),
                note="Inside break DOWN",
                prev_closed_ts=prev_ts,
                prev_strat=prev_s,
                prev_high=_fmt2(prev_high),
                prev_low=_fmt2(prev_low),
                last_closed_ts=last_ts,
                last_strat=last_s,
                last_high=_fmt2(last_high),
                last_low=_fmt2(last_low),
            )
        )

    # -----------------------------
    # OUTSIDE BAR (last = 3): plan both breaks
    # -----------------------------
    if last_s == "3":
        signals.append(
            StratSignal(
                tf=tf,
                kind="NEXT",
                pattern=f"{prev_s}-3",
                setup="OUTSIDE_BREAK_UP",
                direction="bull",
                actionable=f"ALERT if price > {_fmt2(last_high)} (outside break UP); stop < {_fmt2(last_low)}",
                entry=_fmt2(last_high),
                stop=_fmt2(last_low),
                note="Outside break UP",
                prev_closed_ts=prev_ts,
                prev_strat=prev_s,
                prev_high=_fmt2(prev_high),
                prev_low=_fmt2(prev_low),
                last_closed_ts=last_ts,
                last_strat=last_s,
                last_high=_fmt2(last_high),
                last_low=_fmt2(last_low),
            )
        )
        signals.append(
            StratSignal(
                tf=tf,
                kind="NEXT",
                pattern=f"{prev_s}-3",
                setup="OUTSIDE_BREAK_DOWN",
                direction="bear",
                actionable=f"ALERT if price < {_fmt2(last_low)} (outside break DOWN); stop > {_fmt2(last_high)}",
                entry=_fmt2(last_low),
                stop=_fmt2(last_high),
                note="Outside break DOWN",
                prev_closed_ts=prev_ts,
                prev_strat=prev_s,
                prev_high=_fmt2(prev_high),
                prev_low=_fmt2(prev_low),
                last_closed_ts=last_ts,
                last_strat=last_s,
                last_high=_fmt2(last_high),
                last_low=_fmt2(last_low),
            )
        )

    return signals
