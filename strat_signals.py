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
        return t.tz_localize(NY)
    return t.tz_convert(NY)


def _market_close_dt(date_ts: pd.Timestamp) -> pd.Timestamp:
    """
    Use 4:30pm ET as our daily close reference (your preference).
    """
    d = _to_ny(date_ts).date()
    return pd.Timestamp(year=d.year, month=d.month, day=d.day, hour=16, minute=30, tz=NY)


def last_closed_index(tf: str, df_tf: pd.DataFrame) -> int:
    """
    Returns negative index of last CLOSED bar for given timeframe.

    Key behavior:
    - 1H from Yahoo is typically timestamped by BAR START time → closed if now >= ts + 1h
    - 2H/3H/4H resampled with label='right' → timestamp is BAR END → closed if now >= ts
    - W/M/Q/Y labels can be in the future for open periods → use prior bar
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
    """
    Generates actionable NEXT plans using the last 2 CLOSED candles:
      prev_closed, last_closed

    Includes:
      - Inside break (last = 1) both directions
      - Outside break (last = 3) both directions
      - RevStrat watch after 1-2 or 3-2 (i.e., play for 1-2-2 or 3-2-2 reversal)
    """
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

    prev_high = float(prev["high"])
    prev_low = float(prev["low"])
    last_high = float(last["high"])
    last_low = float(last["low"])

    prev_ts = pd.to_datetime(prev["timestamp"])
    last_ts = pd.to_datetime(last["timestamp"])

    signals: List[StratSignal] = []

    # -----------------------------
    # 1) INSIDE BAR (last = 1): play for 1-2 (break either way)
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
                actionable=f"ALERT if price > {_fmt2(last_high)} (inside break UP); stop < {_fmt2(last_low)}",
                entry=_fmt2(last_high),
                stop=_fmt2(last_low),
                note="Inside bar break UP (1-2)",
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
        # Break DOWN
        signals.append(
            StratSignal(
                tf=tf,
                kind="NEXT",
                pattern=f"{prev_s}-1",
                setup="1-2 BREAK_DOWN",
                direction="bear",
                actionable=f"ALERT if price < {_fmt2(last_low)} (inside break DOWN); stop > {_fmt2(last_high)}",
                entry=_fmt2(last_low),
                stop=_fmt2(last_high),
                note="Inside bar break DOWN (1-2)",
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
    # 2) OUTSIDE BAR (last = 3): play for 3-2 (break either way)
    # -----------------------------
    if last_s == "3":
        # Break UP
        signals.append(
            StratSignal(
                tf=tf,
                kind="NEXT",
                pattern=f"{prev_s}-3",
                setup="3-2 BREAK_UP",
                direction="bull",
                actionable=f"ALERT if price > {_fmt2(last_high)} (outside break UP); stop < {_fmt2(last_low)}",
                entry=_fmt2(last_high),
                stop=_fmt2(last_low),
                note="Outside bar break UP (3-2)",
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
        # Break DOWN
        signals.append(
            StratSignal(
                tf=tf,
                kind="NEXT",
                pattern=f"{prev_s}-3",
                setup="3-2 BREAK_DOWN",
                direction="bear",
                actionable=f"ALERT if price < {_fmt2(last_low)} (outside break DOWN); stop > {_fmt2(last_high)}",
                entry=_fmt2(last_low),
                stop=_fmt2(last_high),
                note="Outside bar break DOWN (3-2)",
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
    # 3) REVSTRAT WATCH: after 1-2 or 3-2 already happened
    # Pattern: (1 or 3) then (2U or 2D)
    # We show a WATCH for 1-2-2 or 3-2-2 reversal setup.
    # -----------------------------
    if prev_s in ("1", "3") and last_s in ("2U", "2D"):
        # After 1-2U or 3-2U -> watch for bearish reversal (2D)
        if last_s == "2U":
            signals.append(
                StratSignal(
                    tf=tf,
                    kind="NEXT",
                    pattern=f"{prev_s}-2U",
                    setup=f"REVSTRAT {prev_s}-2-2 (watch 2D)",
                    direction="bear",
                    actionable=f"ALERT if price < {_fmt2(last_low)} (RevStrat bear); stop > {_fmt2(last_high)}",
                    entry=_fmt2(last_low),
                    stop=_fmt2(last_high),
                    note=f"RevStrat after {prev_s}-2U: watch for 2D reversal (i.e., {prev_s}-2U-2D)",
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

        # After 1-2D or 3-2D -> watch for bullish reversal (2U)
        if last_s == "2D":
            signals.append(
                StratSignal(
                    tf=tf,
                    kind="NEXT",
                    pattern=f"{prev_s}-2D",
                    setup=f"REVSTRAT {prev_s}-2-2 (watch 2U)",
                    direction="bull",
                    actionable=f"ALERT if price > {_fmt2(last_high)} (RevStrat bull); stop < {_fmt2(last_low)}",
                    entry=_fmt2(last_high),
                    stop=_fmt2(last_low),
                    note=f"RevStrat after {prev_s}-2D: watch for 2U reversal (i.e., {prev_s}-2D-2U)",
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
