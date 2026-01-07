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
    kind: str  # "NEXT" or "TRIGGERED" (you can filter TRIGGERED out in main/app)
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


def _to_ny(ts: pd.Timestamp) -> pd.Timestamp:
    t = pd.to_datetime(ts)
    if t.tzinfo is None:
        # treat naive timestamps as NY dates
        return t.tz_localize(NY)
    return t.tz_convert(NY)


def _market_close_dt(date_ts: pd.Timestamp) -> pd.Timestamp:
    """
    Returns 4:30pm ET on the date of `date_ts`.
    (You asked to use 4:30pm ET as the daily close reference for next-day scanning.)
    """
    d = _to_ny(date_ts).date()
    return pd.Timestamp(year=d.year, month=d.month, day=d.day, hour=16, minute=30, tz=NY)


def last_closed_index(tf: str, df_tf: pd.DataFrame) -> int:
    """
    Returns the index (negative index) of the last CLOSED bar for a given timeframe.

    Critical behavior:
    - For W/M/Q/Y resampled bars labeled with a future period-end timestamp,
      we must treat that bar as OPEN and use the prior one.
    - For intraday bars, if we're currently inside that bar's window, it's OPEN.
    - For Daily, treat today's bar as OPEN until 4:30pm ET.
    """
    if df_tf is None or df_tf.empty or "timestamp" not in df_tf.columns:
        return -1

    tf = tf.strip().upper()
    now = pd.Timestamp.now(tz=NY)

    ts_last = _to_ny(df_tf.iloc[-1]["timestamp"])

    # Helper to decide if the last bar is open by "end timestamp in the future"
    # Works great for W-FRI, ME, QE, YE where last bar can be labeled in the future.
    if tf in ("W", "M", "Q", "Y"):
        # If the labeled period end is in the future, it's definitely open
        if ts_last > now:
            return -2
        # Even if not in the future, it could still be open for the current period:
        # Weekly: if we're before Fri 4:30pm of that labeled date
        if tf == "W":
            if now < _market_close_dt(ts_last):
                return -2
        # Monthly / Quarterly / Yearly: if we're before 4:30pm of the labeled period end date
        if tf in ("M", "Q", "Y"):
            if now < _market_close_dt(ts_last):
                return -2
        return -1

    # Daily: treat today's daily bar open until 4:30pm ET
    if tf == "D":
        if ts_last.date() == now.date() and now < _market_close_dt(ts_last):
            return -2
        return -1

    # Intraday hour-based: treat the latest bar open if now is before bar_end
    # Your resampler labels bars at the right edge (bar end).
    if tf in ("1H", "2H", "3H", "4H"):
        hours = {"1H": 1, "2H": 2, "3H": 3, "4H": 4}[tf]
        # bar end is ts_last; if now is before bar end, it's still open
        if now < ts_last:
            return -2
        # also protect against weird timestamps by ensuring now is at least end
        # (if now is between start and end, you’ll usually have end timestamp in the future already)
        return -1

    # Default: just use last row
    return -1


def _fmt2(x: float) -> float:
    try:
        return float(round(float(x), 2))
    except Exception:
        return x


def analyze_last_closed_setups(df_tf: pd.DataFrame, tf: str) -> List[StratSignal]:
    """
    Produces actionable "NEXT" plans based on the last 2 CLOSED candles (prev_closed, last_closed).
    This prevents repainting from open higher-timeframe bars.
    """
    if df_tf is None or df_tf.empty or len(df_tf) < 3:
        return []

    df_tf = df_tf.sort_values("timestamp").reset_index(drop=True)

    last_idx = last_closed_index(tf, df_tf)
    prev_idx = last_idx - 1

    if abs(prev_idx) > len(df_tf) or abs(last_idx) > len(df_tf):
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

    # We only care about actionable patterns involving 1 or 3 (your request).
    # We also keep 2U-1 / 2D-1 because those are still "inside bar" setups.
    # We are removing generic 2-2 stuff elsewhere (main/app filters).

    # ---- INSIDE BAR SETUPS (ending with 1) ----
    # prev is 2U or 2D or 3, last is 1 => inside break next candle
    if last_s == "1":
        # Break UP plan
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
        # Break DOWN plan
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

    # ---- OUTSIDE->INSIDE (3-1) ----
    if prev_s == "3" and last_s == "1":
        # same plans as inside break; keep it explicit
        # (already covered above; no extra needed)

        pass

    # ---- REVERSAL STRATS (need a 3 or a 1 in the pattern somewhere) ----
    # 2U-3 or 2D-3 are not “everywhere” but can happen; treat 3 as directionless and plan both sides
    if last_s == "3":
        # Outside bar break both ways next candle (optional: you can keep or remove later)
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
