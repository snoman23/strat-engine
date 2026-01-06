# strat_signals.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional

import pandas as pd

# No TRIGGERED rows. Only NEXT plans.
INCLUDE_22_CONTINUATION_PLANS: bool = False


@dataclass(frozen=True)
class StratSignal:
    tf: str
    kind: str                 # "NEXT"
    pattern: str              # e.g. "2U-2D" (last two closed candles)
    setup: str                # e.g. "BULL_REV_AFTER_2U-2D" (what to trade next)
    direction: Optional[str]  # "bull" | "bear" | None

    prev_closed_ts: Any
    last_closed_ts: Any

    prev_strat: str
    last_strat: str

    prev_open: float
    prev_high: float
    prev_low: float
    prev_close: float

    last_open: float
    last_high: float
    last_low: float
    last_close: float

    entry: Optional[float] = None
    stop: Optional[float] = None
    actionable: str = ""
    note: str = ""


def _tf_to_seconds(tf: str) -> int:
    tf = tf.strip().upper()
    if tf in ("1H", "60M", "60"):
        return 60 * 60
    if tf == "2H":
        return 2 * 60 * 60
    if tf == "3H":
        return 3 * 60 * 60
    if tf == "4H":
        return 4 * 60 * 60
    if tf == "30M":
        return 30 * 60
    if tf == "15M":
        return 15 * 60
    if tf == "10M":
        return 10 * 60
    if tf == "5M":
        return 5 * 60
    if tf == "D":
        return 24 * 60 * 60
    return 60 * 60


def last_closed_index(tf: str, df: Optional[pd.DataFrame] = None) -> int:
    """
    Closed-bar rules:
    - Y/Q/M/W: always use -2 (current period bar is still forming)
    - Intraday + D: if last row is still forming, use -2, else -1
    """
    tf = tf.strip().upper()

    if tf in ("Y", "Q", "M", "W"):
        return -2

    if df is None or df.empty or "timestamp" not in df.columns or len(df) < 3:
        return -2

    df = df.sort_values("timestamp").reset_index(drop=True)

    dur = pd.Timedelta(seconds=_tf_to_seconds(tf))
    last_ts = pd.to_datetime(df.iloc[-1]["timestamp"])
    now = pd.Timestamp.now(tz=last_ts.tz) if getattr(last_ts, "tz", None) is not None else pd.Timestamp.now()

    # If now is before bar end -> last row is in-progress
    if now < last_ts + dur:
        return -2

    return -1


def _sorted(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    return df.sort_values("timestamp").reset_index(drop=True)


def _closed_pair(df: pd.DataFrame, tf: str):
    df = _sorted(df)
    idx_last = last_closed_index(tf, df)
    last_pos = len(df) + idx_last
    prev_pos = last_pos - 1
    if prev_pos < 0:
        return None, None
    return df.iloc[prev_pos], df.iloc[last_pos]


def analyze_last_closed_setups(df: pd.DataFrame, tf: str) -> List[StratSignal]:
    tf_u = tf.strip().upper()

    if df is None or df.empty or len(df) < 3:
        return []
    if "strat" not in df.columns:
        raise ValueError("Missing 'strat' column. Run classify_strat_candles(df) first.")

    df = _sorted(df)
    prev, last = _closed_pair(df, tf_u)
    if prev is None or last is None:
        return []

    prev_s = str(prev["strat"]).upper()
    last_s = str(last["strat"]).upper()

    prev_ts = prev["timestamp"]
    last_ts = last["timestamp"]

    pattern = f"{prev_s}-{last_s}"

    def base_signal(**kwargs) -> dict:
        return dict(
            tf=tf_u,
            kind="NEXT",
            pattern=pattern,
            prev_closed_ts=prev_ts,
            last_closed_ts=last_ts,
            prev_strat=prev_s,
            last_strat=last_s,
            prev_open=float(prev["open"]),
            prev_high=float(prev["high"]),
            prev_low=float(prev["low"]),
            prev_close=float(prev["close"]),
            last_open=float(last["open"]),
            last_high=float(last["high"]),
            last_low=float(last["low"]),
            last_close=float(last["close"]),
            **kwargs,
        )

    last_high = float(last["high"])
    last_low = float(last["low"])

    signals: List[StratSignal] = []

    # 1) X-1 Inside bar => two-sided plan (UP and DOWN)
    if last_s == "1":
        signals.append(
            StratSignal(
                **base_signal(
                    setup=f"INSIDE_BREAK_UP_AFTER_{pattern}",
                    direction="bull",
                    entry=last_high,
                    stop=last_low,
                    actionable=f"ALERT if price > {last_high:.2f} (inside break UP); stop < {last_low:.2f}",
                    note="Inside bar: next candle can break either direction.",
                )
            )
        )
        signals.append(
            StratSignal(
                **base_signal(
                    setup=f"INSIDE_BREAK_DOWN_AFTER_{pattern}",
                    direction="bear",
                    entry=last_low,
                    stop=last_high,
                    actionable=f"ALERT if price < {last_low:.2f} (inside break DOWN); stop > {last_high:.2f}",
                    note="Inside bar: next candle can break either direction.",
                )
            )
        )

    # 2) Two-bar reversal patterns => NEXT reversal plan
    # Pattern: 2U-2D => plan: bull reversal (next candle can become 2U via break above 2D high)
    if prev_s == "2U" and last_s == "2D":
        signals.append(
            StratSignal(
                **base_signal(
                    setup="BULL_REV_AFTER_2U-2D",
                    direction="bull",
                    entry=last_high,
                    stop=last_low,
                    actionable=f"ALERT if price > {last_high:.2f} (bull reversal); stop < {last_low:.2f}",
                    note="After 2U then 2D: watch next candle for bull reversal above 2D high.",
                )
            )
        )

    # Pattern: 2D-2U => plan: bear reversal (next candle can become 2D via break below 2U low)
    if prev_s == "2D" and last_s == "2U":
        signals.append(
            StratSignal(
                **base_signal(
                    setup="BEAR_REV_AFTER_2D-2U",
                    direction="bear",
                    entry=last_low,
                    stop=last_high,
                    actionable=f"ALERT if price < {last_low:.2f} (bear reversal); stop > {last_high:.2f}",
                    note="After 2D then 2U: watch next candle for bear reversal below 2U low.",
                )
            )
        )

    # 3) RevStrat AFTER inside break: show only the REV watch plan
    # Pattern: 1-2U => watch for 2D reversal by breaking the 2U low
    if prev_s == "1" and last_s == "2U":
        signals.append(
            StratSignal(
                **base_signal(
                    setup="REVSTRAT_BEAR_AFTER_1-2U",
                    direction="bear",
                    entry=last_low,
                    stop=last_high,
                    actionable=f"ALERT if price < {last_low:.2f} (RevStrat bear); stop > {last_high:.2f}",
                    note="RevStrat: after 1-2U, watch for 2D reversal.",
                )
            )
        )

    # Pattern: 1-2D => watch for 2U reversal by breaking the 2D high
    if prev_s == "1" and last_s == "2D":
        signals.append(
            StratSignal(
                **base_signal(
                    setup="REVSTRAT_BULL_AFTER_1-2D",
                    direction="bull",
                    entry=last_high,
                    stop=last_low,
                    actionable=f"ALERT if price > {last_high:.2f} (RevStrat bull); stop < {last_low:.2f}",
                    note="RevStrat: after 1-2D, watch for 2U reversal.",
                )
            )
        )

    # Optional 2-2 continuation plans (OFF by default)
    if INCLUDE_22_CONTINUATION_PLANS and prev_s in ("2U", "2D") and last_s == prev_s:
        if last_s == "2U":
            signals.append(
                StratSignal(
                    **base_signal(
                        setup="BULL_CONTINUATION_AFTER_2U-2U",
                        direction="bull",
                        entry=last_high,
                        stop=last_low,
                        actionable=f"ALERT if price > {last_high:.2f} (continuation bull); stop < {last_low:.2f}",
                        note="Optional continuation plan.",
                    )
                )
            )
        else:
            signals.append(
                StratSignal(
                    **base_signal(
                        setup="BEAR_CONTINUATION_AFTER_2D-2D",
                        direction="bear",
                        entry=last_low,
                        stop=last_high,
                        actionable=f"ALERT if price < {last_low:.2f} (continuation bear); stop > {last_high:.2f}",
                        note="Optional continuation plan.",
                    )
                )
            )

    return signals
