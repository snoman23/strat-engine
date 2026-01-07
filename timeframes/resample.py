# timeframes/resample.py

import pandas as pd


def _infer_base_resolution_seconds(df: pd.DataFrame) -> float | None:
    """
    Infer median bar spacing in seconds from the timestamp series.
    Returns None if cannot infer.
    """
    if df is None or df.empty or "timestamp" not in df.columns:
        return None

    ts = pd.to_datetime(df["timestamp"], errors="coerce").dropna().sort_values()
    if len(ts) < 3:
        return None

    diffs = ts.diff().dropna()
    if diffs.empty:
        return None

    # median spacing
    return float(diffs.median().total_seconds())


def resample_timeframe(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    """
    Resample OHLCV data into STRAT-compatible timeframes.

    Required columns:
      timestamp, open, high, low, close, volume

    Behavior:
      - If resampling would effectively be "downsampling" from a coarser input,
        returns empty DF instead of raising errors (keeps scanner stable).
    """
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    # Ensure index
    df = df.set_index("timestamp")

    timeframe = timeframe.strip().upper()

    RULES = {
        "1H": "1h",
        "2H": "2h",
        "3H": "3h",
        "4H": "4h",
        "D": "1D",
        # Weekly: anchor to Friday close; avoids non-fixed <Week: weekday=4> issues
        "W": "W-FRI",
        "M": "ME",
        "Q": "QE",
        "Y": "YE",
    }

    if timeframe not in RULES:
        return pd.DataFrame()

    rule = RULES[timeframe]

    # Prevent "downsampling" for intraday hour-based targets
    # Example: input spacing ~4h, target 2h => invalid to "create" 2h bars.
    base_sec = _infer_base_resolution_seconds(df.reset_index())
    if base_sec is not None and timeframe in ("1H", "2H", "3H", "4H"):
        target_hours = {"1H": 1, "2H": 2, "3H": 3, "4H": 4}[timeframe]
        target_sec = target_hours * 3600.0

        # if input bars are coarser than target (e.g. 4h input but want 2h output), skip
        if base_sec > target_sec * 1.25:
            return pd.DataFrame()

    ohlc = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }

    out = (
        df.resample(rule, label="right", closed="right")
        .agg(ohlc)
        .dropna()
        .reset_index()
        .rename(columns={"index": "timestamp"})
    )

    return out
