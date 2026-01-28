# timeframes/resample.py

import pandas as pd


def resample_timeframe(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    """
    Resample OHLCV into STRAT-compatible timeframes.

    Required columns:
      timestamp, open, high, low, close, volume

    Key choices:
      - Weekly uses market weeks ending Friday: W-FRI
      - Month/Quarter/Year use end-based offsets: ME/QE/YE
      - label='right', closed='right' so timestamps represent bar end
    """
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp")
    df = df.set_index("timestamp")

    timeframe = timeframe.strip().upper()

    RULES = {
        # Intraday
        "1H": "1h",
        "2H": "2h",
        "3H": "3h",
        "4H": "4h",

        # Daily + Weekly (market week ends Friday)
        "D": "1D",
        "W": "W-FRI",

        # Higher TFs
        "M": "ME",
        "Q": "QE",
        "Y": "YE",
    }

    if timeframe not in RULES:
        raise ValueError(f"Unsupported timeframe: {timeframe}")

    rule = RULES[timeframe]

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
    )
    return out
