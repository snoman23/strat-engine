import pandas as pd

NY_TZ = "America/New_York"


def _ensure_timestamp_index(df: pd.DataFrame) -> pd.DataFrame:
    required = {"timestamp", "open", "high", "low", "close", "volume"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    out = df.copy()
    out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce")
    out = out.dropna(subset=["timestamp"])

    # Normalize timezone to NY
    if out["timestamp"].dt.tz is None:
        out["timestamp"] = out["timestamp"].dt.tz_localize(NY_TZ)
    else:
        out["timestamp"] = out["timestamp"].dt.tz_convert(NY_TZ)

    out = out.sort_values("timestamp").set_index("timestamp")
    return out


def _infer_input_resolution_seconds(idx: pd.DatetimeIndex) -> float:
    if len(idx) < 3:
        return float("inf")
    diffs = idx.to_series().diff().dropna()
    if diffs.empty:
        return float("inf")
    return diffs.median().total_seconds()


def resample_timeframe(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    """
    Resample OHLCV data into STRAT-compatible timeframes.

    Supports:
    5M, 10M, 15M, 30M, 60, 1H, 2H, 3H, 4H, D, W, M, Q, Y

    Notes:
    - Intraday aligned to :30 boundaries (12:30/16:30/20:30, etc.)
    - Weekly ends Friday (W-FRI)
    - M/Q/Y use end-of-period anchors (ME/QE/YE)
    - Prevents resampling DOWN for fixed-size rules (minute/hour/day).
      For non-fixed rules (W-FRI/ME/QE/YE), we skip the seconds-based guard.
    """

    df_idx = _ensure_timestamp_index(df)
    tf = timeframe.strip().upper()

    RULES = {
        "5M": "5min",
        "10M": "10min",
        "15M": "15min",
        "30M": "30min",
        "60": "60min",
        "60M": "60min",
        "1H": "1h",
        "2H": "2h",
        "3H": "3h",
        "4H": "4h",
        "D": "1D",
        "W": "W-FRI",
        "M": "ME",
        "Q": "QE",
        "Y": "YE",
    }

    if tf not in RULES:
        raise ValueError(f"Unsupported timeframe: {timeframe}")

    rule = RULES[tf]

    is_intraday = tf in {"5M", "10M", "15M", "30M", "60", "60M", "1H", "2H", "3H", "4H"}
    align_args = {"origin": "start_day", "offset": "30min"} if is_intraday else {}

    # --- Downsample guard (fixed-size only) ---
    input_sec = _infer_input_resolution_seconds(df_idx.index)

    target_sec = None
    try:
        off = pd.tseries.frequencies.to_offset(rule)
        # For fixed offsets, nanos is defined. For non-fixed (W-FRI/ME/QE/YE), this raises.
        target_sec = off.nanos / 1e9
    except Exception:
        target_sec = None  # non-fixed frequency => skip guard

    if target_sec is not None and target_sec < input_sec:
        raise ValueError(
            f"Cannot resample DOWN into {timeframe} from input resolution ~{int(input_sec)}s. "
            f"Load a smaller interval from Yahoo first (e.g., 60m/30m/15m/5m), then resample up."
        )

    ohlc = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }

    out = (
        df_idx.resample(rule, label="right", closed="right", **align_args)
        .agg(ohlc)
        .dropna()
        .reset_index()
    )

    return out
