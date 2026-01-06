# main.py

import sys
import os
import pandas as pd
from zoneinfo import ZoneInfo

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config import DEV_MODE, DEV_TICKERS_LIMIT, MIN_MARKET_CAP, YF_BASE_FEEDS
from snapshot import write_snapshot

from scheduler import should_run_for_any_timeframe, record_timeframes_run

from universe.loader import load_universe
from loaders.yahoo import load_ohlc
from timeframes.resample import resample_timeframe
from strat.classify import classify_strat_candles
from scoring.continuity import continuity_score
from strat_signals import analyze_last_closed_setups, last_closed_index

ET = ZoneInfo("America/New_York")

# ------------------------------------------------------------
# Timeframes: keep 30M+ only (because scheduler runs every 10m)
# ------------------------------------------------------------
TARGET_TFS = ["Y", "Q", "M", "W", "D", "4H", "3H", "2H", "1H", "30M"]

# Pull directly from Yahoo for what it already provides
DIRECT = {
    "D": ("1d", None),
    "1H": ("60m", None),
    "30M": ("30m", None),
}

# Derive (resample) everything else from the closest base feed
DERIVED = {
    "2H": ("60m", "2H"),
    "3H": ("60m", "3H"),
    "4H": ("60m", "4H"),
    "W": ("1d", "W"),
    "M": ("1d", "M"),
    "Q": ("1d", "Q"),
    "Y": ("1d", "Y"),
}


def build_timeframe_frames(ticker: str):
    """
    Loads base Yahoo feeds from YF_BASE_FEEDS, then builds all requested timeframes
    using DIRECT pulls + DERIVED resamples.

    Returns:
      frames: dict[str, DataFrame] (per timeframe like "4H", "D", "M")
      feeds: dict[str, DataFrame]  (raw yahoo feeds like "1d", "60m", "30m")
    """
    feeds = {}

    for interval, cfg in YF_BASE_FEEDS.items():
        df = load_ohlc(ticker, interval=interval, period=cfg["period"])
        if df is not None and not df.empty:
            feeds[interval] = df.sort_values("timestamp").reset_index(drop=True)
        else:
            feeds[interval] = pd.DataFrame()

    frames = {}

    # Direct feeds mapped to timeframe keys
    for tf, (base_interval, _) in DIRECT.items():
        base = feeds.get(base_interval, pd.DataFrame())
        if base is not None and not base.empty:
            frames[tf] = base

    # Resampled / derived timeframes
    for tf, (base_interval, derived_tf) in DERIVED.items():
        base = feeds.get(base_interval, pd.DataFrame())
        if base is not None and not base.empty:
            frames[tf] = resample_timeframe(base, derived_tf)

    return frames, feeds


def get_current_price(feeds: dict) -> float | None:
    """
    Best-effort current price:
    - prefer 30m last close
    - fallback to 60m last close
    - fallback to daily last close
    """
    for k in ("30m", "60m", "1d"):
        df = feeds.get(k)
        if df is not None and not df.empty and "close" in df.columns:
            try:
                return float(df.iloc[-1]["close"])
            except Exception:
                pass
    return None


def _setup_end_position(df_tf: pd.DataFrame, tf: str) -> int:
    idx = last_closed_index(tf, df_tf)
    return len(df_tf) + idx


def _next_plan_already_triggered(df_tf: pd.DataFrame, tf: str, sig) -> bool:
    """
    Prevent stale NEXT plans:
    - bull: later high > entry
    - bear: later low < entry
    """
    if getattr(sig, "kind", None) != "NEXT":
        return False
    if getattr(sig, "entry", None) is None or getattr(sig, "direction", None) not in ("bull", "bear"):
        return False

    df_tf = df_tf.sort_values("timestamp").reset_index(drop=True)
    end_pos = _setup_end_position(df_tf, tf)

    later = df_tf.iloc[end_pos + 1 :]
    if later.empty:
        return False

    if sig.direction == "bull":
        return (later["high"] > sig.entry).any()
    else:
        return (later["low"] < sig.entry).any()


def scan_ticker(ticker: str, scan_time: str):
    frames, feeds = build_timeframe_frames(ticker)

    # Basic guard: needs daily history
    if "D" not in frames or frames["D"].empty or len(frames["D"]) < 50:
        return [], None

    current_price = get_current_price(feeds)

    # Classify all frames
    classified = {}
    for tf, df in frames.items():
        if df is None or df.empty or len(df) < 3:
            continue
        df2 = classify_strat_candles(df)
        df2 = df2.sort_values("timestamp").reset_index(drop=True)
        classified[tf] = df2

    # Continuity context from last CLOSED candles
    context = {}
    for tf in ("Y", "Q", "M", "W", "D"):
        df_tf = classified.get(tf)
        if df_tf is None or df_tf.empty or len(df_tf) < 3:
            continue
        idx = last_closed_index(tf, df_tf)
        context[tf] = str(df_tf.iloc[idx]["strat"])

    rows = []

    # Scan all target timeframes
    for tf in TARGET_TFS:
        df_tf = classified.get(tf)
        if df_tf is None or df_tf.empty or len(df_tf) < 3:
            continue

        signals = analyze_last_closed_setups(df_tf, tf)
        if not signals:
            continue

        for sig in signals:
            # Drop stale NEXT plans (price already crossed entry after the setup candle)
            if _next_plan_already_triggered(df_tf, tf, sig):
                continue

            score = None
            if sig.direction in ("bull", "bear"):
                score, _ = continuity_score(sig.direction, context)

            rows.append(
                {
                    "scan_time": scan_time,
                    "ticker": ticker,
                    "current_price": current_price,

                    "tf": sig.tf,
                    "kind": sig.kind,

                    # What last 2 closed candles were
                    "pattern": sig.pattern,

                    # What trade plan we are proposing next
                    "setup": sig.setup,

                    "dir": sig.direction,
                    "actionable": sig.actionable,
                    "entry": sig.entry,
                    "stop": sig.stop,
                    "score": score,

                    "prev_ts": str(sig.prev_closed_ts),
                    "prev_strat": sig.prev_strat,
                    "prev_high": sig.prev_high,
                    "prev_low": sig.prev_low,

                    "last_ts": str(sig.last_closed_ts),
                    "last_strat": sig.last_strat,
                    "last_high": sig.last_high,
                    "last_low": sig.last_low,

                    "note": sig.note,
                }
            )

    return rows, current_price


def main():
    # Gate the scan to only run when at least one timeframe has a new CLOSED candle
    should_run, debug = should_run_for_any_timeframe(TARGET_TFS)
    print("[Scheduler] Check:", debug)

    if not should_run:
        print("[Scheduler] No new closed candles detected across target timeframes. Exiting.")
        return

    scan_time = pd.Timestamp.now(tz=ET).strftime("%Y-%m-%d %H:%M:%S %Z")

    tickers = load_universe(min_market_cap=MIN_MARKET_CAP)

    if DEV_MODE:
        tickers = tickers[:DEV_TICKERS_LIMIT]

    print(f"Scan time: {scan_time}")
    print(f"Scanning {len(tickers)} tickers...\n")

    all_rows = []

    for ticker in tickers:
        try:
            rows, px = scan_ticker(ticker, scan_time)
            all_rows.extend(rows)

            if rows:
                print(f"\n====================\nTICKER: {ticker} | current_price={px}\n====================")
                df_out = pd.DataFrame(rows)

                cols = [
                    "tf", "kind", "pattern", "setup", "dir", "current_price", "score",
                    "actionable", "entry", "stop",
                    "prev_ts", "prev_strat", "prev_high", "prev_low",
                    "last_ts", "last_strat", "last_high", "last_low",
                ]
                cols = [c for c in cols if c in df_out.columns]
                print(df_out[cols].to_string(index=False))

        except Exception as e:
            print(f"Error scanning {ticker}: {e}")

    write_snapshot(all_rows)
    record_timeframes_run(TARGET_TFS)

    print(f"\nSnapshot written: {len(all_rows)} rows\n")
    print("[Scheduler] Recorded last closed timestamps for this run.")


if __name__ == "__main__":
    main()
