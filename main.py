# main.py

import sys
import os
import pandas as pd
from zoneinfo import ZoneInfo

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config import DEV_MODE, DEV_TICKERS_LIMIT, MIN_MARKET_CAP, DEV_YF_BASE_FEEDS
from snapshot import write_snapshot
from universe.loader import load_universe
from loaders.yahoo import load_ohlc
from timeframes.resample import resample_timeframe
from strat.classify import classify_strat_candles
from scoring.continuity import continuity_score
from strat_signals import analyze_last_closed_setups, last_closed_index


# Keep it simple & stable
TARGET_TFS = ["Y", "Q", "M", "W", "D", "4H", "1H"]

DIRECT = {
    "D": ("1d", None),
    "1H": ("60m", None),
}

DERIVED = {
    "4H": ("60m", "4H"),
    "W": ("1d", "W"),
    "M": ("1d", "M"),
    "Q": ("1d", "Q"),
    "Y": ("1d", "Y"),
}


def _infer_resolution_seconds(df: pd.DataFrame) -> float | None:
    if df is None or df.empty or "timestamp" not in df.columns:
        return None
    ts = pd.to_datetime(df["timestamp"], errors="coerce").dropna().sort_values()
    if len(ts) < 3:
        return None
    diffs = ts.diff().dropna()
    if diffs.empty:
        return None
    return float(diffs.median().total_seconds())


def build_timeframe_frames(ticker: str):
    feeds = {}

    # Load base feeds from config
    for interval, cfg in DEV_YF_BASE_FEEDS.items():
        df = load_ohlc(ticker, interval=interval, period=cfg["period"])
        if df is not None and not df.empty:
            feeds[interval] = df.sort_values("timestamp").reset_index(drop=True)
        else:
            feeds[interval] = pd.DataFrame()

    frames = {}

    # Direct frames
    for tf, (base_interval, _) in DIRECT.items():
        base = feeds.get(base_interval, pd.DataFrame())
        if base is not None and not base.empty:
            frames[tf] = base

    # Derived frames
    for tf, (base_interval, derived_tf) in DERIVED.items():
        base = feeds.get(base_interval, pd.DataFrame())
        if base is None or base.empty:
            continue

        # Critical: only attempt intraday resamples if the input is truly ~1H bars
        if derived_tf in ("4H",):
            sec = _infer_resolution_seconds(base)
            # If it's not close to hourly bars, skip 4H for this ticker
            if sec is None or sec > 5400:  # > 1.5 hours median spacing
                continue

        out = resample_timeframe(base, derived_tf)
        if out is not None and not out.empty:
            frames[tf] = out

    return frames, feeds


def get_current_price(feeds: dict) -> float | None:
    df_60 = feeds.get("60m")
    if df_60 is not None and not df_60.empty and "close" in df_60.columns:
        try:
            return float(df_60.iloc[-1]["close"])
        except Exception:
            pass

    df_d = feeds.get("1d")
    if df_d is not None and not df_d.empty and "close" in df_d.columns:
        try:
            return float(df_d.iloc[-1]["close"])
        except Exception:
            pass

    return None


def scan_ticker(ticker: str, scan_time: str):
    frames, feeds = build_timeframe_frames(ticker)

    # Must have daily to proceed
    if "D" not in frames or frames["D"].empty or len(frames["D"]) < 50:
        return []

    current_price = get_current_price(feeds)

    classified = {}
    for tf, df in frames.items():
        if df is None or df.empty or len(df) < 3:
            continue
        df2 = classify_strat_candles(df)
        df2 = df2.sort_values("timestamp").reset_index(drop=True)
        classified[tf] = df2

    # Continuity context from last CLOSED candles (only the TFs we score)
    context = {}
    for tf in ("Y", "Q", "M", "W", "D"):
        df_tf = classified.get(tf)
        if df_tf is None or df_tf.empty or len(df_tf) < 3:
            continue
        idx = last_closed_index(tf, df_tf)
        context[tf] = str(df_tf.iloc[idx]["strat"])

    rows = []

    for tf in TARGET_TFS:
        df_tf = classified.get(tf)
        if df_tf is None or df_tf.empty or len(df_tf) < 3:
            continue

        signals = analyze_last_closed_setups(df_tf, tf)
        if not signals:
            continue

        for sig in signals:
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
                    "pattern": sig.pattern,
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

    return rows


def main():
    scan_time = (
        pd.Timestamp.now(tz=ZoneInfo("America/New_York"))
        .strftime("%Y-%m-%d %H:%M:%S %Z")
    )

    tickers = load_universe(min_market_cap=MIN_MARKET_CAP)

    if DEV_MODE:
        tickers = tickers[:DEV_TICKERS_LIMIT]
        print("[Config] DEV_MODE=True")

    print(f"[Config] MIN_MARKET_CAP={MIN_MARKET_CAP:,}")
    print(f"Scan time: {scan_time}")
    print(f"Scanning {len(tickers)} tickers...\n")

    all_rows = []
    ok = 0
    skipped = 0

    for t in tickers:
        try:
            rows = scan_ticker(t, scan_time)
            if rows:
                all_rows.extend(rows)
                ok += 1
            else:
                skipped += 1
        except Exception:
            # Don’t spam logs; universe scans will always have a few weird tickers.
            skipped += 1
            continue

    write_snapshot(all_rows)
    print(f"\nDone. tickers_with_rows={ok}, skipped_or_empty={skipped}, rows={len(all_rows)}")
    print("Snapshot written.\n")


if __name__ == "__main__":
    main()
