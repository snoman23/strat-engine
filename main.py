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


# =========================
# TIMEFRAMES (>= 1H ONLY)
# =========================
TARGET_TFS = ["Y", "Q", "M", "W", "D", "4H", "3H", "2H", "1H"]

DIRECT = {
    "D": ("1d", None),
    "1H": ("60m", None),
}

DERIVED = {
    "2H": ("60m", "2H"),
    "3H": ("60m", "3H"),
    "4H": ("60m", "4H"),
    "W": ("1d", "W"),
    "M": ("1d", "M"),
    "Q": ("1d", "Q"),
    "Y": ("1d", "Y"),
}


# =========================
# DATA BUILDING
# =========================
def build_timeframe_frames(ticker: str):
    feeds = {}

    for interval, cfg in DEV_YF_BASE_FEEDS.items():
        df = load_ohlc(ticker, interval=interval, period=cfg["period"])
        feeds[interval] = (
            df.sort_values("timestamp").reset_index(drop=True)
            if df is not None and not df.empty
            else pd.DataFrame()
        )

    frames = {}

    for tf, (base_interval, _) in DIRECT.items():
        base = feeds.get(base_interval)
        if base is not None and not base.empty:
            frames[tf] = base

    for tf, (base_interval, derived_tf) in DERIVED.items():
        base = feeds.get(base_interval)
        if base is not None and not base.empty:
            frames[tf] = resample_timeframe(base, derived_tf)

    return frames, feeds


def get_current_price(feeds: dict) -> float | None:
    for k in ("60m", "1d"):
        df = feeds.get(k)
        if df is not None and not df.empty:
            return float(df.iloc[-1]["close"])
    return None


# =========================
# SCANNER CORE
# =========================
def scan_ticker(ticker: str, scan_time: str):
    frames, feeds = build_timeframe_frames(ticker)

    if "D" not in frames or frames["D"].empty:
        return [], None

    current_price = get_current_price(feeds)

    classified = {}
    for tf, df in frames.items():
        if df is not None and len(df) >= 3:
            classified[tf] = classify_strat_candles(df)

    # Continuity context (higher TFs only)
    context = {}
    for tf in ("Y", "Q", "M", "W", "D"):
        df_tf = classified.get(tf)
        if df_tf is None or len(df_tf) < 3:
            continue
        idx = last_closed_index(tf, df_tf)
        context[tf] = str(df_tf.iloc[idx]["strat"])

    rows = []

    for tf in TARGET_TFS:
        df_tf = classified.get(tf)
        if df_tf is None or len(df_tf) < 3:
            continue

        signals = analyze_last_closed_setups(df_tf, tf)
        for sig in signals:
            score = None
            if sig.direction in ("bull", "bear"):
                score, _ = continuity_score(sig.direction, context)

            rows.append({
                "scan_time": scan_time,
                "ticker": ticker,
                "current_price": round(current_price, 2) if current_price else None,

                "tf": sig.tf,
                "kind": sig.kind,
                "pattern": sig.pattern,
                "setup": sig.setup,
                "dir": sig.direction,
                "entry": round(sig.entry, 2) if sig.entry else None,
                "stop": round(sig.stop, 2) if sig.stop else None,
                "score": score,

                "prev_ts": sig.prev_closed_ts,
                "prev_strat": sig.prev_strat,
                "last_ts": sig.last_closed_ts,
                "last_strat": sig.last_strat,
                "note": sig.note,
            })

    return rows, current_price


# =========================
# MAIN
# =========================
def main():
    scan_time = (
        pd.Timestamp.now(tz=ZoneInfo("America/New_York"))
        .strftime("%Y-%m-%d %H:%M:%S ET")
    )

    tickers = load_universe(min_market_cap=MIN_MARKET_CAP)
    if DEV_MODE:
        tickers = tickers[:DEV_TICKERS_LIMIT]

    print(f"Scan time: {scan_time}")
    print(f"Scanning {len(tickers)} tickers...\n")

    all_rows = []

    for ticker in tickers:
        try:
            rows, _ = scan_ticker(ticker, scan_time)
            all_rows.extend(rows)
        except Exception as e:
            print(f"Error scanning {ticker}: {e}")

    write_snapshot(all_rows)
    print(f"\nSnapshot written: {len(all_rows)} rows")


if __name__ == "__main__":
    main()
