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
from strat_signals import analyze_last_closed_setups, last_closed_index


# Only show TF >= 1H
TARGET_TFS = ["Y", "Q", "M", "W", "D", "4H", "3H", "2H", "1H"]

# Pull only what Yahoo already provides
DIRECT = {
    "D": ("1d", None),
    "1H": ("60m", None),
}

# Derive the rest from 60m or 1d
DERIVED = {
    "2H": ("60m", "2H"),
    "3H": ("60m", "3H"),
    "4H": ("60m", "4H"),
    "W": ("1d", "W"),
    "M": ("1d", "M"),
    "Q": ("1d", "Q"),
    "Y": ("1d", "Y"),
}


WEIGHTS = {"Y": 5, "Q": 4, "M": 3, "W": 2, "D": 1}


def build_timeframe_frames(ticker: str):
    # Choose feeds (DEV or FULL)
    YF_BASE_FEEDS = DEV_YF_BASE_FEEDS if DEV_MODE else {
        "1d": {"period": "max"},
        "60m": {"period": "730d"},  # Yahoo limit for many tickers
    }

    feeds = {}
    for interval, cfg in YF_BASE_FEEDS.items():
        df = load_ohlc(ticker, interval=interval, period=cfg["period"])
        feeds[interval] = (
            df.sort_values("timestamp").reset_index(drop=True)
            if df is not None and not df.empty
            else pd.DataFrame()
        )

    frames = {}

    # Direct TFs
    for tf, (base_interval, _) in DIRECT.items():
        base = feeds.get(base_interval, pd.DataFrame())
        if base is not None and not base.empty:
            frames[tf] = base

    # Derived TFs
    for tf, (base_interval, derived_tf) in DERIVED.items():
        base = feeds.get(base_interval, pd.DataFrame())
        if base is not None and not base.empty:
            frames[tf] = resample_timeframe(base, derived_tf)

    return frames, feeds


def get_current_price(feeds: dict) -> float | None:
    # Prefer last close from 60m, else daily
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


def compute_bias_score(context: dict) -> int:
    """
    Score = market bias only (NOT per-signal direction).
    + score => higher TFs bullish (more 2U)
    - score => higher TFs bearish (more 2D)
    """
    score = 0
    for tf, strat in context.items():
        w = WEIGHTS.get(tf, 0)
        if strat == "2U":
            score += w
        elif strat == "2D":
            score -= w
    return score


def scan_ticker(ticker: str, scan_time: str):
    frames, feeds = build_timeframe_frames(ticker)

    # Need at least daily history to do anything useful
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

    # Continuity context from last CLOSED candles (Y/Q/M/W/D)
    context = {}
    for tf in ("Y", "Q", "M", "W", "D"):
        df_tf = classified.get(tf)
        if df_tf is None or df_tf.empty or len(df_tf) < 3:
            continue
        idx = last_closed_index(tf, df_tf)
        context[tf] = str(df_tf.iloc[idx]["strat"])

    bias_score = compute_bias_score(context)

    rows = []

    for tf in TARGET_TFS:
        df_tf = classified.get(tf)
        if df_tf is None or df_tf.empty or len(df_tf) < 3:
            continue

        signals = analyze_last_closed_setups(df_tf, tf)
        if not signals:
            continue

        for sig in signals:
            # Remove TRIGGERED rows completely (your request)
            if getattr(sig, "kind", "") == "TRIGGERED":
                continue

            # Round prices for output readability
            entry = round(float(sig.entry), 2) if sig.entry is not None else None
            stop = round(float(sig.stop), 2) if sig.stop is not None else None

            px = round(float(current_price), 2) if current_price is not None else None

            # Chart link (for UI)
            chart_url = f"https://finance.yahoo.com/quote/{ticker}/chart"

            # Helpful: whether signal aligns with bias
            aligned = None
            if sig.direction in ("bull", "bear"):
                if sig.direction == "bull":
                    aligned = bias_score > 0
                else:
                    aligned = bias_score < 0

            rows.append(
                {
                    "scan_time": scan_time,
                    "ticker": ticker,
                    "chart_url": chart_url,
                    "current_price": px,

                    "tf": sig.tf,
                    "pattern": sig.pattern,   # last 2 closed candles pattern
                    "setup": sig.setup,       # what we are planning to trade next
                    "dir": sig.direction,

                    "entry": entry,
                    "stop": stop,

                    # IMPORTANT: score is market bias only (not flipped per setup)
                    "score": bias_score,
                    "aligned": aligned,

                    "actionable": sig.actionable,
                    "note": sig.note,

                    # keep raw context for debugging / future features
                    "ctx_Y": context.get("Y"),
                    "ctx_Q": context.get("Q"),
                    "ctx_M": context.get("M"),
                    "ctx_W": context.get("W"),
                    "ctx_D": context.get("D"),
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
        print("\n[Universe] DEV mode active\n")

    print(f"Scan time: {scan_time}")
    print(f"Scanning {len(tickers)} tickers...\n")

    all_rows = []

    for ticker in tickers:
        try:
            rows = scan_ticker(ticker, scan_time)
            all_rows.extend(rows)

            if rows:
                print(f"\n====================\nTICKER: {ticker}\n====================")
                df_out = pd.DataFrame(rows)

                cols = [
                    "tf", "pattern", "setup", "dir",
                    "current_price", "entry", "stop",
                    "score", "aligned",
                    "actionable",
                ]
                cols = [c for c in cols if c in df_out.columns]
                print(df_out[cols].to_string(index=False))

        except Exception as e:
            print(f"Error scanning {ticker}: {e}")

    write_snapshot(all_rows)
    print(f"\nSnapshot written: {len(all_rows)} rows\n")


if __name__ == "__main__":
    main()
