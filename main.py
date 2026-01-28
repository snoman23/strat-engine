# main.py

import sys
import os
import pandas as pd
from zoneinfo import ZoneInfo

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config import (
    DEV_MODE,
    DEV_TICKERS_LIMIT,
    MIN_MARKET_CAP,
    DEV_YF_BASE_FEEDS,
    YF_BASE_FEEDS,
    SECTOR_TOP_ETFS,
)
from snapshot import write_snapshot

from universe.loader import load_universe
from loaders.yahoo import load_ohlc
from timeframes.resample import resample_timeframe
from strat.classify import classify_strat_candles
from strat_signals import analyze_last_closed_setups, last_closed_index

TARGET_TFS = ["Y", "Q", "M", "W", "D", "4H", "3H", "2H", "1H"]

DIRECT = {"D": ("1d", None), "1H": ("60m", None)}

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

SECTOR_MAP_PATH = os.path.join("cache", "universe", "sector_map.csv")
ETF_HOLDINGS_PATH = os.path.join("cache", "universe", "core_etf_holdings.csv")
STOCKS_BIGGEST_PATH = os.path.join("cache", "universe", "stocks_biggest.csv")

CONTEXT_OUT_PATH = os.path.join("cache", "results", "context.csv")


def _norm_ticker(x: str) -> str:
    s = str(x).strip().upper()
    if s.startswith("$"):
        s = s[1:]
    s = s.replace(".", "-")
    out = []
    for ch in s:
        if ("A" <= ch <= "Z") or ("0" <= ch <= "9") or ch == "-":
            out.append(ch)
    return "".join(out)


def _infer_resolution_seconds(df: pd.DataFrame) -> int:
    if df is None or df.empty or "timestamp" not in df.columns or len(df) < 3:
        return 0
    ts = pd.to_datetime(df["timestamp"], errors="coerce").dropna().sort_values()
    if len(ts) < 3:
        return 0
    diffs = ts.diff().dropna().dt.total_seconds()
    if diffs.empty:
        return 0
    return int(diffs.median())


def _is_ok_base_for_60m(df_60: pd.DataFrame) -> bool:
    res = _infer_resolution_seconds(df_60)
    return res != 0 and res <= 3600 * 2


def _is_ok_base_for_1d(df_d: pd.DataFrame) -> bool:
    res = _infer_resolution_seconds(df_d)
    return res != 0 and res >= 3600 * 12


def build_timeframe_frames(ticker: str):
    feeds_cfg = DEV_YF_BASE_FEEDS if DEV_MODE else YF_BASE_FEEDS

    feeds: dict[str, pd.DataFrame] = {}
    for interval, cfg in feeds_cfg.items():
        df = load_ohlc(ticker, interval=interval, period=cfg["period"])
        feeds[interval] = (
            df.sort_values("timestamp").reset_index(drop=True)
            if df is not None and not df.empty
            else pd.DataFrame()
        )

    frames: dict[str, pd.DataFrame] = {}

    for tf, (base_interval, _) in DIRECT.items():
        base = feeds.get(base_interval, pd.DataFrame())
        if base is not None and not base.empty:
            frames[tf] = base

    base_60 = feeds.get("60m", pd.DataFrame())
    base_1d = feeds.get("1d", pd.DataFrame())

    ok_60 = base_60 is not None and not base_60.empty and _is_ok_base_for_60m(base_60)
    ok_1d = base_1d is not None and not base_1d.empty and _is_ok_base_for_1d(base_1d)

    for tf, (base_interval, derived_tf) in DERIVED.items():
        if base_interval == "60m":
            if not ok_60:
                continue
            frames[tf] = resample_timeframe(base_60, derived_tf)
        elif base_interval == "1d":
            if not ok_1d:
                continue
            frames[tf] = resample_timeframe(base_1d, derived_tf)

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


def compute_bias_score(context_closed: dict) -> int:
    """
    Confirmed bias only: uses CLOSED bars (Y/Q/M/W/D).
    """
    score = 0
    for tf, strat in context_closed.items():
        w = WEIGHTS.get(tf, 0)
        if strat == "2U":
            score += w
        elif strat == "2D":
            score -= w
    return score


def candlestick_name(o: float, h: float, l: float, c: float) -> str:
    try:
        o = float(o); h = float(h); l = float(l); c = float(c)
    except Exception:
        return "Unknown"

    rng = max(h - l, 1e-9)
    body = abs(c - o)
    upper = h - max(o, c)
    lower = min(o, c) - l

    bull = c > o
    bear = c < o

    if body <= 0.10 * rng:
        if lower >= 0.60 * rng and upper <= 0.15 * rng:
            return "Dragonfly Doji (bullish-leaning)"
        if upper >= 0.60 * rng and lower <= 0.15 * rng:
            return "Gravestone Doji (bearish-leaning)"
        return "Doji"

    if lower >= 2.0 * body and upper <= 0.35 * body:
        return "Bullish Hammer-like" if bull else "Bearish Hanging-Man-like"

    if upper >= 2.0 * body and lower <= 0.35 * body:
        return "Bullish Inverted Hammer-like" if bull else "Bearish Shooting-Star-like"

    if upper <= 0.10 * rng and lower <= 0.10 * rng:
        return "Bullish Marubozu-like" if bull else "Bearish Marubozu-like"

    if bull:
        return "Bullish Candle"
    if bear:
        return "Bearish Candle"
    return "Neutral Candle"


def _load_sector_map() -> pd.DataFrame:
    if not os.path.exists(SECTOR_MAP_PATH):
        return pd.DataFrame(columns=["ticker", "sector"])
    df = pd.read_csv(SECTOR_MAP_PATH)
    if "ticker" not in df.columns or "sector" not in df.columns:
        return pd.DataFrame(columns=["ticker", "sector"])
    df = df.copy()
    df["ticker"] = df["ticker"].astype(str).map(_norm_ticker)
    df["sector"] = df["sector"].astype(str).fillna("Unknown")
    return df[["ticker", "sector"]]


def _load_etf_membership() -> pd.DataFrame:
    if not os.path.exists(ETF_HOLDINGS_PATH):
        return pd.DataFrame(columns=["ticker", "etfs", "etf_count", "etfs_pretty"])
    df = pd.read_csv(ETF_HOLDINGS_PATH)
    if "ticker" not in df.columns:
        return pd.DataFrame(columns=["ticker", "etfs", "etf_count", "etfs_pretty"])
    df = df.copy()
    df["ticker"] = df["ticker"].astype(str).map(_norm_ticker)
    df["etfs"] = df.get("etfs", "").astype(str).fillna("")
    if "etf_count" not in df.columns:
        df["etf_count"] = df["etfs"].apply(lambda x: len([e for e in str(x).split("|") if e]))
    df["etf_count"] = pd.to_numeric(df["etf_count"], errors="coerce").fillna(0).astype(int)
    df["etfs_pretty"] = df["etfs"].apply(lambda x: ", ".join([e for e in str(x).split("|") if e]))
    return df[["ticker", "etfs", "etf_count", "etfs_pretty"]]


def _load_industry_map() -> pd.DataFrame:
    if not os.path.exists(STOCKS_BIGGEST_PATH):
        return pd.DataFrame(columns=["ticker", "industry"])
    df = pd.read_csv(STOCKS_BIGGEST_PATH)
    cols = {c.lower().strip(): c for c in df.columns}
    sym_col = cols.get("symbol")
    ind_col = cols.get("industry")
    if sym_col is None or ind_col is None:
        return pd.DataFrame(columns=["ticker", "industry"])
    out = df[[sym_col, ind_col]].copy()
    out.columns = ["ticker", "industry"]
    out["ticker"] = out["ticker"].astype(str).map(_norm_ticker)
    out["industry"] = out["industry"].astype(str).fillna("Unknown")
    return out


def _sector_etf_lookup() -> dict[str, str]:
    rev = {}
    for sec, etfs in SECTOR_TOP_ETFS.items():
        for e in etfs:
            rev[_norm_ticker(e)] = sec
    return rev


def enrich_df_with_metadata(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or "ticker" not in df.columns:
        return df

    df = df.copy()
    df["ticker"] = df["ticker"].astype(str).map(_norm_ticker)

    sector_df = _load_sector_map()
    etf_df = _load_etf_membership()
    ind_df = _load_industry_map()
    sec_etf = _sector_etf_lookup()

    if not sector_df.empty:
        df = df.merge(sector_df, on="ticker", how="left")
    else:
        df["sector"] = "Unknown"
    df["sector"] = df["sector"].fillna("Unknown")

    if not ind_df.empty:
        df = df.merge(ind_df, on="ticker", how="left")
    else:
        df["industry"] = "Unknown"
    df["industry"] = df["industry"].fillna("Unknown")

    # If ticker is one of the sector ETFs (XLB, XLK, etc), enforce sector + industry tag
    df["sector"] = df.apply(lambda r: sec_etf.get(r["ticker"], r["sector"]), axis=1)
    df["industry"] = df.apply(lambda r: ("Sector ETF" if r["ticker"] in sec_etf else r["industry"]), axis=1)

    if not etf_df.empty:
        df = df.merge(etf_df, on="ticker", how="left")
    else:
        df["etfs"] = ""
        df["etf_count"] = 0
        df["etfs_pretty"] = ""

    for c in ["etfs", "etfs_pretty"]:
        if c not in df.columns:
            df[c] = ""
        df[c] = df[c].fillna("")
    if "etf_count" not in df.columns:
        df["etf_count"] = 0
    df["etf_count"] = pd.to_numeric(df["etf_count"], errors="coerce").fillna(0).astype(int)

    return df


def _write_context_csv(context_rows: list[dict]) -> None:
    os.makedirs(os.path.dirname(CONTEXT_OUT_PATH), exist_ok=True)
    pd.DataFrame(context_rows).to_csv(CONTEXT_OUT_PATH, index=False)


def scan_ticker(ticker: str, scan_time: str):
    frames, feeds = build_timeframe_frames(ticker)
    if "D" not in frames or frames["D"].empty or len(frames["D"]) < 50:
        return [], None

    current_price = get_current_price(feeds)
    px = round(float(current_price), 2) if current_price is not None else None

    classified: dict[str, pd.DataFrame] = {}
    for tf, df in frames.items():
        if df is None or df.empty or len(df) < 3:
            continue
        df2 = classify_strat_candles(df)
        df2 = df2.sort_values("timestamp").reset_index(drop=True)
        classified[tf] = df2

    # Confirmed (CLOSED) context for setups/bias score
    ctx_closed = {}
    for tf in ("Y", "Q", "M", "W", "D"):
        df_tf = classified.get(tf)
        if df_tf is None or df_tf.empty or len(df_tf) < 3:
            continue
        idx = last_closed_index(tf, df_tf)
        ctx_closed[tf] = str(df_tf.iloc[idx]["strat"])

    # Live (CURRENT BAR) context for heatmap
    ctx_live = {}
    for tf in ("Y", "Q", "M", "W", "D"):
        df_tf = classified.get(tf)
        if df_tf is None or df_tf.empty or len(df_tf) < 2:
            continue
        # current bar strat (can repaint)
        v = df_tf.iloc[-1]["strat"]
        ctx_live[tf] = str(v) if pd.notna(v) else None

    bias_score = compute_bias_score(ctx_closed)

    # Context row (for heatmap)
    context_row = {
        "scan_time": scan_time,
        "ticker": ticker,
        "current_price": px,

        "ctx_Y_closed": ctx_closed.get("Y"),
        "ctx_Q_closed": ctx_closed.get("Q"),
        "ctx_M_closed": ctx_closed.get("M"),
        "ctx_W_closed": ctx_closed.get("W"),
        "ctx_D_closed": ctx_closed.get("D"),

        "ctx_Y_live": ctx_live.get("Y"),
        "ctx_Q_live": ctx_live.get("Q"),
        "ctx_M_live": ctx_live.get("M"),
        "ctx_W_live": ctx_live.get("W"),
        "ctx_D_live": ctx_live.get("D"),

        "score": int(bias_score),
    }

    rows = []
    for tf in TARGET_TFS:
        df_tf = classified.get(tf)
        if df_tf is None or df_tf.empty or len(df_tf) < 3:
            continue

        signals = analyze_last_closed_setups(df_tf, tf)
        if not signals:
            continue

        for sig in signals:
            # Setups must remain LAST CLOSED (no repaint)
            if getattr(sig, "kind", "") == "TRIGGERED":
                continue

            chart_url = f"https://finance.yahoo.com/quote/{ticker}/chart"

            aligned = None
            if sig.direction in ("bull", "bear"):
                aligned = (bias_score > 0) if sig.direction == "bull" else (bias_score < 0)

            last_candle_type = candlestick_name(sig.last_open, sig.last_high, sig.last_low, sig.last_close)

            rows.append(
                {
                    "scan_time": scan_time,
                    "ticker": ticker,
                    "chart_url": chart_url,
                    "current_price": px,

                    "tf": sig.tf,
                    "pattern": sig.pattern,
                    "setup": sig.setup,
                    "dir": sig.direction,

                    "entry": round(float(sig.entry), 2) if sig.entry is not None else None,
                    "stop": round(float(sig.stop), 2) if sig.stop is not None else None,

                    "score": int(bias_score),
                    "aligned": aligned,

                    "last_strat": sig.last_strat,
                    "last_candle_type": last_candle_type,

                    "actionable": sig.actionable,
                    "note": sig.note,

                    "ctx_Y": ctx_closed.get("Y"),
                    "ctx_Q": ctx_closed.get("Q"),
                    "ctx_M": ctx_closed.get("M"),
                    "ctx_W": ctx_closed.get("W"),
                    "ctx_D": ctx_closed.get("D"),
                }
            )

    return rows, context_row


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
    context_rows = []

    for ticker in tickers:
        try:
            rows, ctx = scan_ticker(ticker, scan_time)
            all_rows.extend(rows)
            if ctx:
                context_rows.append(ctx)
        except Exception as e:
            print(f"Error scanning {ticker}: {e}")

    # Enrich with sector/industry/etf membership for UI filtering + heatmaps
    df_rows = enrich_df_with_metadata(pd.DataFrame(all_rows)) if all_rows else pd.DataFrame()
    df_ctx = enrich_df_with_metadata(pd.DataFrame(context_rows)) if context_rows else pd.DataFrame()

    all_rows_out = df_rows.to_dict(orient="records") if not df_rows.empty else []
    ctx_out = df_ctx.to_dict(orient="records") if not df_ctx.empty else []

    write_snapshot(all_rows_out)
    _write_context_csv(ctx_out)

    print(f"\nSnapshot written: {len(all_rows_out)} rows")
    print(f"Context written: {len(ctx_out)} tickers -> {CONTEXT_OUT_PATH}\n")


if __name__ == "__main__":
    main()
