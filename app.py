# app.py

import os
import time
import pandas as pd
import streamlit as st

st.set_page_config(page_title="STRAT Scanner", layout="wide")

st.title("STRAT Scanner")
st.caption("Loads latest scan results from cache/results/latest.csv")

CSV_PATH = os.path.join("cache", "results", "latest.csv")


@st.cache_data(ttl=30)
def load_data(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def file_updated_ago_seconds(path: str) -> int | None:
    try:
        return int(time.time() - os.path.getmtime(path))
    except Exception:
        return None


df = load_data(CSV_PATH)

if df.empty:
    st.warning("No scan results found yet. If you just deployed, reboot the app once from Streamlit Cloud.")
    st.stop()

# numeric conversion
num_cols = ["entry", "stop", "current_price", "score", "prev_high", "prev_low", "last_high", "last_low"]
for col in num_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

secs = file_updated_ago_seconds(CSV_PATH)
if secs is not None:
    st.caption(f"Data file updated ~{secs}s ago.")

# Sidebar filters
st.sidebar.header("Filters")

ticker_search = st.sidebar.text_input("Ticker search", "").strip().upper()

tfs = sorted([x for x in df["tf"].dropna().unique().tolist()])
selected_tfs = st.sidebar.multiselect("Timeframes", tfs, default=tfs)

# keep filtering on setup (plan name)
setups = sorted([x for x in df["setup"].dropna().unique().tolist()]) if "setup" in df.columns else []
selected_setups = st.sidebar.multiselect("Setup (plan)", setups, default=setups) if setups else []

# optional filter on pattern
patterns = sorted([x for x in df["pattern"].dropna().unique().tolist()]) if "pattern" in df.columns else []
selected_patterns = st.sidebar.multiselect("Pattern (last 2 candles)", patterns, default=patterns) if patterns else []

dirs = sorted([x for x in df["dir"].dropna().unique().tolist()])
selected_dirs = st.sidebar.multiselect("Direction", dirs, default=dirs)

min_score = None
max_score = None
if "score" in df.columns and df["score"].notna().any():
    smin = int(df["score"].min())
    smax = int(df["score"].max())
    min_score, max_score = st.sidebar.slider("Score range", smin, smax, (smin, smax))

# Apply filters
filtered = df.copy()

if ticker_search:
    filtered = filtered[filtered["ticker"].astype(str).str.contains(ticker_search, na=False)]
if selected_tfs:
    filtered = filtered[filtered["tf"].isin(selected_tfs)]
if selected_setups:
    filtered = filtered[filtered["setup"].isin(selected_setups)]
if selected_patterns:
    filtered = filtered[filtered["pattern"].isin(selected_patterns)]
if selected_dirs:
    filtered = filtered[filtered["dir"].isin(selected_dirs)]
if min_score is not None and "score" in filtered.columns:
    filtered = filtered[(filtered["score"].fillna(0) >= min_score) & (filtered["score"].fillna(0) <= max_score)]

# Header metrics
c1, c2, c3 = st.columns(3)
c1.metric("Rows", len(filtered))
if "scan_time" in df.columns and df["scan_time"].notna().any():
    c2.metric("Last scan_time", str(df["scan_time"].dropna().iloc[-1]))
c3.metric("Tickers in file", df["ticker"].nunique())

# Optional ticker focus
tickers = sorted(filtered["ticker"].dropna().unique().tolist())
selected_ticker = st.selectbox("Focus ticker (optional)", ["(All)"] + tickers)

if selected_ticker != "(All)":
    filtered = filtered[filtered["ticker"] == selected_ticker]
    px = None
    if "current_price" in filtered.columns and filtered["current_price"].dropna().any():
        px = float(filtered["current_price"].dropna().iloc[0])
    st.subheader(f"{selected_ticker}  (Current: {px:.2f})" if px is not None else selected_ticker)

# Display formatting: 2 decimals
display = filtered.copy()
for col in ["current_price", "entry", "stop", "prev_high", "prev_low", "last_high", "last_low"]:
    if col in display.columns:
        display[col] = display[col].map(lambda x: f"{x:.2f}" if pd.notna(x) else "")

preferred_cols = [
    "ticker", "current_price",
    "tf",
    "pattern",   # NEW: last 2 candles
    "setup",     # NEW: plan name (what we trade next)
    "dir", "score",
    "actionable",
    "entry", "stop",
    "prev_ts", "prev_strat", "prev_high", "prev_low",
    "last_ts", "last_strat", "last_high", "last_low",
]
cols = [c for c in preferred_cols if c in display.columns]

st.dataframe(
    display[cols].sort_values(["ticker", "tf", "setup", "dir"], ascending=True),
    width="stretch",
    height=650,
)

st.download_button(
    "Download filtered CSV",
    data=filtered.to_csv(index=False).encode("utf-8"),
    file_name="strat_scanner_filtered.csv",
    mime="text/csv",
)

st.divider()

st.markdown(
    """
### Disclaimer
This scanner is provided **for informational and educational purposes only** and does not constitute
financial, investment, trading, tax, or legal advice. Signals, setups, and scoring are generated from
historical market data and may be inaccurate, delayed, incomplete, or change due to data vendor behavior.

You are solely responsible for your own trading decisions and risk management. Trading and investing involve
substantial risk, and you may lose some or all of your investment. Past performance is not indicative of
future results. Consult a qualified financial professional before making any investment decisions.
"""
)
