# app.py

import os
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


df = load_data(CSV_PATH)

if df.empty:
    st.warning("No scan results found yet. Run:  python main.py  then refresh this page.")
    st.stop()

# Ensure numeric types
num_cols = ["entry", "stop", "current_price", "score", "prev_high", "prev_low", "last_high", "last_low"]
for col in num_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# Sidebar filters
st.sidebar.header("Filters")

ticker_search = st.sidebar.text_input("Ticker search", "").strip().upper()

tfs = sorted([x for x in df["tf"].dropna().unique().tolist()])
selected_tfs = st.sidebar.multiselect("Timeframes", tfs, default=tfs)

setups = sorted([x for x in df["setup"].dropna().unique().tolist()])
selected_setups = st.sidebar.multiselect("Setups", setups, default=setups)

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
    filtered = filtered[filtered["ticker"].str.contains(ticker_search, na=False)]
if selected_tfs:
    filtered = filtered[filtered["tf"].isin(selected_tfs)]
if selected_setups:
    filtered = filtered[filtered["setup"].isin(selected_setups)]
if selected_dirs:
    filtered = filtered[filtered["dir"].isin(selected_dirs)]
if min_score is not None:
    filtered = filtered[(filtered["score"].fillna(0) >= min_score) & (filtered["score"].fillna(0) <= max_score)]

# Header metrics
c1, c2, c3 = st.columns(3)
c1.metric("Rows", len(filtered))
if "scan_time" in df.columns and df["scan_time"].notna().any():
    c2.metric("Last scan_time", str(df["scan_time"].dropna().iloc[-1]))
c3.metric("Tickers in file", df["ticker"].nunique())

# Optional: ticker dropdown for easier viewing
tickers = sorted(filtered["ticker"].dropna().unique().tolist())
selected_ticker = st.selectbox("View a single ticker (optional)", ["(All)"] + tickers)

if selected_ticker != "(All)":
    filtered = filtered[filtered["ticker"] == selected_ticker]
    px = filtered["current_price"].dropna().iloc[0] if "current_price" in filtered.columns and filtered["current_price"].dropna().any() else None
    if px is not None:
        st.subheader(f"{selected_ticker}  (Current: {px:.2f})")
    else:
        st.subheader(selected_ticker)

# Round display to 2 decimals (without changing stored data)
display = filtered.copy()
for col in ["current_price", "entry", "stop", "prev_high", "prev_low", "last_high", "last_low"]:
    if col in display.columns:
        display[col] = display[col].map(lambda x: f"{x:.2f}" if pd.notna(x) else "")

# Columns to show
preferred_cols = [
    "ticker", "current_price",
    "tf", "setup", "dir", "score",
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

# Disclaimer
st.markdown(
    """
### Disclaimer
This scanner is provided **for informational and educational purposes only** and does not constitute
financial, investment, trading, or legal advice. Signals, setups, and scoring are generated from historical
market data and may be inaccurate, delayed, incomplete, or change due to data vendor behavior.

You are solely responsible for your own trading decisions and risk management. Trading and investing involve
substantial risk, and you may lose more than your initial investment. Past performance is not indicative of
future results. Consult a qualified financial professional before making any investment decisions.
"""
)
