# app.py
import pandas as pd
import streamlit as st

RESULTS_PATH = "cache/results/latest.csv"

st.set_page_config(page_title="STRAT Scanner", page_icon="📈", layout="wide")

st.title("STRAT Scanner")
st.caption("Educational / informational purposes only. Not financial advice. Trading involves risk.")


@st.cache_data(ttl=60)
def load_results() -> pd.DataFrame:
    try:
        return pd.read_csv(RESULTS_PATH)
    except Exception:
        return pd.DataFrame()


df = load_results()
if df.empty:
    st.error("No scan results found yet. The workflow hasn't written cache/results/latest.csv yet.")
    st.stop()

# Ensure columns exist
for col in [
    "scan_time", "ticker", "chart_url", "current_price",
    "tf", "pattern", "setup", "dir",
    "entry", "stop", "score", "aligned",
    "last_strat", "last_candle_type",
    "actionable"
]:
    if col not in df.columns:
        df[col] = None

# Normalize types
df["ticker"] = df["ticker"].astype(str).fillna("")
df["tf"] = df["tf"].astype(str).fillna("")
df["dir"] = df["dir"].astype(str).fillna("")
df["score"] = pd.to_numeric(df["score"], errors="coerce").fillna(0).astype(int)
for c in ["current_price", "entry", "stop"]:
    df[c] = pd.to_numeric(df[c], errors="coerce").round(2)

# Last scan time
last_scan = str(df["scan_time"].dropna().iloc[0]) if df["scan_time"].notna().any() else "Unknown"
st.markdown(f"**Last scan_time (ET):** `{last_scan}`")

# Sidebar filters
st.sidebar.header("Filters")
ticker_search = st.sidebar.text_input("Ticker contains", value="").strip().upper()

tf_options = sorted([x for x in df["tf"].dropna().unique().tolist() if x])
tf_selected = st.sidebar.multiselect("Timeframe", options=tf_options, default=tf_options)

dir_options = [x for x in ["bull", "bear"] if x in df["dir"].dropna().unique().tolist()]
dir_selected = st.sidebar.multiselect("Direction", options=dir_options, default=dir_options)

setup_search = st.sidebar.text_input("Setup contains", value="").strip()
only_aligned = st.sidebar.checkbox("Only aligned with bias", value=False)

# Apply filters
f = df.copy()

if ticker_search:
    f = f[f["ticker"].str.upper().str.contains(ticker_search, na=False)]

if tf_selected:
    f = f[f["tf"].isin(tf_selected)]

if dir_selected:
    f = f[f["dir"].isin(dir_selected)]

if setup_search:
    f = f[f["setup"].astype(str).str.contains(setup_search, case=False, na=False)]

if only_aligned:
    f = f[f["aligned"] == True]

# Sort: strongest bias first, then ticker
f["_abs_score"] = f["score"].abs()
f = f.sort_values(by=["_abs_score", "ticker", "tf"], ascending=[False, True, True]).drop(columns=["_abs_score"])

# Legend
with st.expander("Score legend (what it means)"):
    st.markdown(
        """
**Score = Market bias only (from last closed candles on higher TFs).**

- Positive score → HTF bias bullish  
- Negative score → HTF bias bearish  
- Near 0 → mixed / neutral  

**Last Candle Type** is a shape-based heuristic (hammer/doji/etc).  
It does **not** know trend context, so “hammer vs hanging man” is labeled as “hammer-like/hanging-man-like”.
        """
    )

st.subheader("Grouped Results (Ticker → TF)")

# Group by ticker
tickers = f["ticker"].dropna().unique().tolist()

for t in tickers:
    ft = f[f["ticker"] == t].copy()
    if ft.empty:
        continue

    price = ft["current_price"].dropna().iloc[0] if ft["current_price"].notna().any() else None
    score = int(ft["score"].dropna().iloc[0]) if ft["score"].notna().any() else 0
    chart = ft["chart_url"].dropna().iloc[0] if ft["chart_url"].notna().any() else f"https://finance.yahoo.com/quote/{t}/chart"

    header = f"{t} | price={price} | score={score}"
    with st.expander(header, expanded=False):
        st.markdown(f"[Open Yahoo Chart]({chart})")

        # group within ticker by TF
        for tf in ["Y","Q","M","W","D","4H","3H","2H","1H"]:
            ftt = ft[ft["tf"] == tf].copy()
            if ftt.empty:
                continue

            st.markdown(f"### {tf}")

            cols = [
                "pattern", "setup", "dir",
                "last_strat", "last_candle_type",
                "entry", "stop",
                "aligned",
                "actionable",
            ]
            cols = [c for c in cols if c in ftt.columns]
            st.dataframe(ftt[cols], use_container_width=True, hide_index=True)
