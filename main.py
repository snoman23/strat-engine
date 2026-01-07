# app.py

import pandas as pd
import streamlit as st

st.set_page_config(page_title="STRAT Scanner", layout="wide")

st.title("STRAT Scanner")
st.caption("Educational / informational purposes only. Not financial advice. Trading involves risk.")

with st.expander("Legend: Score & Alignment", expanded=False):
    st.markdown(
        """
**bias_dir** = higher-timeframe (Y/Q/M/W/D) directional bias  
**bias_score** = strength of that bias (0+). Same for all setups on the ticker.  

**aligned** tells you whether the setup direction matches the bias:
- **aligned** → setup direction matches bias (green)
- **counter** → setup direction opposes bias (red)
- **neutral** → bias neutral or setup directionless (gray)
        """
    )

@st.cache_data(ttl=60)
def load_results(path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()

df = load_results("cache/results/latest.csv")

if df.empty:
    st.warning("No scan results found yet. Wait for GitHub Actions to write latest.csv, then refresh.")
    st.stop()

# Keep only >= 1H timeframes
KEEP_TFS = ["Y", "Q", "M", "W", "D", "4H", "1H"]
if "tf" in df.columns:
    df = df[df["tf"].isin(KEEP_TFS)].copy()

# Remove generic patterns with no 1 or 3
if "pattern" in df.columns:
    df["pattern"] = df["pattern"].astype(str)
    df = df[df["pattern"].str.contains("1|3", regex=True)].copy()

# Scan time
last_scan = str(df["scan_time"].dropna().iloc[-1]) if "scan_time" in df.columns and df["scan_time"].dropna().any() else "unknown"
st.subheader("Latest Scan Results")
st.write(f"Last scan_time: **{last_scan}**")

# Sidebar filters (no Kind filter)
st.sidebar.header("Filters")

ticker_query = st.sidebar.text_input("Ticker contains", value="")
tf_sel = st.sidebar.multiselect(
    "Timeframe",
    sorted(df["tf"].dropna().unique().tolist()) if "tf" in df.columns else [],
    default=sorted(df["tf"].dropna().unique().tolist()) if "tf" in df.columns else [],
)
dir_sel = st.sidebar.multiselect("Setup direction", ["bull", "bear"], default=["bull", "bear"])
align_sel = st.sidebar.multiselect("Alignment", ["aligned", "counter", "neutral"], default=["aligned", "counter", "neutral"])
setup_query = st.sidebar.text_input("Setup contains", value="")
hide_triggered = st.sidebar.checkbox("Hide TRIGGERED", value=True)

# Apply filters
f = df.copy()

if ticker_query.strip():
    f = f[f["ticker"].astype(str).str.contains(ticker_query.strip().upper(), na=False)]

if tf_sel and "tf" in f.columns:
    f = f[f["tf"].isin(tf_sel)]

if dir_sel and "dir" in f.columns:
    f = f[f["dir"].isin(dir_sel)]

if align_sel and "aligned" in f.columns:
    f = f[f["aligned"].isin(align_sel)]

if setup_query.strip() and "setup" in f.columns:
    f = f[f["setup"].astype(str).str.contains(setup_query.strip(), case=False, na=False)]

if hide_triggered and "kind" in f.columns:
    f = f[f["kind"] != "TRIGGERED"]

# Round numeric columns
for col in ["current_price", "entry", "stop", "bias_score"]:
    if col in f.columns:
        f[col] = pd.to_numeric(f[col], errors="coerce").round(2)

# Clickable Yahoo chart link (stable markdown)
if "ticker" in f.columns:
    f["chart"] = f["ticker"].astype(str).apply(lambda t: f"[Chart](https://finance.yahoo.com/quote/{t}/chart)")

def style_by_alignment(row):
    a = row.get("aligned")
    if a == "aligned":
        return ["color: #0a7a0a" for _ in row]  # green
    if a == "counter":
        return ["color: #b00020" for _ in row]  # red
    return ["color: #666666" for _ in row]      # gray

cols = [
    "ticker", "current_price",
    "tf", "pattern", "setup", "dir",
    "bias_dir", "bias_score", "aligned",
    "entry", "stop", "actionable",
    "chart",
]
cols = [c for c in cols if c in f.columns]
f_out = f[cols].copy()

st.dataframe(
    f_out.style.apply(style_by_alignment, axis=1),
    use_container_width=True,
)

st.caption("Row color is based on alignment vs higher-timeframe bias (not on bull/bear label).")
