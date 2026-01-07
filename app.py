# app.py

import pandas as pd
import streamlit as st

st.set_page_config(page_title="STRAT Scanner", layout="wide")

st.title("STRAT Scanner")
st.caption("Educational / informational purposes only. Not financial advice. Trading involves risk.")


# -------------------------
# Legend / Score meaning
# -------------------------
with st.expander("Legend: What does the score mean?", expanded=False):
    st.markdown(
        """
**Score = Timeframe Continuity Alignment**

Your scanner generates a *bull* or *bear* trade plan (the setup direction).  
Then it checks higher-timeframe context (Y / Q / M / W / D) to see whether the plan is aligned.

- **Score > 0 (green)** → setup direction is **aligned** with higher-timeframe bias  
- **Score < 0 (red)** → setup direction is **counter-trend** vs higher-timeframe bias  
- **Score = 0 or blank (gray)** → **neutral / insufficient context**  

This score is **not** “expected profit” and **not** a guarantee. It’s just alignment.
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

# Ensure expected columns exist
expected = [
    "scan_time", "ticker", "tf", "kind", "pattern", "setup", "dir",
    "score", "current_price", "entry", "stop", "actionable",
]
for c in expected:
    if c not in df.columns:
        df[c] = None

# Normalize types
df["score"] = pd.to_numeric(df["score"], errors="coerce")
df["current_price"] = pd.to_numeric(df["current_price"], errors="coerce")
df["entry"] = pd.to_numeric(df["entry"], errors="coerce")
df["stop"] = pd.to_numeric(df["stop"], errors="coerce")

# Keep only >= 1H timeframes (per your request)
KEEP_TFS = ["Y", "Q", "M", "W", "D", "4H", "1H"]
df = df[df["tf"].isin(KEEP_TFS)].copy()

# Remove generic patterns that contain no 1 or 3 (your rule)
df["pattern"] = df["pattern"].astype(str)
df = df[df["pattern"].str.contains("1|3", regex=True)].copy()

# Scan time display
last_scan = str(df["scan_time"].dropna().iloc[-1]) if df["scan_time"].dropna().any() else "unknown"
st.subheader("Latest Scan Results")
st.write(f"Last scan_time: **{last_scan}**")

# Sidebar filters (NO Kind filter)
st.sidebar.header("Filters")

ticker_query = st.sidebar.text_input("Ticker contains", value="")
tf_sel = st.sidebar.multiselect(
    "Timeframe",
    sorted(df["tf"].dropna().unique().tolist()),
    default=sorted(df["tf"].dropna().unique().tolist()),
)
dir_sel = st.sidebar.multiselect("Direction", ["bull", "bear"], default=["bull", "bear"])
setup_query = st.sidebar.text_input("Setup contains", value="")
hide_triggered = st.sidebar.checkbox("Hide TRIGGERED", value=True)

score_min = float(df["score"].min()) if df["score"].notna().any() else -999.0
score_max = float(df["score"].max()) if df["score"].notna().any() else 999.0
score_range = st.sidebar.slider(
    "Score range",
    min_value=float(score_min),
    max_value=float(score_max),
    value=(float(score_min), float(score_max)),
)

# Apply filters
f = df.copy()

if ticker_query.strip():
    f = f[f["ticker"].astype(str).str.contains(ticker_query.strip().upper(), na=False)]

if tf_sel:
    f = f[f["tf"].isin(tf_sel)]

if dir_sel:
    f = f[f["dir"].isin(dir_sel)]

if setup_query.strip():
    f = f[f["setup"].astype(str).str.contains(setup_query.strip(), case=False, na=False)]

if hide_triggered:
    f = f[f["kind"] != "TRIGGERED"]

# Score filter (fill NaN with 0 for filtering)
f = f[(f["score"].fillna(0) >= score_range[0]) & (f["score"].fillna(0) <= score_range[1])]

# Alignment label
def aligned_label(score):
    if pd.isna(score) or score == 0:
        return "neutral"
    return "aligned" if score > 0 else "counter"

f["aligned"] = f["score"].apply(aligned_label)

# Round prices to 2 decimals
for col in ["current_price", "entry", "stop"]:
    f[col] = f[col].round(2)

# Make chart clickable: use Markdown link column (most stable across Streamlit versions)
# This avoids the LinkColumn callable display_text bug you hit earlier.
f["chart"] = f["ticker"].astype(str).apply(
    lambda t: f"[Chart](https://finance.yahoo.com/quote/{t}/chart)"
)

# Style rows by SCORE (not by direction)
def style_by_score(row):
    s = row.get("score")
    if pd.isna(s) or s == 0:
        return ["color: #666666" for _ in row]  # gray
    if s > 0:
        return ["color: #0a7a0a" for _ in row]  # green
    return ["color: #b00020" for _ in row]      # red

# Display columns
cols = [
    "ticker",
    "current_price",
    "tf",
    "pattern",
    "setup",
    "dir",
    "score",
    "aligned",
    "entry",
    "stop",
    "actionable",
    "chart",
]
cols = [c for c in cols if c in f.columns]
f_out = f[cols].copy()

# Render: allow clickable markdown
st.dataframe(
    f_out.style.apply(style_by_score, axis=1),
    use_container_width=True,
)

st.markdown(
    "<style> a { text-decoration: none; } </style>",
    unsafe_allow_html=True,
)

st.caption("Color meaning: green = aligned with higher-timeframe continuity, red = counter-trend, gray = neutral/unknown.")
