# app.py
import pandas as pd
import streamlit as st

RESULTS_PATH = "cache/results/latest.csv"

st.set_page_config(
    page_title="STRAT Scanner",
    page_icon="📈",
    layout="wide",
)

# --- Styling (simple but nicer) ---
st.markdown(
    """
    <style>
      .small-note { color: #6b7280; font-size: 0.9rem; }
      .pill { padding: 2px 10px; border-radius: 999px; font-size: 0.85rem; display: inline-block; }
      .pill-bull { background: rgba(34,197,94,0.15); color: rgb(34,197,94); border: 1px solid rgba(34,197,94,0.35); }
      .pill-bear { background: rgba(239,68,68,0.15); color: rgb(239,68,68); border: 1px solid rgba(239,68,68,0.35); }
      table { width: 100%; }
      thead th { position: sticky; top: 0; background: #0e1117; z-index: 1; }
      .score-pos { color: rgb(34,197,94); font-weight: 700; }
      .score-neg { color: rgb(239,68,68); font-weight: 700; }
      .score-zero { color: #9ca3af; font-weight: 700; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("STRAT Scanner")
st.caption("Educational / informational purposes only. Not financial advice. Trading involves risk.")

@st.cache_data(ttl=60)
def load_results() -> pd.DataFrame:
    try:
        df = pd.read_csv(RESULTS_PATH)
        return df
    except Exception:
        return pd.DataFrame()


df = load_results()

if df.empty:
    st.error("No scan results found yet. The scheduled scanner hasn't written cache/results/latest.csv.")
    st.stop()

# Ensure expected columns exist
for col in ["scan_time", "ticker", "chart_url", "current_price", "tf", "pattern", "setup", "dir", "entry", "stop", "score"]:
    if col not in df.columns:
        df[col] = None

# Show last scan time (already ET string from main.py)
last_scan = df["scan_time"].dropna().iloc[0] if df["scan_time"].notna().any() else "Unknown"
st.markdown(f"**Last scan_time:** `{last_scan}`")

# --- Sidebar filters ---
st.sidebar.header("Filters")

ticker_search = st.sidebar.text_input("Ticker contains", value="").strip().upper()

tf_options = sorted([x for x in df["tf"].dropna().unique().tolist()])
tf_selected = st.sidebar.multiselect("Timeframe", options=tf_options, default=tf_options)

dir_options = [x for x in ["bull", "bear"] if x in df["dir"].dropna().unique().tolist()]
dir_selected = st.sidebar.multiselect("Direction", options=dir_options, default=dir_options)

setup_search = st.sidebar.text_input("Setup contains", value="").strip()

# Score filter
score_min = int(df["score"].fillna(0).min())
score_max = int(df["score"].fillna(0).max())
score_range = st.sidebar.slider("Score range", min_value=score_min, max_value=score_max, value=(score_min, score_max))

only_aligned = st.sidebar.checkbox("Only aligned with bias", value=False)

# Apply filters
f = df.copy()

if ticker_search:
    f = f[f["ticker"].astype(str).str.upper().str.contains(ticker_search, na=False)]

if tf_selected:
    f = f[f["tf"].isin(tf_selected)]

if dir_selected:
    f = f[f["dir"].isin(dir_selected)]

if setup_search:
    f = f[f["setup"].astype(str).str.contains(setup_search, case=False, na=False)]

f["score"] = pd.to_numeric(f["score"], errors="coerce").fillna(0).astype(int)
f = f[(f["score"] >= score_range[0]) & (f["score"] <= score_range[1])]

if only_aligned and "aligned" in f.columns:
    f = f[f["aligned"] == True]

# Round numbers for display
for col in ["current_price", "entry", "stop"]:
    f[col] = pd.to_numeric(f[col], errors="coerce").round(2)

# --- Score legend ---
with st.expander("Score legend (what it means)"):
    st.markdown(
        """
**Score = Market bias only (from last closed candles on higher TFs), not the setup direction.**

Weights:
- **Y=5, Q=4, M=3, W=2, D=1**

Scoring rule:
- If TF last closed candle is **2U**, add weight.
- If TF last closed candle is **2D**, subtract weight.
- If TF last closed candle is **1 or 3**, add **0**.

Interpretation:
- **Positive score** → higher timeframes are **bull-biased**
- **Negative score** → higher timeframes are **bear-biased**
- **Near zero** → **mixed / neutral**

The **Direction (bull/bear)** is your planned trade direction for the setup.  
Use **“Only aligned with bias”** if you want signals that match the bias.
        """
    )

# --- Build a nicer table with clickable tickers ---
def pill_dir(d: str) -> str:
    if d == "bull":
        return '<span class="pill pill-bull">bull</span>'
    if d == "bear":
        return '<span class="pill pill-bear">bear</span>'
    return str(d)

def fmt_score(s: int) -> str:
    if s > 0:
        return f'<span class="score-pos">{s}</span>'
    if s < 0:
        return f'<span class="score-neg">{s}</span>'
    return f'<span class="score-zero">{s}</span>'

# Create display dataframe
show = f.copy()

# Clickable ticker name -> Yahoo chart
show["Ticker"] = show.apply(
    lambda r: f'<a href="{r["chart_url"]}" target="_blank" rel="noopener noreferrer">{r["ticker"]}</a>',
    axis=1,
)

show["Dir"] = show["dir"].astype(str).apply(pill_dir)
show["Score"] = show["score"].astype(int).apply(fmt_score)

# Keep columns you care about
display_cols = [
    "Ticker", "current_price", "tf", "pattern", "setup", "Dir",
    "entry", "stop", "Score", "aligned", "actionable",
]

for c in display_cols:
    if c not in show.columns:
        show[c] = None

show = show[display_cols].rename(
    columns={
        "current_price": "Price",
        "tf": "TF",
        "pattern": "Pattern",
        "setup": "Setup to Trade",
        "entry": "Entry",
        "stop": "Stop",
        "aligned": "Aligned?",
        "actionable": "Plan",
    }
)

# Sort: highest absolute bias first, then TF
show["_abs_score"] = pd.to_numeric(f["score"], errors="coerce").fillna(0).abs().values
show = show.sort_values(by=["_abs_score", "TF"], ascending=[False, True]).drop(columns=["_abs_score"])

st.subheader("Latest Scan Results")

st.markdown(
    show.to_html(escape=False, index=False),
    unsafe_allow_html=True,
)
