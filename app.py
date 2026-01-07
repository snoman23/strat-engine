# app.py
import pandas as pd
import streamlit as st

RESULTS_PATH = "cache/results/latest.csv"

st.set_page_config(
    page_title="STRAT Scanner",
    page_icon="📈",
    layout="wide",
)

# -----------------------------
# Styling
# -----------------------------
st.markdown(
    """
    <style>
      .small-note { color: #9ca3af; font-size: 0.9rem; }
      .header-card {
        padding: 14px 16px;
        border-radius: 14px;
        border: 1px solid rgba(255,255,255,0.08);
        background: rgba(255,255,255,0.03);
        margin-bottom: 10px;
      }
      .pill { padding: 2px 10px; border-radius: 999px; font-size: 0.85rem; display: inline-block; }
      .pill-bull { background: rgba(34,197,94,0.15); color: rgb(34,197,94); border: 1px solid rgba(34,197,94,0.35); }
      .pill-bear { background: rgba(239,68,68,0.15); color: rgb(239,68,68); border: 1px solid rgba(239,68,68,0.35); }
      .score-pos { color: rgb(34,197,94); font-weight: 800; }
      .score-neg { color: rgb(239,68,68); font-weight: 800; }
      .score-zero { color: #9ca3af; font-weight: 800; }
      table { width: 100%; }
      thead th { position: sticky; top: 0; background: #0e1117; z-index: 1; }
      a { text-decoration: none; }
      a:hover { text-decoration: underline; }
      .muted { color: #9ca3af; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("STRAT Scanner")
st.caption("Educational / informational purposes only. Not financial advice. Trading involves risk.")

# -----------------------------
# Data loader
# -----------------------------
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

# Ensure expected columns exist (safe)
expected = [
    "scan_time", "ticker", "chart_url", "current_price",
    "tf", "pattern", "setup", "dir",
    "entry", "stop", "score", "aligned", "actionable"
]
for col in expected:
    if col not in df.columns:
        df[col] = None

# Normalize types
df["ticker"] = df["ticker"].astype(str).fillna("")
df["tf"] = df["tf"].astype(str).fillna("")
df["dir"] = df["dir"].astype(str).fillna("")
df["pattern"] = df["pattern"].astype(str).fillna("")
df["setup"] = df["setup"].astype(str).fillna("")
df["actionable"] = df["actionable"].astype(str).fillna("")
df["chart_url"] = df["chart_url"].astype(str).fillna("")

df["score"] = pd.to_numeric(df["score"], errors="coerce").fillna(0).astype(int)
for c in ["current_price", "entry", "stop"]:
    df[c] = pd.to_numeric(df[c], errors="coerce")

# Last scan time (already ET string from main.py)
last_scan = "Unknown"
try:
    if df["scan_time"].notna().any():
        last_scan = str(df["scan_time"].dropna().iloc[0])
except Exception:
    pass

# Quick stats
total_rows = len(df)
unique_tickers = df["ticker"].nunique()

# Header card
st.markdown(
    f"""
    <div class="header-card">
      <div><b>Latest Scan Results</b></div>
      <div class="small-note">Last scan_time (ET): <code>{last_scan}</code> · Rows: <b>{total_rows}</b> · Tickers: <b>{unique_tickers}</b></div>
    </div>
    """,
    unsafe_allow_html=True,
)

# -----------------------------
# Sidebar filters
# -----------------------------
st.sidebar.header("Filters")

ticker_search = st.sidebar.text_input("Ticker contains", value="").strip().upper()

tf_options = sorted([x for x in df["tf"].dropna().unique().tolist() if x])
tf_selected = st.sidebar.multiselect("Timeframe", options=tf_options, default=tf_options)

dir_options = [x for x in ["bull", "bear"] if x in df["dir"].dropna().unique().tolist()]
dir_selected = st.sidebar.multiselect("Direction", options=dir_options, default=dir_options)

setup_search = st.sidebar.text_input("Setup contains", value="").strip()

score_min = int(df["score"].min()) if len(df) else 0
score_max = int(df["score"].max()) if len(df) else 0
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

f = f[(f["score"] >= score_range[0]) & (f["score"] <= score_range[1])]

if only_aligned and "aligned" in f.columns:
    # aligned can be True/False/None
    f = f[f["aligned"] == True]

# Round numbers for display
for col in ["current_price", "entry", "stop"]:
    f[col] = pd.to_numeric(f[col], errors="coerce").round(2)

# -----------------------------
# Legend
# -----------------------------
with st.expander("Score legend (what it means)"):
    st.markdown(
        """
**Score = Market bias only (from last closed candles on higher TFs), not the setup direction.**

Weights:
- **Y=5, Q=4, M=3, W=2, D=1**

Rule:
- Last closed candle **2U** → **add** weight  
- Last closed candle **2D** → **subtract** weight  
- Last closed candle **1 or 3** → **0**

Interpretation:
- **Positive score** → higher TFs are **bull-biased**
- **Negative score** → higher TFs are **bear-biased**
- **Near zero** → **mixed / neutral**

The **Dir (bull/bear)** column is the trade plan direction for the setup.  
Use **Only aligned with bias** to filter signals matching the bias.
        """
    )

# -----------------------------
# Helpers for HTML table
# -----------------------------
def pill_dir(d: str) -> str:
    d = (d or "").lower()
    if d == "bull":
        return '<span class="pill pill-bull">bull</span>'
    if d == "bear":
        return '<span class="pill pill-bear">bear</span>'
    return f'<span class="muted">{d}</span>'

def fmt_score(s: int) -> str:
    try:
        s = int(s)
    except Exception:
        s = 0
    if s > 0:
        return f'<span class="score-pos">{s}</span>'
    if s < 0:
        return f'<span class="score-neg">{s}</span>'
    return f'<span class="score-zero">{s}</span>'

def safe_str(x) -> str:
    if x is None:
        return ""
    if isinstance(x, float) and pd.isna(x):
        return ""
    return str(x)

# -----------------------------
# Build display dataframe
# -----------------------------
show = f.copy()

# Clickable ticker (only the ticker text visible)
def _ticker_link(row) -> str:
    t = safe_str(row.get("ticker", "")).upper()
    url = safe_str(row.get("chart_url", ""))
    if not t:
        return ""
    if not url or url.lower() == "nan":
        url = f"https://finance.yahoo.com/quote/{t}/chart"
    return f'<a href="{url}" target="_blank" rel="noopener noreferrer">{t}</a>'

show["Ticker"] = show.apply(_ticker_link, axis=1)
show["Dir"] = show["dir"].apply(pill_dir)
show["Score"] = show["score"].apply(fmt_score)

# nicer columns
show = show.rename(
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

# column order
display_cols = [
    "Ticker", "Price", "TF", "Pattern", "Setup to Trade", "Dir",
    "Entry", "Stop", "Score", "Aligned?", "Plan",
]
for c in display_cols:
    if c not in show.columns:
        show[c] = None

show = show[display_cols]

# Sort: strongest bias first, then TF
tmp_abs = f["score"].abs().values
show["_abs_score"] = tmp_abs
show = show.sort_values(by=["_abs_score", "TF"], ascending=[False, True]).drop(columns=["_abs_score"])

# -----------------------------
# Render
# -----------------------------
st.markdown(
    show.to_html(escape=False, index=False),
    unsafe_allow_html=True,
)
