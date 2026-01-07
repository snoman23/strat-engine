# app.py
import os
import pandas as pd
import streamlit as st

RESULTS_CSV = os.path.join("cache", "results", "latest.csv")

# -----------------------------
# Page config + basic styling
# -----------------------------
st.set_page_config(
    page_title="STRAT Scanner",
    page_icon="📈",
    layout="wide",
)

st.markdown(
    """
    <style>
      .small-muted { color: #6b7280; font-size: 0.9rem; }
      .pill {
        display:inline-block; padding: 2px 10px; border-radius:999px;
        font-size: 0.85rem; border: 1px solid rgba(0,0,0,0.08);
        margin-right: 6px; margin-bottom: 6px;
      }
      .pill-bull { background: rgba(16,185,129,0.12); color: rgb(6,95,70); }
      .pill-bear { background: rgba(239,68,68,0.12); color: rgb(153,27,27); }
      .pill-neutral { background: rgba(107,114,128,0.12); color: rgb(55,65,81); }

      /* Make the table feel less "plain" */
      div[data-testid="stDataFrame"] { border-radius: 12px; overflow: hidden; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("STRAT Scanner")
st.caption("Educational / informational purposes only. Not financial advice. Trading involves risk.")

# -----------------------------
# Helpers
# -----------------------------
def load_results() -> pd.DataFrame:
    if not os.path.exists(RESULTS_CSV):
        return pd.DataFrame()

    df = pd.read_csv(RESULTS_CSV)

    # Normalize expected columns (your CSV may evolve)
    # These are the columns we try to work with.
    expected = [
        "scan_time", "ticker", "current_price",
        "tf", "pattern", "setup", "dir",
        "bias_dir", "bias_score", "aligned",
        "entry", "stop", "actionable",
    ]
    for c in expected:
        if c not in df.columns:
            df[c] = None

    # Make a proper clickable URL column (NOT markdown)
    # Streamlit LinkColumn expects URL values, not "[Chart](url)" strings.
    df["chart_url"] = df["ticker"].astype(str).apply(lambda t: f"https://finance.yahoo.com/quote/{t}/chart")

    # Format numeric columns
    for col in ["current_price", "entry", "stop", "bias_score"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def style_rows(styler: pd.io.formats.style.Styler) -> pd.io.formats.style.Styler:
    # Color bull rows green-ish, bear rows red-ish
    def _row_style(row):
        d = str(row.get("dir", "")).lower()
        if d == "bull":
            return ["background-color: rgba(16,185,129,0.08)"] * len(row)
        if d == "bear":
            return ["background-color: rgba(239,68,68,0.08)"] * len(row)
        return [""] * len(row)

    styler = styler.apply(_row_style, axis=1)

    # Format floats to 2 decimals
    fmt_cols = {}
    for c in ["current_price", "entry", "stop", "bias_score"]:
        if c in styler.data.columns:
            fmt_cols[c] = "{:,.2f}"
    if fmt_cols:
        styler = styler.format(fmt_cols, na_rep="—")

    # Slightly emphasize alignment
    if "aligned" in styler.data.columns:
        def _aligned_style(val):
            v = str(val).lower()
            if v == "aligned":
                return "font-weight: 700;"
            if v == "counter":
                return "opacity: 0.9;"
            return ""
        styler = styler.map(_aligned_style, subset=["aligned"])

    return styler


# -----------------------------
# Load
# -----------------------------
df = load_results()

if df.empty:
    st.warning("No scan results found yet. Your workflow should create `cache/results/latest.csv`.")
    st.stop()

# Scan time (already saved in ET in your main.py)
scan_time = str(df["scan_time"].dropna().iloc[0]) if "scan_time" in df.columns and df["scan_time"].notna().any() else "—"

# Header summary
colA, colB, colC = st.columns([2, 2, 6])
with colA:
    st.markdown(f"**Last scan_time:**  \n<span class='small-muted'>{scan_time}</span>", unsafe_allow_html=True)
with colB:
    tickers_count = df["ticker"].nunique() if "ticker" in df.columns else 0
    st.markdown(f"**Tickers in results:**  \n<span class='small-muted'>{tickers_count}</span>", unsafe_allow_html=True)
with colC:
    # Quick pills for bias_dir breakdown if present
    if "bias_dir" in df.columns:
        b = (df["bias_dir"].fillna("neutral").astype(str).str.lower())
        bull_n = int((b == "bull").sum())
        bear_n = int((b == "bear").sum())
        neut_n = int((b == "neutral").sum())
        st.markdown(
            f"""
            <span class="pill pill-bull">bull bias: {bull_n}</span>
            <span class="pill pill-bear">bear bias: {bear_n}</span>
            <span class="pill pill-neutral">neutral: {neut_n}</span>
            """,
            unsafe_allow_html=True
        )

st.divider()

# -----------------------------
# Legend (updated for new scoring model)
# -----------------------------
with st.expander("Legend (How to read score / bias)", expanded=False):
    st.markdown(
        """
**Core idea:** The scanner separates **(1) higher-timeframe bias** from **(2) the setup direction**.

### Bias fields
- **bias_dir**: direction of higher timeframe continuity (`bull`, `bear`, or `neutral`)
- **bias_score**: *strength* of that continuity (bigger number = stronger agreement)
  - **It is NOT bullish or bearish by itself.** Direction comes from **bias_dir**.
- **aligned**:
  - `aligned` = setup direction matches bias_dir  
  - `counter` = setup is against the bias (riskier)  
  - `neutral` = no bias edge

### Reading rows (examples)
- `bias_dir=bull, bias_score=14` → strong bullish wind
- `bias_dir=bear, bias_score=14` → strong bearish wind  
- A **bull** setup can still appear when bias is **bear** — it will show `aligned=counter`.

### Table fields
- **pattern** = what the last 2 closed candles were (e.g., `2U-1`)
- **setup** = what you’re planning to trade next (e.g., `Inside Break UP`)
- **entry / stop** = trigger and invalidation levels for alerts (2 decimals shown)
        """
    )

# -----------------------------
# Filters (sidebar)
# -----------------------------
st.sidebar.header("Filters")

# Only show >= 1H (as requested)
ALLOWED_TFS = ["1H", "2H", "3H", "4H", "D", "W", "M", "Q", "Y"]
df = df[df["tf"].isin(ALLOWED_TFS)].copy()

search = st.sidebar.text_input("Ticker search", value="").strip().upper()
if search:
    df = df[df["ticker"].astype(str).str.contains(search, na=False)]

tf_sel = st.sidebar.multiselect("Timeframes", options=ALLOWED_TFS, default=ALLOWED_TFS)
df = df[df["tf"].isin(tf_sel)].copy()

dir_opts = ["bull", "bear"]
dir_sel = st.sidebar.multiselect("Setup direction", options=dir_opts, default=dir_opts)
df = df[df["dir"].isin(dir_sel)].copy()

aligned_opts = ["aligned", "counter", "neutral"]
if "aligned" in df.columns:
    aligned_sel = st.sidebar.multiselect("Aligned vs Bias", options=aligned_opts, default=aligned_opts)
    df = df[df["aligned"].isin(aligned_sel)].copy()

min_score = st.sidebar.number_input("Min bias_score (strength)", min_value=0, max_value=100, value=0, step=1)
if "bias_score" in df.columns:
    df = df[df["bias_score"].fillna(0).abs() >= float(min_score)].copy()

st.sidebar.caption("Tip: start by filtering to `aligned` + higher bias_score.")

# -----------------------------
# Main table
# -----------------------------
st.subheader("Latest Scan Results")

# Reorder + keep the important columns only
display_cols = [
    "ticker",
    "current_price",
    "tf",
    "pattern",
    "setup",
    "dir",
    "bias_dir",
    "bias_score",
    "aligned",
    "entry",
    "stop",
    "chart_url",
]
display_cols = [c for c in display_cols if c in df.columns]

df_show = df[display_cols].copy()

# Round to 2 decimals everywhere relevant
for col in ["current_price", "entry", "stop", "bias_score"]:
    if col in df_show.columns:
        df_show[col] = pd.to_numeric(df_show[col], errors="coerce").round(2)

# Column configs: make chart_url a real clickable link labeled "Chart"
column_config = {
    "chart_url": st.column_config.LinkColumn(
        "Chart",
        help="Open Yahoo Finance interactive chart",
        display_text="Chart",   # IMPORTANT: must be a string (not a lambda)
    ),
}

# Nicely label columns
rename_map = {
    "ticker": "Ticker",
    "current_price": "Price",
    "tf": "TF",
    "pattern": "Pattern",
    "setup": "Setup",
    "dir": "Dir",
    "bias_dir": "Bias",
    "bias_score": "BiasScore",
    "aligned": "Aligned",
    "entry": "Entry",
    "stop": "Stop",
}
df_show = df_show.rename(columns=rename_map)

# Styled dataframe (bull/bear shading + 2 decimals)
styler = df_show.style
styler = style_rows(styler)

st.dataframe(
    styler,
    use_container_width=True,
    column_config=column_config,
    hide_index=True,
)

st.caption("If something looks off (e.g., 3-candle classification), we’ll validate that next by comparing the last two closed bars vs Yahoo/TradingView on a few tickers/timeframes.")
