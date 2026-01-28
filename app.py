# app.py
import pandas as pd
import streamlit as st

from config import SECTORS_11, SECTOR_TOP_ETFS

RESULTS_PATH = "cache/results/latest.csv"
CONTEXT_PATH = "cache/results/context.csv"

st.set_page_config(page_title="STRAT Scanner", page_icon="📈", layout="wide")
st.title("STRAT Scanner")
st.caption("Educational / informational purposes only. Not financial advice. Trading involves risk.")


@st.cache_data(ttl=60)
def load_results() -> pd.DataFrame:
    try:
        return pd.read_csv(RESULTS_PATH)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=60)
def load_context() -> pd.DataFrame:
    try:
        return pd.read_csv(CONTEXT_PATH)
    except Exception:
        return pd.DataFrame()


def make_ticker_link(ticker: str) -> str:
    t = str(ticker).strip().upper()
    if not t:
        return ""
    url = f"https://finance.yahoo.com/quote/{t}/chart"
    return f'<a href="{url}" target="_blank" rel="noopener noreferrer">{t}</a>'


def heat_color(val: int) -> str:
    try:
        v = int(val)
    except Exception:
        return ""
    if v >= 75:
        return "background-color: rgba(34,197,94,0.45);"
    if v >= 50:
        return "background-color: rgba(34,197,94,0.20);"
    if v <= 25:
        return "background-color: rgba(239,68,68,0.45);"
    if v < 50:
        return "background-color: rgba(239,68,68,0.20);"
    return ""


def _pct_bull(df: pd.DataFrame, col: str) -> pd.Series:
    x = df[col].astype(str).fillna("")
    bull = (x == "2U").astype(int)
    bear = (x == "2D").astype(int)
    denom = bull + bear
    pct = (bull / denom).where(denom > 0, 0.0)
    return pct


def _sector_heatmap(ctx: pd.DataFrame, mode: str) -> pd.DataFrame:
    # mode: "live" or "closed"
    cols = {
        "Y": f"ctx_Y_{mode}",
        "Q": f"ctx_Q_{mode}",
        "M": f"ctx_M_{mode}",
        "W": f"ctx_W_{mode}",
        "D": f"ctx_D_{mode}",
    }

    ctx = ctx.copy()
    ctx["sector"] = ctx.get("sector", "Unknown").astype(str).fillna("Unknown")

    idx = SECTORS_11 + (["Unknown"] if "Unknown" in ctx["sector"].unique() else [])
    out = pd.DataFrame(index=idx)

    for tf, c in cols.items():
        if c not in ctx.columns:
            out[tf] = 0
            continue
        tmp = ctx[["sector", c]].copy()
        tmp[c] = tmp[c].astype(str).fillna("")
        tmp["bull"] = (tmp[c] == "2U").astype(int)
        tmp["bear"] = (tmp[c] == "2D").astype(int)
        agg = tmp.groupby("sector")[["bull", "bear"]].sum()
        pct = (agg["bull"] / (agg["bull"] + agg["bear"])).where((agg["bull"] + agg["bear"]) > 0, 0.0)
        out[tf] = (pct * 100.0).round(0).astype(int)

    out = out[[c for c in ["Y", "Q", "M", "W", "D"] if c in out.columns]]
    return out


def _industry_heatmap(ctx: pd.DataFrame, sector: str, mode: str) -> pd.DataFrame:
    cols = {
        "Y": f"ctx_Y_{mode}",
        "Q": f"ctx_Q_{mode}",
        "M": f"ctx_M_{mode}",
        "W": f"ctx_W_{mode}",
        "D": f"ctx_D_{mode}",
    }
    ctx = ctx.copy()
    ctx["sector"] = ctx.get("sector", "Unknown").astype(str).fillna("Unknown")
    ctx["industry"] = ctx.get("industry", "Unknown").astype(str).fillna("Unknown")
    ctx = ctx[ctx["sector"] == sector]
    if ctx.empty:
        return pd.DataFrame()

    industries = sorted(ctx["industry"].unique().tolist())
    out = pd.DataFrame(index=industries)

    for tf, c in cols.items():
        if c not in ctx.columns:
            out[tf] = 0
            continue
        tmp = ctx[["industry", c]].copy()
        tmp[c] = tmp[c].astype(str).fillna("")
        tmp["bull"] = (tmp[c] == "2U").astype(int)
        tmp["bear"] = (tmp[c] == "2D").astype(int)
        agg = tmp.groupby("industry")[["bull", "bear"]].sum()
        pct = (agg["bull"] / (agg["bull"] + agg["bear"])).where((agg["bull"] + agg["bear"]) > 0, 0.0)
        out[tf] = (pct * 100.0).round(0).astype(int)

    out = out[[c for c in ["Y", "Q", "M", "W", "D"] if c in out.columns]]
    return out


# -----------------------------
# Load results (scanner table)
# -----------------------------
df = load_results()
if df.empty:
    st.error("No scan results found yet. The workflow hasn't written cache/results/latest.csv yet.")
    st.stop()

for col in [
    "scan_time","ticker","current_price","sector","industry","etfs_pretty",
    "tf","pattern","setup","dir","entry","stop",
    "score","aligned","last_strat","last_candle_type","actionable"
]:
    if col not in df.columns:
        df[col] = None

df["scan_time"] = df["scan_time"].astype(str).fillna("")
df["ticker"] = df["ticker"].astype(str).fillna("")
df["tf"] = df["tf"].astype(str).fillna("")
df["dir"] = df["dir"].astype(str).fillna("")
df["sector"] = df["sector"].astype(str).fillna("Unknown")
df["industry"] = df["industry"].astype(str).fillna("Unknown")
df["etfs_pretty"] = df["etfs_pretty"].astype(str).fillna("")
df["score"] = pd.to_numeric(df["score"], errors="coerce").fillna(0).astype(int)
for c in ["current_price","entry","stop"]:
    df[c] = pd.to_numeric(df[c], errors="coerce").round(2)

last_scan = df["scan_time"].iloc[0] if len(df) else "Unknown"
st.markdown(f"**Last scan_time (ET):** `{last_scan}`")

# Sidebar (scanner)
st.sidebar.header("Filters")
max_rows_render = st.sidebar.slider("Max rows to render (keeps app fast)", 100, 5000, 500, 100)

ticker_search = st.sidebar.text_input("Ticker contains", value="").strip().upper()
tf_options = sorted([x for x in df["tf"].dropna().unique().tolist() if x])
tf_selected = st.sidebar.multiselect("Timeframe", options=tf_options, default=tf_options)

dir_options = [x for x in ["bull", "bear"] if x in df["dir"].dropna().unique().tolist()]
dir_selected = st.sidebar.multiselect("Direction", options=dir_options, default=dir_options)

setup_search = st.sidebar.text_input("Setup contains", value="").strip()

sector_options = SECTORS_11 + (["Unknown"] if "Unknown" in df["sector"].unique() else [])
sector_selected = st.sidebar.multiselect("Sector", options=sector_options, default=sector_options)

only_aligned = st.sidebar.checkbox("Only aligned with bias", value=False)

f = df.copy()
if ticker_search:
    f = f[f["ticker"].str.upper().str.contains(ticker_search, na=False)]
if tf_selected:
    f = f[f["tf"].isin(tf_selected)]
if dir_selected:
    f = f[f["dir"].isin(dir_selected)]
if setup_search:
    f = f[f["setup"].astype(str).str.contains(setup_search, case=False, na=False)]
if sector_selected:
    f = f[f["sector"].isin(sector_selected)]
if only_aligned and "aligned" in f.columns:
    f = f[f["aligned"] == True]

f["_abs_score"] = f["score"].abs()
f = f.sort_values(by=["_abs_score","ticker","tf"], ascending=[False, True, True]).drop(columns=["_abs_score"])

tab_scan, tab_sectors = st.tabs(["Scanner", "Industry Sectors"])

with tab_scan:
    st.subheader("Latest Scan Results")
    csv_bytes = f.to_csv(index=False).encode("utf-8")
    st.download_button("Download filtered results (CSV)", data=csv_bytes, file_name="filtered_results.csv", mime="text/csv")

    out = f.head(max_rows_render).copy()
    out["Ticker"] = out["ticker"].apply(make_ticker_link)

    cols = [
        "Ticker","current_price","sector","industry","etfs_pretty",
        "tf","pattern","setup","dir",
        "last_strat","last_candle_type",
        "entry","stop",
        "score","aligned","actionable",
    ]
    cols = [c for c in cols if c in out.columns]

    out = out[cols].rename(columns={
        "current_price": "Price",
        "sector": "Sector",
        "industry": "Industry",
        "etfs_pretty": "ETF(s)",
        "tf": "TF",
        "pattern": "Pattern",
        "setup": "Setup",
        "dir": "Dir",
        "last_strat": "Last STRAT",
        "last_candle_type": "Last Candle Type",
        "entry": "Entry",
        "stop": "Stop",
        "score": "Score (Closed Bias)",
        "aligned": "Aligned?",
        "actionable": "Plan",
    })

    st.caption(f"Showing first {min(len(f), max_rows_render)} of {len(f)} filtered rows.")
    st.markdown(out.to_html(escape=False, index=False), unsafe_allow_html=True)

with tab_sectors:
    st.subheader("Sector & Industry Heatmaps (CURRENT candle — Live)")
    st.markdown(
        """
Heatmaps use the **current in-progress candle** for each timeframe (**D/W/M/Q/Y**).  
This matches “how it’s doing *right now*” and **can change until the candle closes**.

**Setups remain based on last CLOSED candles** (scanner does not repaint).
        """
    )

    ctx = load_context()
    if ctx.empty:
        st.error("No context.csv found yet. Run the workflow once after updating main.py.")
        st.stop()

    for col in ["sector","industry","ctx_Y_live","ctx_Q_live","ctx_M_live","ctx_W_live","ctx_D_live"]:
        if col not in ctx.columns:
            ctx[col] = None
    ctx["sector"] = ctx["sector"].astype(str).fillna("Unknown")
    ctx["industry"] = ctx["industry"].astype(str).fillna("Unknown")

    st.markdown("### Sector Heatmap (Live)")
    hm = _sector_heatmap(ctx, mode="live")
    hm2 = hm.copy()
    hm2.index = [
        f"{s} (ETFs: {', '.join(SECTOR_TOP_ETFS.get(s, [])[:3])})"
        if s in SECTOR_TOP_ETFS else s
        for s in hm2.index
    ]
    st.dataframe(hm2.style.applymap(heat_color), use_container_width=True)

    st.markdown("### Industry Heatmap (within a Sector — Live)")
    sector_pick = st.selectbox("Pick a sector", options=SECTORS_11, index=0)
    ihm = _industry_heatmap(ctx, sector_pick, mode="live")
    if ihm.empty:
        st.info("No industry data for this sector in the current scan slice yet.")
    else:
        st.dataframe(ihm.style.applymap(heat_color), use_container_width=True)
