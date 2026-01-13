# app.py
import pandas as pd
import streamlit as st

from config import SECTORS_11, SECTOR_TOP_ETFS

RESULTS_PATH = "cache/results/latest.csv"
HOLDINGS_PATH = "cache/universe/core_etf_holdings.csv"
SECTOR_MAP_PATH = "cache/universe/sector_map.csv"

st.set_page_config(page_title="STRAT Scanner", page_icon="📈", layout="wide")
st.title("STRAT Scanner")
st.caption("Educational / informational purposes only. Not financial advice. Trading involves risk.")


@st.cache_data(ttl=60)
def load_results() -> pd.DataFrame:
    try:
        return pd.read_csv(RESULTS_PATH)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_holdings() -> pd.DataFrame:
    try:
        return pd.read_csv(HOLDINGS_PATH)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_sector_map() -> pd.DataFrame:
    try:
        return pd.read_csv(SECTOR_MAP_PATH)
    except Exception:
        return pd.DataFrame()


def enrich_sector(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ticker"] = df["ticker"].astype(str).fillna("").str.upper()
    df["sector"] = "Unknown"

    sm = load_sector_map()
    if sm.empty or "ticker" not in sm.columns or "sector" not in sm.columns:
        return df

    sm = sm.copy()
    sm["ticker"] = sm["ticker"].astype(str).fillna("").str.upper()
    sm["sector"] = sm["sector"].astype(str).fillna("Unknown")

    out = df.merge(sm[["ticker", "sector"]], on="ticker", how="left", suffixes=("", "_m"))
    out["sector"] = out["sector_m"].fillna(out["sector"]).fillna("Unknown")
    out = out.drop(columns=["sector_m"], errors="ignore")
    return out


def enrich_etf_membership(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ticker"] = df["ticker"].astype(str).fillna("").str.upper()

    h = load_holdings()
    if h.empty or "ticker" not in h.columns:
        df["etfs"] = ""
        df["etfs_pretty"] = ""
        return df

    h = h.copy()
    h["ticker"] = h["ticker"].astype(str).fillna("").str.upper()
    h["etfs"] = h.get("etfs", "").astype(str).fillna("")

    out = df.merge(h[["ticker", "etfs"]], on="ticker", how="left")
    out["etfs"] = out["etfs"].fillna("")
    out["etfs_pretty"] = out["etfs"].apply(lambda x: ", ".join([e for e in str(x).split("|") if e]))
    return out


def make_ticker_link(ticker: str) -> str:
    t = str(ticker).strip().upper()
    if not t:
        return ""
    url = f"https://finance.yahoo.com/quote/{t}/chart"
    return f'<a href="{url}" target="_blank" rel="noopener noreferrer">{t}</a>'


def build_sector_heatmap(df: pd.DataFrame) -> pd.DataFrame:
    use_tfs = ["D", "W", "M", "Q", "Y"]
    d = df.copy()
    d["dir"] = d["dir"].astype(str).str.lower()
    d = d[d["dir"].isin(["bull", "bear"])]
    d = d[d["tf"].isin(use_tfs)]
    d["sector"] = d["sector"].astype(str).fillna("Unknown")

    if d.empty:
        return pd.DataFrame()

    grp = d.groupby(["sector", "tf", "dir"]).size().reset_index(name="n")
    piv = grp.pivot_table(index=["sector", "tf"], columns="dir", values="n", fill_value=0).reset_index()

    if "bull" not in piv.columns:
        piv["bull"] = 0
    if "bear" not in piv.columns:
        piv["bear"] = 0

    piv["total"] = piv["bull"] + piv["bear"]
    piv["bull_pct"] = piv.apply(lambda r: (r["bull"] / r["total"]) if r["total"] else 0.0, axis=1)

    mat = piv.pivot_table(index="sector", columns="tf", values="bull_pct", fill_value=0.0)
    mat = mat.reindex(SECTORS_11 + (["Unknown"] if "Unknown" in mat.index else []))
    mat = mat[[c for c in ["Y", "Q", "M", "W", "D"] if c in mat.columns]]

    mat = mat.replace([float("inf"), float("-inf")], 0.0).fillna(0.0)
    return (mat * 100.0).round(0).astype(int)


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


# -----------------------------
# Load results
# -----------------------------
df = load_results()
if df.empty:
    st.error("No scan results found yet. The workflow hasn't written cache/results/latest.csv yet.")
    st.stop()

for col in [
    "scan_time","ticker","current_price","tf","pattern","setup","dir",
    "entry","stop","score","aligned","last_strat","last_candle_type","actionable"
]:
    if col not in df.columns:
        df[col] = None

df["scan_time"] = df["scan_time"].astype(str).fillna("")
df["ticker"] = df["ticker"].astype(str).fillna("")
df["tf"] = df["tf"].astype(str).fillna("")
df["dir"] = df["dir"].astype(str).fillna("")
df["pattern"] = df["pattern"].astype(str).fillna("")
df["setup"] = df["setup"].astype(str).fillna("")
df["actionable"] = df["actionable"].astype(str).fillna("")
df["score"] = pd.to_numeric(df["score"], errors="coerce").fillna(0).astype(int)

for c in ["current_price","entry","stop"]:
    df[c] = pd.to_numeric(df[c], errors="coerce").round(2)

last_scan = df["scan_time"].iloc[0] if len(df) else "Unknown"
st.markdown(f"**Last scan_time (ET):** `{last_scan}`")

# Enrich (fast)
df = enrich_sector(df)
df = enrich_etf_membership(df)

# -----------------------------
# Sidebar controls (render cap prevents spinner-of-death)
# -----------------------------
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

all_etfs = set()
for x in df["etfs"].fillna("").astype(str).tolist():
    for e in x.split("|"):
        if e:
            all_etfs.add(e)
etf_options = sorted(all_etfs)
etf_selected = st.sidebar.multiselect("ETF membership contains", options=etf_options, default=[])

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
if sector_selected:
    f = f[f["sector"].isin(sector_selected)]
if etf_selected:
    def _has_any(etfs_str: str) -> bool:
        if not etfs_str or str(etfs_str).lower() == "nan":
            return False
        s = set([e for e in str(etfs_str).split("|") if e])
        return any(e in s for e in etf_selected)
    f = f[f["etfs"].apply(_has_any)]
if only_aligned and "aligned" in f.columns:
    f = f[f["aligned"] == True]

# Sort (stable)
f["_abs_score"] = f["score"].abs()
f = f.sort_values(by=["_abs_score","ticker","tf"], ascending=[False, True, True]).drop(columns=["_abs_score"])

# Tabs
tab_scan, tab_sectors = st.tabs(["Scanner", "Industry Sectors"])

with tab_scan:
    st.subheader("Latest Scan Results")

    # Download full filtered data (so we can render small but still give full access)
    csv_bytes = f.to_csv(index=False).encode("utf-8")
    st.download_button("Download filtered results (CSV)", data=csv_bytes, file_name="filtered_results.csv", mime="text/csv")

    # Render only first N rows as HTML (keeps app responsive)
    out = f.head(max_rows_render).copy()
    out["Ticker"] = out["ticker"].apply(make_ticker_link)

    cols = [
        "Ticker","current_price","sector","etfs_pretty",
        "tf","pattern","setup","dir",
        "last_strat","last_candle_type",
        "entry","stop",
        "score","aligned","actionable",
    ]
    cols = [c for c in cols if c in out.columns]

    out = out[cols].rename(columns={
        "current_price": "Price",
        "sector": "Sector",
        "etfs_pretty": "ETF(s)",
        "tf": "TF",
        "pattern": "Pattern",
        "setup": "Setup",
        "dir": "Dir",
        "last_strat": "Last STRAT",
        "last_candle_type": "Last Candle Type",
        "entry": "Entry",
        "stop": "Stop",
        "score": "Score",
        "aligned": "Aligned?",
        "actionable": "Plan",
    })

    st.caption(f"Showing first {min(len(f), max_rows_render)} of {len(f)} filtered rows (to keep the app fast).")
    st.markdown(out.to_html(escape=False, index=False), unsafe_allow_html=True)

with tab_sectors:
    st.subheader("Industry Sectors (Daily and Higher)")
    st.markdown(
        """
Heatmap shows **% bullish setups** within each **Sector × Timeframe** (**D/W/M/Q/Y only**).
- **Dark green** ≥ 75% bullish  
- **Light green** 50–74% bullish  
- **Light red** 26–49% bullish  
- **Dark red** ≤ 25% bullish  
        """
    )

    hm = build_sector_heatmap(df)
    if hm.empty:
        st.info("Not enough data to build the heatmap yet.")
    else:
        hm2 = hm.copy()
        hm2.index = [
            f"{s} (ETFs: {', '.join(SECTOR_TOP_ETFS.get(s, [])[:3])})"
            if s in SECTOR_TOP_ETFS else s
            for s in hm2.index
        ]
        styled = hm2.style.applymap(heat_color)
        st.dataframe(styled, use_container_width=True)
