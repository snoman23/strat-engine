# app.py
import pandas as pd
import streamlit as st

from config import SECTORS_11, SECTOR_TOP_ETFS

RESULTS_PATH = "cache/results/latest.csv"
STOCKS_BIGGEST_PATH = "cache/universe/stocks_biggest.csv"
HOLDINGS_PATH = "cache/universe/core_etf_holdings.csv"

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
def load_stocks_biggest() -> pd.DataFrame:
    try:
        return pd.read_csv(STOCKS_BIGGEST_PATH)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_holdings() -> pd.DataFrame:
    try:
        return pd.read_csv(HOLDINGS_PATH)
    except Exception:
        return pd.DataFrame()


def normalize_to_11_sector(raw: str) -> str:
    """
    Convert whatever StockAnalysis gives into your exact 11 sector names.
    If unknown, return "Unknown".
    """
    if raw is None:
        return "Unknown"
    s = str(raw).strip()
    if not s or s.lower() == "nan":
        return "Unknown"

    mapping = {
        "Communication Services": "Communication Services",
        "Consumer Discretionary": "Consumer Discretionary",
        "Consumer Staples": "Consumer Staples",
        "Energy": "Energy",
        "Financials": "Financials",
        "Health Care": "Health Care",
        "Healthcare": "Health Care",
        "Industrials": "Industrials",
        "Information Technology": "Information Technology",
        "Technology": "Information Technology",
        "Materials": "Materials",
        "Real Estate": "Real Estate",
        "Utilities": "Utilities",
    }

    if s in mapping:
        return mapping[s]

    for k, v in mapping.items():
        if k.lower() in s.lower():
            return v

    return "Unknown"


def enrich_sector_from_stockanalysis(df: pd.DataFrame) -> pd.DataFrame:
    """
    Assign ONE sector (from your 11) using StockAnalysis cached table.
    """
    df = df.copy()
    df["ticker"] = df["ticker"].astype(str).fillna("").str.upper()
    df["sector"] = "Unknown"

    s = load_stocks_biggest()
    if s.empty or "Symbol" not in s.columns:
        return df

    s = s.copy()
    s["Symbol"] = s["Symbol"].astype(str).fillna("").str.upper()

    sector_col = None
    for cand in ["Sector", "sector", "Industry", "industry"]:
        if cand in s.columns:
            sector_col = cand
            break

    if not sector_col:
        return df

    s = s[["Symbol", sector_col]].rename(columns={"Symbol": "ticker", sector_col: "sector_raw"})
    s["sector_raw"] = s["sector_raw"].astype(str).fillna("")

    merged = df.merge(s, on="ticker", how="left")
    merged["sector"] = merged["sector_raw"].apply(normalize_to_11_sector)
    merged = merged.drop(columns=["sector_raw"], errors="ignore")

    return merged


def enrich_etf_membership(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add ETF membership for STOCKS using cache/universe/core_etf_holdings.csv
    Columns expected: ticker, etfs (pipe-delimited), etf_count
    """
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

    merged = df.merge(h[["ticker", "etfs"]], on="ticker", how="left")
    merged["etfs"] = merged["etfs"].fillna("")
    merged["etfs_pretty"] = merged["etfs"].apply(lambda x: ", ".join([e for e in str(x).split("|") if e]))
    return merged


def make_ticker_link(ticker: str) -> str:
    t = str(ticker).strip().upper()
    if not t:
        return ""
    url = f"https://finance.yahoo.com/quote/{t}/chart"
    return f'<a href="{url}" target="_blank" rel="noopener noreferrer">{t}</a>'


def build_sector_heatmap(df: pd.DataFrame) -> pd.DataFrame:
    """
    Heatmap: % bullish setups by Sector x TF.
    Only Daily or higher: D/W/M/Q/Y.
    """
    use_tfs = ["D", "W", "M", "Q", "Y"]
    d = df.copy()
    d["dir"] = d["dir"].astype(str).str.lower()
    d = d[d["dir"].isin(["bull", "bear"])]
    d = d[d["tf"].isin(use_tfs)]

    if d.empty:
        return pd.DataFrame()

    grp = d.groupby(["sector", "tf", "dir"]).size().reset_index(name="n")
    pivot = grp.pivot_table(index=["sector", "tf"], columns="dir", values="n", fill_value=0).reset_index()

    if "bull" not in pivot.columns:
        pivot["bull"] = 0
    if "bear" not in pivot.columns:
        pivot["bear"] = 0

    pivot["total"] = pivot["bull"] + pivot["bear"]
    pivot["bull_pct"] = pivot.apply(lambda r: (r["bull"] / r["total"]) if r["total"] else 0.0, axis=1)

    mat = pivot.pivot_table(index="sector", columns="tf", values="bull_pct", fill_value=0.0)

    # enforce 11-sector order
    mat = mat.reindex(SECTORS_11)

    cols = [c for c in ["Y", "Q", "M", "W", "D"] if c in mat.columns]
    mat = mat[cols]

    # show as % integers
    mat = (mat * 100.0).round(0).astype(int)
    return mat


def heat_color(val: int) -> str:
    """
    Correct thresholds:
      Dark green >= 75% bullish
      Light green 50–74%
      Light red 26–49%
      Dark red <= 25%
    """
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
# Load + normalize results
# -----------------------------
df = load_results()
if df.empty:
    st.error("No scan results found yet. The workflow hasn't written cache/results/latest.csv yet.")
    st.stop()

# Ensure expected columns exist
for col in [
    "scan_time", "ticker", "current_price",
    "tf", "pattern", "setup", "dir",
    "entry", "stop", "score", "aligned",
    "last_strat", "last_candle_type",
    "actionable"
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

for c in ["current_price", "entry", "stop"]:
    df[c] = pd.to_numeric(df[c], errors="coerce").round(2)

last_scan = df["scan_time"].iloc[0] if len(df) else "Unknown"
st.markdown(f"**Last scan_time (ET):** `{last_scan}`")

# Enrich with single-sector + ETF membership
df = enrich_sector_from_stockanalysis(df)
df = enrich_etf_membership(df)

# -----------------------------
# Tabs
# -----------------------------
tab_scan, tab_sectors = st.tabs(["Scanner", "Industry Sectors"])

with tab_scan:
    st.sidebar.header("Filters")

    ticker_search = st.sidebar.text_input("Ticker contains", value="").strip().upper()

    tf_options = sorted([x for x in df["tf"].dropna().unique().tolist() if x])
    tf_selected = st.sidebar.multiselect("Timeframe", options=tf_options, default=tf_options)

    dir_options = [x for x in ["bull", "bear"] if x in df["dir"].dropna().unique().tolist()]
    dir_selected = st.sidebar.multiselect("Direction", options=dir_options, default=dir_options)

    setup_search = st.sidebar.text_input("Setup contains", value="").strip()

    # Sector filter (strict 11 sectors + Unknown)
    sector_options = SECTORS_11 + (["Unknown"] if "Unknown" in df["sector"].unique() else [])
    sector_selected = st.sidebar.multiselect("Sector", options=sector_options, default=sector_options)

    # ETF membership filter (for stocks)
    all_etfs = set()
    for x in df["etfs"].fillna("").astype(str).tolist():
        for e in x.split("|"):
            if e:
                all_etfs.add(e)
    etf_options = sorted(all_etfs)
    etf_selected = st.sidebar.multiselect("ETF membership contains", options=etf_options, default=[])

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
    if etf_selected:
        def _has_any_etf(etfs_str: str) -> bool:
            if not etfs_str or str(etfs_str).lower() == "nan":
                return False
            s = set([e for e in str(etfs_str).split("|") if e])
            return any(e in s for e in etf_selected)
        f = f[f["etfs"].apply(_has_any_etf)]
    if only_aligned and "aligned" in f.columns:
        f = f[f["aligned"] == True]

    # sort strongest bias first
    f["_abs_score"] = f["score"].abs()
    f = f.sort_values(by=["_abs_score", "ticker", "tf"], ascending=[False, True, True]).drop(columns=["_abs_score"])

    st.subheader("Latest Scan Results")

    # Ticker clickable (no separate chart column)
    out = f.copy()
    out["Ticker"] = out["ticker"].apply(make_ticker_link)

    # Keep ETF membership visible for stocks
    display_cols = [
        "Ticker", "current_price", "sector", "etfs_pretty",
        "tf", "pattern", "setup", "dir",
        "last_strat", "last_candle_type",
        "entry", "stop",
        "score", "aligned",
        "actionable",
    ]

    out = out.rename(columns={
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

    out = out[[c for c in display_cols if c in out.columns]].rename(columns={
        "current_price": "Price",
        "etfs_pretty": "ETF(s)",
    })

    # Render as HTML so the ticker links work
    st.markdown(out.to_html(escape=False, index=False), unsafe_allow_html=True)

with tab_sectors:
    st.subheader("Industry Sectors (Daily and Higher)")

    st.markdown(
        """
Heatmap shows **% bullish setups** within each **Sector × Timeframe** (D/W/M/Q/Y only).
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
        # Add 2–3 top ETFs next to sector name
        hm2 = hm.copy()
        hm2.index = [
            f"{s}  (ETFs: {', '.join(SECTOR_TOP_ETFS.get(s, [])[:3])})"
            for s in hm2.index
        ]
        styled = hm2.style.applymap(heat_color)
        st.dataframe(styled, use_container_width=True)
