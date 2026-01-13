# app.py
import pandas as pd
import streamlit as st

from config import CORE_ETFS, ETF_SECTOR_MAP

RESULTS_PATH = "cache/results/latest.csv"
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
def load_holdings() -> pd.DataFrame:
    try:
        df = pd.read_csv(HOLDINGS_PATH)
        # expected: ticker, etfs, etf_count
        return df
    except Exception:
        return pd.DataFrame()


def add_membership_and_sector(df: pd.DataFrame) -> pd.DataFrame:
    h = load_holdings()
    df = df.copy()

    df["ticker"] = df["ticker"].astype(str).fillna("").str.upper()

    if not h.empty and "ticker" in h.columns:
        h = h.copy()
        h["ticker"] = h["ticker"].astype(str).fillna("").str.upper()
        h["etfs"] = h.get("etfs", "").astype(str).fillna("")
        df = df.merge(h[["ticker", "etfs"]], on="ticker", how="left")
    else:
        df["etfs"] = ""

    # sector derived from ETF memberships
    def _sector_from_etfs(etfs_str: str) -> str:
        if not etfs_str or str(etfs_str).lower() == "nan":
            return "Unknown"
        etf_list = [e for e in str(etfs_str).split("|") if e]
        sectors = []
        for e in etf_list:
            s = ETF_SECTOR_MAP.get(e.upper())
            if s:
                sectors.append(s)
        sectors = sorted(set(sectors))
        if not sectors:
            return "Unknown"
        # if multiple, show combined
        if len(sectors) == 1:
            return sectors[0]
        return " / ".join(sectors)

    df["sector"] = df["etfs"].apply(_sector_from_etfs)

    # pretty ETF string for display
    df["etfs_pretty"] = df["etfs"].fillna("").apply(lambda x: ", ".join([e for e in str(x).split("|") if e]))
    return df


def sector_heatmap(df: pd.DataFrame) -> pd.DataFrame:
    """
    Builds a heatmap table: Sector x TF with % bullish setups.
    Proxy metric: bullish fraction among (bull+bear) rows for that TF.
    """
    d = df.copy()
    d["dir"] = d["dir"].astype(str).str.lower()
    d = d[d["dir"].isin(["bull", "bear"])]

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

    # Sector x TF matrix of bull_pct
    mat = pivot.pivot_table(index="sector", columns="tf", values="bull_pct", fill_value=0.0)

    # Keep TF order consistent (only those present)
    order = ["Y", "Q", "M", "W", "D", "4H", "3H", "2H", "1H"]
    cols = [c for c in order if c in mat.columns]
    mat = mat[cols]

    # Convert to % for display
    mat = (mat * 100.0).round(0).astype(int)
    return mat


def heatmap_style(val: int) -> str:
    """
    Color rules:
      - >= 75% bull => dark green
      - 50-74% bull => light green
      - 26-49% bull => light red (meaning more bear)
      - <= 25% bull => dark red
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
# Load + prepare data
# -----------------------------
df = load_results()
if df.empty:
    st.error("No scan results found yet. The workflow hasn't written cache/results/latest.csv yet.")
    st.stop()

# Ensure columns
for col in [
    "scan_time","ticker","chart_url","current_price",
    "tf","pattern","setup","dir","entry","stop","score","aligned",
    "last_strat","last_candle_type","actionable"
]:
    if col not in df.columns:
        df[col] = None

# Types
df["scan_time"] = df["scan_time"].astype(str).fillna("")
df["ticker"] = df["ticker"].astype(str).fillna("")
df["tf"] = df["tf"].astype(str).fillna("")
df["dir"] = df["dir"].astype(str).fillna("")
df["pattern"] = df["pattern"].astype(str).fillna("")
df["setup"] = df["setup"].astype(str).fillna("")
df["chart_url"] = df["chart_url"].astype(str).fillna("")
df["score"] = pd.to_numeric(df["score"], errors="coerce").fillna(0).astype(int)

for c in ["current_price","entry","stop"]:
    df[c] = pd.to_numeric(df[c], errors="coerce").round(2)

# Last scan time
last_scan = df["scan_time"].iloc[0] if len(df) else "Unknown"
st.markdown(f"**Last scan_time (ET):** `{last_scan}`")

# Add ETF membership + sector
df = add_membership_and_sector(df)

# -----------------------------
# Tabs
# -----------------------------
tab_scan, tab_sectors = st.tabs(["Scanner", "Industry Sectors"])

with tab_scan:
    # Sidebar filters
    st.sidebar.header("Filters")

    view_mode = st.sidebar.radio("View mode", ["Flat (all rows)", "Grouped by ticker"], index=0)

    ticker_search = st.sidebar.text_input("Ticker contains", value="").strip().upper()

    tf_options = sorted([x for x in df["tf"].dropna().unique().tolist() if x])
    tf_selected = st.sidebar.multiselect("Timeframe", options=tf_options, default=tf_options)

    dir_options = [x for x in ["bull", "bear"] if x in df["dir"].dropna().unique().tolist()]
    dir_selected = st.sidebar.multiselect("Direction", options=dir_options, default=dir_options)

    setup_search = st.sidebar.text_input("Setup contains", value="").strip()

    # ETF filter (choose one or more ETFs)
    etf_options = sorted([e for e in CORE_ETFS if e])
    etf_selected = st.sidebar.multiselect("Filter by ETF membership", options=etf_options, default=[])

    # Sector filter
    sector_options = sorted([s for s in df["sector"].dropna().unique().tolist() if s])
    sector_selected = st.sidebar.multiselect("Sector", options=sector_options, default=sector_options)

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

    if sector_selected:
        f = f[f["sector"].isin(sector_selected)]

    if etf_selected:
        # keep rows where etfs contain any selected
        def _has_any_etf(etfs_str: str) -> bool:
            if not etfs_str or str(etfs_str).lower() == "nan":
                return False
            s = set([x for x in str(etfs_str).split("|") if x])
            return any(e in s for e in etf_selected)
        f = f[f["etfs"].apply(_has_any_etf)]

    if only_aligned and "aligned" in f.columns:
        f = f[f["aligned"] == True]

    # Sorting: strongest bias then ticker then tf
    f["_abs_score"] = f["score"].abs()
    f = f.sort_values(by=["_abs_score","ticker","tf"], ascending=[False, True, True]).drop(columns=["_abs_score"])

    with st.expander("Score legend (what it means)"):
        st.markdown(
            """
**Score = Higher-timeframe bias only (Y/Q/M/W/D last closed candles).**
- Positive score → HTF bullish bias  
- Negative score → HTF bearish bias  
- Near 0 → mixed / neutral  

**ETF membership / sector** here is derived from CORE ETF holdings (SPY/QQQ/SMH/XL* etc.).  
This makes filtering fast and free.
            """
        )

    st.subheader("Latest Scan Results")

    # Flat table view (default)
    if view_mode.startswith("Flat"):
        display_cols = [
            "ticker", "current_price", "sector", "etfs_pretty",
            "tf", "pattern", "setup", "dir",
            "last_strat", "last_candle_type",
            "entry", "stop", "score", "aligned",
            "actionable", "chart_url"
        ]
        display_cols = [c for c in display_cols if c in f.columns]
        out = f[display_cols].copy()

        # Rename headers
        out = out.rename(columns={
            "ticker":"Ticker",
            "current_price":"Price",
            "sector":"Sector",
            "etfs_pretty":"ETF(s)",
            "tf":"TF",
            "pattern":"Pattern",
            "setup":"Setup",
            "dir":"Dir",
            "last_strat":"Last STRAT",
            "last_candle_type":"Last Candle Type",
            "entry":"Entry",
            "stop":"Stop",
            "score":"Score",
            "aligned":"Aligned?",
            "actionable":"Plan",
            "chart_url":"Chart",
        })

        st.dataframe(
            out,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Chart": st.column_config.LinkColumn(
                    "Chart",
                    display_text="Chart",
                    help="Open Yahoo Finance interactive chart"
                )
            }
        )

    # Grouped view
    else:
        for t in f["ticker"].dropna().unique().tolist():
            ft = f[f["ticker"] == t].copy()
            if ft.empty:
                continue

            price = ft["current_price"].dropna().iloc[0] if ft["current_price"].notna().any() else None
            score = int(ft["score"].dropna().iloc[0]) if ft["score"].notna().any() else 0
            sector = ft["sector"].dropna().iloc[0] if ft["sector"].notna().any() else "Unknown"
            etfs = ft["etfs_pretty"].dropna().iloc[0] if ft["etfs_pretty"].notna().any() else ""
            chart = ft["chart_url"].dropna().iloc[0] if ft["chart_url"].notna().any() else f"https://finance.yahoo.com/quote/{t}/chart"

            header = f"{t} | price={price} | score={score} | sector={sector}"
            if etfs:
                header += f" | ETFs={etfs}"

            with st.expander(header, expanded=False):
                st.markdown(f"[Open Yahoo Chart]({chart})")

                for tf in ["Y","Q","M","W","D","4H","3H","2H","1H"]:
                    ftt = ft[ft["tf"] == tf].copy()
                    if ftt.empty:
                        continue
                    st.markdown(f"### {tf}")
                    cols = [
                        "pattern","setup","dir",
                        "last_strat","last_candle_type",
                        "entry","stop","aligned","actionable"
                    ]
                    cols = [c for c in cols if c in ftt.columns]
                    st.dataframe(ftt[cols], use_container_width=True, hide_index=True)

with tab_sectors:
    st.subheader("Industry Sectors Heatmap (by TF)")

    st.markdown(
        """
This heatmap shows **% bullish setups** per **Sector × Timeframe** (proxy for “scanner bias” by sector).  
- **Dark green** ≥ 75% bullish  
- **Light green** ≥ 50% bullish  
- **Light red** < 50% bullish  
- **Dark red** ≤ 25% bullish  
        """
    )

    hm = sector_heatmap(df)
    if hm.empty:
        st.info("Not enough data to build the heatmap yet.")
    else:
        styled = hm.style.applymap(heatmap_style)
        st.dataframe(styled, use_container_width=True)
