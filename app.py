# app.py

import streamlit as st
import pandas as pd

st.set_page_config(layout="wide")

st.title("STRAT Scanner")
st.caption(
    "Educational / informational purposes only. "
    "Not financial advice. Trading involves risk."
)

DATA_PATH = "cache/results/latest.csv"

# =========================
# LOAD DATA
# =========================
try:
    df = pd.read_csv(DATA_PATH)
except Exception:
    st.warning("No scan results found yet. Scanner has not produced data.")
    st.stop()

# =========================
# FILTER TIMEFRAMES (>= 1H)
# =========================
ALLOWED_TFS = ["Y", "Q", "M", "W", "D", "4H", "3H", "2H", "1H"]
df = df[df["tf"].isin(ALLOWED_TFS)].copy()

# =========================
# FORMAT NUMBERS
# =========================
for col in ["current_price", "entry", "stop"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

# =========================
# CLICKABLE TICKER COLUMN
# =========================
df["ticker_link"] = df["ticker"].apply(
    lambda t: f"https://finance.yahoo.com/quote/{t}"
)

# =========================
# COLUMN ORDER (UX)
# =========================
preferred_cols = [
    "ticker_link",
    "current_price",
    "tf",
    "kind",
    "pattern",
    "setup",
    "dir",
    "entry",
    "stop",
    "score",
    "note",
    "scan_time",
]

cols = [c for c in preferred_cols if c in df.columns]
df = df[cols]

# =========================
# STYLING
# =========================
def style_dir(val):
    if val == "bull":
        return "color: #16a34a; font-weight: 700"  # green
    if val == "bear":
        return "color: #dc2626; font-weight: 700"  # red
    return ""

st.subheader("Latest Scan Results")

st.dataframe(
    df.style.map(style_dir, subset=["dir"]),
    column_config={
        "ticker_link": st.column_config.LinkColumn(
            "Ticker",
            display_text=lambda url: url.split("/")[-1],
            help="Open Yahoo Finance interactive chart",
        ),
        "current_price": st.column_config.NumberColumn("Price", format="%.2f"),
        "entry": st.column_config.NumberColumn("Entry", format="%.2f"),
        "stop": st.column_config.NumberColumn("Stop", format="%.2f"),
        "score": st.column_config.NumberColumn("Score"),
    },
    width="stretch",
    height=800,
)

