# app.py

import streamlit as st
import pandas as pd

st.set_page_config(layout="wide")

st.title("STRAT Scanner")
st.caption(
    "Educational / informational purposes only. Not financial advice. "
    "Trading involves risk."
)

DATA_PATH = "cache/results/latest.csv"

try:
    df = pd.read_csv(DATA_PATH)
except Exception:
    st.warning("No scan results found yet.")
    st.stop()

# Keep >= 1H only
ALLOWED_TFS = ["Y", "Q", "M", "W", "D", "4H", "3H", "2H", "1H"]
df = df[df["tf"].isin(ALLOWED_TFS)].copy()

# Round numbers
for col in ["current_price", "entry", "stop"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

# Create a link column (supported everywhere)
df["chart"] = df["ticker"].apply(lambda t: f"https://finance.yahoo.com/quote/{t}")

# Reorder columns
preferred_cols = [
    "ticker", "chart", "current_price",
    "tf", "kind", "pattern", "setup", "dir",
    "entry", "stop", "score", "note", "scan_time",
]
cols = [c for c in preferred_cols if c in df.columns]
df = df[cols]

def style_dir(val):
    if val == "bull":
        return "color: #16a34a; font-weight: 700"
    if val == "bear":
        return "color: #dc2626; font-weight: 700"
    return ""

st.subheader("Latest Scan Results")

st.dataframe(
    df.style.map(style_dir, subset=["dir"]),
    column_config={
        "chart": st.column_config.LinkColumn(
            "Chart",
            help="Open Yahoo Finance interactive chart",
            display_text="Yahoo",
        ),
        "current_price": st.column_config.NumberColumn("Price", format="%.2f"),
        "entry": st.column_config.NumberColumn("Entry", format="%.2f"),
        "stop": st.column_config.NumberColumn("Stop", format="%.2f"),
        "score": st.column_config.NumberColumn("Score"),
    },
    width="stretch",
    height=800,
)

