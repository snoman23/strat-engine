# app.py

import streamlit as st
import pandas as pd

st.set_page_config(layout="wide")

st.title("STRAT Scanner")
st.caption("Educational / informational only — not financial advice.")

DATA_PATH = "cache/results/latest.csv"

try:
    df = pd.read_csv(DATA_PATH)
except Exception:
    st.warning("No scan results available yet.")
    st.stop()

# =========================
# FILTER TIMEFRAMES (>=1H)
# =========================
ALLOWED_TFS = ["Y", "Q", "M", "W", "D", "4H", "3H", "2H", "1H"]
df = df[df["tf"].isin(ALLOWED_TFS)].copy()

# Round numbers for UX
for col in ["current_price", "entry", "stop"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

# Add Yahoo URL column (clickable)
df["yahoo"] = df["ticker"].apply(lambda t: f"https://finance.yahoo.com/quote/{t}")

# Optional: reorder columns so link is near ticker
preferred = [
    "ticker", "yahoo", "current_price",
    "tf", "kind", "pattern", "setup", "dir",
    "entry", "stop", "score", "note",
    "scan_time",
]
cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
df = df[cols]

def color_dir(val):
    if val == "bull":
        return "color: #16a34a; font-weight: 700"  # green
    if val == "bear":
        return "color: #dc2626; font-weight: 700"  # red
    return ""

st.subheader("Latest Scan Results")

st.dataframe(
    df.style.map(color_dir, subset=["dir"]),
    column_config={
        "yahoo": st.column_config.LinkColumn(
            "Chart",
            display_text="Yahoo",
            help="Open Yahoo Finance interactive chart",
        )
    },
    width="stretch",
    height=750,
)

