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
df = df[df["tf"].isin(ALLOWED_TFS)]

df["ticker"] = df["ticker"].apply(
    lambda t: f"[{t}](https://finance.yahoo.com/quote/{t})"
)

df["current_price"] = df["current_price"].round(2)
df["entry"] = df["entry"].round(2)
df["stop"] = df["stop"].round(2)

def color_dir(val):
    if val == "bull":
        return "color: green; font-weight: bold"
    if val == "bear":
        return "color: red; font-weight: bold"
    return ""

st.subheader("Latest Scan Results")

st.dataframe(
    df.style.applymap(color_dir, subset=["dir"]),
    width="stretch",
    height=700,
)
