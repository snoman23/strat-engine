# strat/classify.py
import pandas as pd


def classify_strat_candles(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds STRAT candle type:
    1  = Inside bar
    2U = Directional up
    2D = Directional down
    3  = Outside bar
    """

    df = df.copy().reset_index(drop=True)
    df["strat"] = None

    for i in range(1, len(df)):
        prev = df.loc[i - 1]
        curr = df.loc[i]

        # Inside bar
        if curr["high"] <= prev["high"] and curr["low"] >= prev["low"]:
            df.loc[i, "strat"] = "1"

        # Outside bar
        elif curr["high"] > prev["high"] and curr["low"] < prev["low"]:
            df.loc[i, "strat"] = "3"

        # Directional up
        elif curr["high"] > prev["high"]:
            df.loc[i, "strat"] = "2U"

        # Directional down
        elif curr["low"] < prev["low"]:
            df.loc[i, "strat"] = "2D"

    return df
