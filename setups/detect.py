import pandas as pd


def detect_strat_setups(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detects STRAT combos and adds a 'setup' column
    """

    df = df.copy().reset_index(drop=True)
    df["setup"] = None

    for i in range(2, len(df)):
        a = df.loc[i - 2, "strat"]
        b = df.loc[i - 1, "strat"]
        c = df.loc[i, "strat"]

        # 2-1-2 Continuations
        if a == "2U" and b == "1" and c == "2U":
            df.loc[i, "setup"] = "2-1-2 Bullish Continuation"
        elif a == "2D" and b == "1" and c == "2D":
            df.loc[i, "setup"] = "2-1-2 Bearish Continuation"

        # 2-1-2 Reversals
        elif a == "2D" and b == "1" and c == "2U":
            df.loc[i, "setup"] = "2-1-2 Bullish Reversal"
        elif a == "2U" and b == "1" and c == "2D":
            df.loc[i, "setup"] = "2-1-2 Bearish Reversal"

        # 3-1-2 Reversals
        elif a == "3" and b == "1" and c == "2U":
            df.loc[i, "setup"] = "3-1-2 Bullish Reversal"
        elif a == "3" and b == "1" and c == "2D":
            df.loc[i, "setup"] = "3-1-2 Bearish Reversal"

        # 1-Bar Reversal
        elif b == "1" and c == "3":
            df.loc[i, "setup"] = "1-Bar Reversal"

        # 1-2 Reversals
        elif b == "1" and c == "2U":
            df.loc[i, "setup"] = "1-2 Bullish Reversal"
        elif b == "1" and c == "2D":
            df.loc[i, "setup"] = "1-2 Bearish Reversal"

        # 2-2 Continuations
        elif a == "2U" and b == "2U":
            df.loc[i, "setup"] = "2-2 Bullish Continuation"
        elif a == "2D" and b == "2D":
            df.loc[i, "setup"] = "2-2 Bearish Continuation"

        # 2-2 Reversals
        elif a == "2D" and b == "2U":
            df.loc[i, "setup"] = "2-2 Bullish Reversal"
        elif a == "2U" and b == "2D":
            df.loc[i, "setup"] = "2-2 Bearish Reversal"

        # 3-2-2 Reversals
        elif a == "3" and b == "2U" and c == "2U":
            df.loc[i, "setup"] = "3-2-2 Bullish Reversal"
        elif a == "3" and b == "2D" and c == "2D":
            df.loc[i, "setup"] = "3-2-2 Bearish Reversal"

        # Rev Strat 1-2-2
        elif a == "1" and b == "2U" and c == "2U":
            df.loc[i, "setup"] = "Rev Strat 1-2-2 Bullish"
        elif a == "1" and b == "2D" and c == "2D":
            df.loc[i, "setup"] = "Rev Strat 1-2-2 Bearish"

    return df
