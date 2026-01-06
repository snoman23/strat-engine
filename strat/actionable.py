import pandas as pd


def detect_actionable(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detect ACTIONABLE (pre-trigger) STRAT setups.
    Uses ONLY fully CLOSED candles.

    Rule:
    - Evaluate candles i-2 and i-1
    - Mark candle i as actionable
    """

    df = df.copy().reset_index(drop=True)

    df["actionable"] = False
    df["action_type"] = None

    # We start at i = 2 so i-2 and i-1 both exist
    for i in range(2, len(df)):

        prev2 = df.loc[i - 2]  # CLOSED
        prev1 = df.loc[i - 1]  # CLOSED
        curr = df.loc[i]       # CURRENT (action candle)

        # -------------------------
        # 2 → 1 (Break Setup)
        # -------------------------
        if prev2["strat"] in ["2U", "2D"] and prev1["strat"] == "1":
            df.loc[i, "actionable"] = True
            direction = "2U → Break" if prev2["strat"] == "2U" else "2D → Break"
            df.loc[i, "action_type"] = f"2-1 ({direction})"

        # -------------------------
        # 3 → 1 (Expansion Play)
        # -------------------------
        elif prev2["strat"] == "3" and prev1["strat"] == "1":
            df.loc[i, "actionable"] = True
            df.loc[i, "action_type"] = "3-1 Expansion"

        # -------------------------
        # 1 → 1 (Coil)
        # -------------------------
        elif prev2["strat"] == "1" and prev1["strat"] == "1":
            df.loc[i, "actionable"] = True
            df.loc[i, "action_type"] = "1-1 Coil"

    return df
