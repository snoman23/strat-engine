def detect_setups(df):
    """
    Detect STRAT multi-candle setups.
    """
    df = df.copy()
    df["setup"] = None

    for i in range(2, len(df)):
        s1 = df.loc[i - 2, "strat"]
        s2 = df.loc[i - 1, "strat"]
        s3 = df.loc[i, "strat"]

        # 2-1-2 continuation
        if s1 in ["2U", "2D"] and s2 == "1" and s3 == s1:
            df.loc[i, "setup"] = f"2-1-2 {'Bullish' if s3 == '2U' else 'Bearish'} Continuation"
        # 3-1-2 reversal
        elif s1 == "3" and s2 == "1" and s3 in ["2U", "2D"]:
            df.loc[i, "setup"] = f"3-1-2 {'Bullish' if s3 == '2U' else 'Bearish'} Reversal"
        # Rev Strat 1-2-2
        elif s1 == "1" and s2 in ["2U", "2D"] and s3 == s2:
            df.loc[i, "setup"] = f"Rev Strat 1-2-2 {'Bullish' if s3 == '2U' else 'Bearish'}"
        # 2-2 continuation
        elif s2 in ["2U", "2D"] and s3 == s2:
            df.loc[i, "setup"] = f"2-2 {'Bullish' if s3 == '2U' else 'Bearish'} Continuation"
        # 2-2 reversal
        elif s2 in ["2U", "2D"] and s3 in ["2U", "2D"] and s2 != s3:
            df.loc[i, "setup"] = f"2-2 {'Bullish' if s3 == '2U' else 'Bearish'} Reversal"

    return df
