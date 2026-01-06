import pandas as pd

CONTINUITY_SCORE_MEANING = {
    5:  "Strong Bullish Continuity (All timeframes aligned up)",
    4:  "Bullish Continuity (HTFs aligned, minor LTF pullback)",
    3:  "Bullish Continuity (Trend intact)",
    2:  "Bullish Bias (Mixed but favorable)",
    1:  "Slight Bullish Bias",
    0:  "Neutral / No Edge",
   -1:  "Slight Bearish Bias",
   -2:  "Bearish Bias (Mixed but unfavorable)",
   -3:  "Bearish Continuity (Trend intact)",
   -4:  "Bearish Continuity (HTFs aligned down)",
   -5:  "Strong Bearish Continuity (All timeframes aligned down)"
}

def strat_bias(strat_value: str) -> str:
    """
    Convert STRAT candle to directional bias
    """
    if strat_value == "2U":
        return "bullish"
    elif strat_value == "2D":
        return "bearish"
    else:
        return "neutral"


def timeframe_continuity(timeframes: dict) -> dict:
    """
    timeframes = {
        "Y": df_yearly,
        "Q": df_quarterly,
        "M": df_monthly,
        "D": df_daily,
        "60": df_60m
    }
    """

    result = {}
    score = 0

    for tf, df in timeframes.items():
        if df.empty:
            result[tf] = "neutral"
            continue

        last_strat = df.iloc[-1]["strat"]
        bias = strat_bias(last_strat)
        result[tf] = bias

        if bias == "bullish":
            score += 1
        elif bias == "bearish":
            score -= 1

    result["score"] = score

    # Overall continuity label
    if score >= 3:
        result["continuity"] = "Bullish Continuity"
    elif score <= -3:
        result["continuity"] = "Bearish Continuity"
    else:
        result["continuity"] = "Mixed / Neutral"

    result["score_meaning"] = CONTINUITY_SCORE_MEANING.get(
       score,
       "Extreme / Undefined"
    )

    return result
