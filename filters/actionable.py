import pandas as pd

# Minimum continuity threshold for regular setups
MIN_CONTINUITY_SCORE = 2

# Minimum consecutive 2U/2D candles for PMG
PMG_MIN_CANDLES = 5

def filter_actionable_setups(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter STRAT setups to return only actionable trade candidates.
    Works with:
    - Continuations
    - Reversals
    - Special setups
    - PMG (5 or more consecutive 2U/2D candles)
    """

    if df.empty:
        return df

    actionable = df.copy()

    # Step 1: Keep only rows with a valid setup
    actionable = actionable[actionable["setup_type"].notna()]

    # Ensure continuity_score exists
    if 'continuity_score' not in actionable.columns:
        actionable['continuity_score'] = MIN_CONTINUITY_SCORE

    # Step 2: Continuity filter for regular setups (not PMG)
    regular_setups = actionable[~actionable["setup_type"].str.contains("PMG")]
    regular_setups = regular_setups[regular_setups["continuity_score"] >= MIN_CONTINUITY_SCORE]

    # Step 3: Candle type sanity check for regular setups
    regular_setups = regular_setups[
        (
            (regular_setups["direction"] == "long") &
            (regular_setups["candle_type"].isin(["2U", "3"]))
        ) |
        (
            (regular_setups["direction"] == "short") &
            (regular_setups["candle_type"].isin(["2D", "3"]))
        ) |
        (regular_setups["setup_type"].str.contains("Reversal|Special"))
    ]

    # Step 4: PMG filter (5+ consecutive 2U/2D)
    pmg_setups = actionable[actionable["setup_type"].str.contains("PMG")]
    if not pmg_setups.empty:
        pmg_setups = pmg_setups[pmg_setups["consecutive_count"] >= PMG_MIN_CANDLES]

    # Step 5: Combine regular + PMG setups
    actionable_filtered = pd.concat([regular_setups, pmg_setups], ignore_index=True)

    # Step 6: Optional higher timeframe alignment
    if "htf_bias" in actionable_filtered.columns:
        actionable_filtered = actionable_filtered[
            actionable_filtered["direction"] == actionable_filtered["htf_bias"]
        ]

    return actionable_filtered.reset_index(drop=True)
