# scoring/continuity.py

def strat_direction(strat: str):
    """
    Map STRAT candle type to directional bias.
    2U => bull, 2D => bear, 1/3/None => neutral
    """
    if strat == "2U":
        return "bull"
    if strat == "2D":
        return "bear"
    return None


# User-friendly hierarchy weights (HTF dominates)
TIMEFRAME_WEIGHTS = {
    "Y": 5,
    "Q": 4,
    "M": 3,
    "W": 2,
    "D": 1,
}


def continuity_score(action_dir: str, last_closed_strat_by_tf: dict):
    """
    Compute continuity score based on how HTFs align with action direction.

    action_dir: "bull" or "bear"
    last_closed_strat_by_tf: e.g. {"Y":"2U","Q":"1","M":"2U","W":"2D","D":"2D"}

    Returns:
      (score:int, breakdown:dict)
    """
    score = 0
    breakdown = {}

    for tf, weight in TIMEFRAME_WEIGHTS.items():
        strat = last_closed_strat_by_tf.get(tf)
        tf_dir = strat_direction(strat)

        if tf_dir is None:
            breakdown[tf] = 0
            continue

        if tf_dir == action_dir:
            score += weight
            breakdown[tf] = weight
        else:
            score -= weight
            breakdown[tf] = -weight

    return score, breakdown
