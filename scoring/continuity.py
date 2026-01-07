# scoring/continuity.py

from typing import Dict, Tuple

# weights for Y/Q/M/W/D
WEIGHTS = {
    "Y": 5,
    "Q": 4,
    "M": 3,
    "W": 2,
    "D": 1,
}

BULL = {"2U"}
BEAR = {"2D"}


def continuity_bias(context: Dict[str, str]) -> Tuple[str, int, Dict[str, int]]:
    """
    context: dict like {"Y":"2U","Q":"2D",...} using last CLOSED candle strat type per TF.

    Returns:
      bias_dir: "bull" | "bear" | "neutral"
      bias_score: non-negative int magnitude (strength of continuity)
      tf_votes: per-tf signed votes (for debugging / transparency)
    """
    tf_votes: Dict[str, int] = {}
    total = 0

    for tf, w in WEIGHTS.items():
        s = context.get(tf)
        if s in BULL:
            tf_votes[tf] = +w
            total += w
        elif s in BEAR:
            tf_votes[tf] = -w
            total -= w
        else:
            tf_votes[tf] = 0

    if total > 0:
        return "bull", abs(total), tf_votes
    if total < 0:
        return "bear", abs(total), tf_votes
    return "neutral", 0, tf_votes


def setup_alignment(setup_dir: str, bias_dir: str) -> str:
    """
    setup_dir: "bull"|"bear"|None
    bias_dir: "bull"|"bear"|"neutral"
    """
    if setup_dir not in ("bull", "bear"):
        return "neutral"
    if bias_dir == "neutral":
        return "neutral"
    return "aligned" if setup_dir == bias_dir else "counter"
