import math
from typing import Dict, List, Optional

def devig_power_method(odds_list: List[float]) -> List[float]:
    """
    Strips bookmaker vig using the Power Method (k-exponent).
    Corrects for Favorite-Longshot Bias so heavy underdogs are not overvalued.
    """
    if not odds_list or any(o <= 1.0 for o in odds_list):
        return []

    raw_probs = [1.0 / o for o in odds_list]
    overround = sum(raw_probs)

    # If no vig or invalid market
    if abs(overround - 1.0) < 0.001:
        return raw_probs

    # Binary search to find exponent k where sum(p_i ^ k) == 1.0
    low, high = 1.0, 3.0
    k = 1.0
    
    for _ in range(30):  # 30 iterations gives high precision
        mid = (low + high) / 2.0
        val = sum(math.pow(p, mid) for p in raw_probs)
        if val > 1.0:
            low = mid
        else:
            high = mid
        k = mid

    # Calculate fair probabilities using k-exponent
    fair_probs = [math.pow(p, k) for p in raw_probs]
    total_fair = sum(fair_probs)
    
    return [p / total_fair for p in fair_probs]


def calculate_ev(fair_prob: float, target_odds: float) -> float:
    """Calculates Expected Value (EV) as a percentage decimal."""
    if target_odds <= 1.0 or fair_prob <= 0:
        return -1.0
    return (fair_prob * target_odds) - 1.0


def calculate_kelly_stake(
    fair_prob: float, 
    target_odds: float, 
    kelly_fraction: float = 0.25, 
    max_stake_pct: float = 0.05
) -> float:
    """Calculates bankroll stake percentage using Fractional Kelly Criterion."""
    if target_odds <= 1.0 or fair_prob <= 0:
        return 0.0
    
    b = target_odds - 1.0
    q = 1.0 - fair_prob
    full_kelly = ((b * fair_prob) - q) / b
    
    if full_kelly <= 0:
        return 0.0
    
    fractional_kelly = full_kelly * kelly_fraction
    return min(fractional_kelly, max_stake_pct)


def scan_match_arbitrage(outcomes_data: List[Dict], total_stake: float = 100.0) -> Optional[Dict]:
    """Evaluates cross-bookmaker arbitrage (Surebets)."""
    if not outcomes_data or len(outcomes_data) < 2:
        return None

    best_odds = [item["odds"] for item in outcomes_data]
    if any(o <= 1.0 for o in best_odds):
        return None

    arb_sum = sum(1.0 / o for o in best_odds)

    if arb_sum < 1.0:
        profit_margin_pct = ((1.0 / arb_sum) - 1.0) * 100.0
        stakes = [(total_stake / (arb_sum * o)) for o in best_odds]
        guaranteed_payout = stakes[0] * best_odds[0]
        guaranteed_profit = guaranteed_payout - total_stake

        return {
            "arb_sum": round(arb_sum, 4),
            "profit_margin_pct": round(profit_margin_pct, 2),
            "total_stake": round(total_stake, 2),
            "guaranteed_profit": round(guaranteed_profit, 2),
            "guaranteed_payout": round(guaranteed_payout, 2),
            "stakes": [round(s, 2) for s in stakes],
            "details": outcomes_data
        }
    return None