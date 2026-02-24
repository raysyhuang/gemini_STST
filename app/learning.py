"""
Outcome-driven quality recalibration — learning engine.

Analyzes closed paper trades to learn which quality factors predict
winning trades, using Bayesian-smoothed quartile win-rate analysis.

Key functions:
  - analyze_factor_weights()  → full analysis pipeline
  - create_recommendation()   → persist a WeightRecommendation
  - apply_recommendation()    → approve & activate weights
  - get_active_weights()      → load learned weights (or None → fallback)
"""

import logging
from datetime import datetime

import numpy as np
from sqlalchemy.orm import Session

from app.models import PaperTrade, ActiveWeight, WeightRecommendation
from app.recalibration_config import RECAL_CONFIG
from app.screener import MOMENTUM_DEFAULT_WEIGHTS
from app.mean_reversion import REVERSION_DEFAULT_WEIGHTS

logger = logging.getLogger(__name__)

# Factor keys per strategy
STRATEGY_FACTORS = {
    "momentum": list(MOMENTUM_DEFAULT_WEIGHTS.keys()),
    "reversion": list(REVERSION_DEFAULT_WEIGHTS.keys()),
}

DEFAULT_WEIGHTS = {
    "momentum": MOMENTUM_DEFAULT_WEIGHTS,
    "reversion": REVERSION_DEFAULT_WEIGHTS,
}


# ── Bayesian Quartile Analysis ────────────────────────────────────

def _assign_quartile(score: float) -> int:
    """Map a 0-100 factor score to quartile 1-4."""
    if score < 25:
        return 1
    elif score < 50:
        return 2
    elif score < 75:
        return 3
    else:
        return 4


def _compute_factor_predictive_power(
    trades: list[dict],
    factor: str,
    prior_wr: float,
    k: float,
) -> dict:
    """
    Bayesian-smoothed quartile win-rate analysis for a single factor.

    Args:
        trades: list of {"factor_scores": {...}, "won": bool}
        factor: factor key name
        prior_wr: overall win rate (used as Bayesian prior)
        k: shrinkage strength (0.3 * min_observations)

    Returns dict with quartile detail and predictive power.
    """
    # Bucket trades into quartiles
    quartiles = {q: {"wins": 0, "total": 0} for q in range(1, 5)}

    for t in trades:
        fs = t.get("factor_scores") or {}
        score = fs.get(factor)
        if score is None:
            continue
        q = _assign_quartile(score)
        quartiles[q]["total"] += 1
        if t["won"]:
            quartiles[q]["wins"] += 1

    # Bayesian-smoothed win rate per quartile
    smoothed = {}
    for q in range(1, 5):
        n_q = quartiles[q]["total"]
        observed = quartiles[q]["wins"] / n_q if n_q > 0 else prior_wr
        smoothed[q] = (n_q * observed + k * prior_wr) / (n_q + k)

    # Predictive power = smoothed(Q4) - smoothed(Q1)
    power = smoothed[4] - smoothed[1]

    return {
        "factor": factor,
        "quartiles": {
            str(q): {
                "n": quartiles[q]["total"],
                "raw_wr": round(quartiles[q]["wins"] / quartiles[q]["total"], 3)
                if quartiles[q]["total"] > 0
                else None,
                "smoothed_wr": round(smoothed[q], 3),
            }
            for q in range(1, 5)
        },
        "predictive_power": round(power, 4),
    }


def _compute_new_weights(
    factor_analysis: list[dict],
    current_weights: dict,
    n_trades: int,
    k: float,
) -> dict:
    """
    Compute new weights from predictive powers:
      1. Normalize max(0, power) to sum to 1.0
      2. Bayesian blend with current weights
      3. Clamp each to [0.05, 0.50], re-normalize
    """
    clamp_min = RECAL_CONFIG["weight_clamp_min"]
    clamp_max = RECAL_CONFIG["weight_clamp_max"]

    # Step 1: raw weights from predictive power
    raw = {}
    for fa in factor_analysis:
        raw[fa["factor"]] = max(0.0, fa["predictive_power"])

    total_power = sum(raw.values())
    if total_power <= 0:
        # No factor has positive predictive power → keep current weights
        return dict(current_weights)

    normalized = {f: p / total_power for f, p in raw.items()}

    # Step 2: Bayesian blend with current weights
    blended = {}
    for f in normalized:
        cur = current_weights.get(f, 1.0 / len(normalized))
        blended[f] = (n_trades * normalized[f] + k * cur) / (n_trades + k)

    # Step 3: Iterative clamp-and-normalize (converges in 2-3 rounds)
    weights = dict(blended)
    for _ in range(20):
        clamped = {f: max(clamp_min, min(clamp_max, w)) for f, w in weights.items()}
        total = sum(clamped.values())
        weights = {f: w / total for f, w in clamped.items()}
        # Check convergence: all within clamp bounds (with tolerance)
        if all(clamp_min - 1e-6 <= w <= clamp_max + 1e-6 for w in weights.values()):
            break

    # Final hard clamp + normalize to guarantee bounds after rounding
    final = {f: max(clamp_min, min(clamp_max, round(w, 4))) for f, w in weights.items()}
    total = sum(final.values())
    if abs(total - 1.0) > 0.001:
        final = {f: round(w / total, 4) for f, w in final.items()}
    return final


def _compute_optimal_quality_floor(trades: list[dict]) -> float | None:
    """
    Test quality floors from 40-80 (step 5) and find the one that
    maximizes the balance of win_rate * sqrt(trade_count).

    Returns the optimal floor, or None if insufficient data.
    """
    lo, hi = RECAL_CONFIG["quality_floor_search_range"]
    step = RECAL_CONFIG["quality_floor_step"]

    best_floor = None
    best_score = -1.0

    for floor in range(lo, hi + 1, step):
        above = [t for t in trades if (t.get("quality_score") or 0) >= floor]
        n = len(above)
        if n < 5:
            continue
        wins = sum(1 for t in above if t["won"])
        wr = wins / n
        # Balance metric: win rate weighted by sqrt of sample size
        score = wr * np.sqrt(n)
        if score > best_score:
            best_score = score
            best_floor = floor

    return float(best_floor) if best_floor is not None else None


def _compute_regime_performance(trades: list[dict]) -> dict | None:
    """
    Compute win rate per regime → learned regime multipliers.

    Returns dict like {"Bullish": 1.1, "Mixed": 0.8, "Bearish": 0.5}
    or None if insufficient data.
    """
    lo_clamp, hi_clamp = RECAL_CONFIG["regime_multiplier_clamp"]
    regime_stats: dict[str, dict] = {}

    for t in trades:
        regime = t.get("regime_tag") or "Unknown"
        if regime == "Unknown":
            continue
        if regime not in regime_stats:
            regime_stats[regime] = {"wins": 0, "total": 0}
        regime_stats[regime]["total"] += 1
        if t["won"]:
            regime_stats[regime]["wins"] += 1

    if not regime_stats:
        return None

    # Compute win rate per regime
    overall_wr = sum(s["wins"] for s in regime_stats.values()) / max(
        1, sum(s["total"] for s in regime_stats.values())
    )

    multipliers = {}
    for regime, stats in regime_stats.items():
        if stats["total"] < 5:
            multipliers[regime] = 1.0
            continue
        regime_wr = stats["wins"] / stats["total"]
        # Multiplier = regime_wr / overall_wr, clamped
        if overall_wr > 0:
            mult = regime_wr / overall_wr
        else:
            mult = 1.0
        multipliers[regime] = round(max(lo_clamp, min(hi_clamp, mult)), 2)

    return multipliers


# ── Main Analysis Pipeline ────────────────────────────────────────

def _load_closed_trades(db: Session, strategy: str) -> list[dict]:
    """Load closed paper trades with factor_scores for a strategy."""
    trades = (
        db.query(PaperTrade)
        .filter(
            PaperTrade.strategy == strategy,
            PaperTrade.status == "closed",
            PaperTrade.factor_scores.isnot(None),
        )
        .all()
    )
    result = []
    for t in trades:
        result.append({
            "id": t.id,
            "factor_scores": t.factor_scores,
            "quality_score": t.quality_score,
            "regime_tag": t.regime_tag,
            "pnl_dollars": t.pnl_dollars or 0,
            "won": (t.pnl_dollars or 0) > 0,
        })
    return result


def analyze_factor_weights(
    db: Session,
    strategy: str,
    min_trades: int | None = None,
) -> dict | None:
    """
    Run the full Bayesian factor weight analysis for a strategy.

    Returns analysis dict or None if insufficient data.
    """
    if strategy not in STRATEGY_FACTORS:
        raise ValueError(f"Unknown strategy: {strategy}")

    if min_trades is None:
        min_trades = RECAL_CONFIG["min_observations"].get(strategy, 30)

    trades = _load_closed_trades(db, strategy)
    if len(trades) < min_trades:
        logger.info(
            "Insufficient data for %s: %d trades (need %d)",
            strategy, len(trades), min_trades,
        )
        return None

    # Overall win rate (Bayesian prior)
    n_total = len(trades)
    n_wins = sum(1 for t in trades if t["won"])
    overall_wr = n_wins / n_total

    # Bayesian shrinkage parameter
    k = RECAL_CONFIG["bayesian_prior_weight"] * min_trades

    # Per-factor analysis
    factors = STRATEGY_FACTORS[strategy]
    current_weights = DEFAULT_WEIGHTS[strategy]
    factor_analysis = []
    for factor in factors:
        fa = _compute_factor_predictive_power(trades, factor, overall_wr, k)
        factor_analysis.append(fa)

    # Compute new weights
    recommended_weights = _compute_new_weights(
        factor_analysis, current_weights, n_total, k,
    )

    # Optimal quality floor
    quality_floor = _compute_optimal_quality_floor(trades)

    # Regime performance
    regime_multipliers = _compute_regime_performance(trades)

    return {
        "strategy": strategy,
        "n_trades": n_total,
        "n_wins": n_wins,
        "overall_win_rate": round(overall_wr, 4),
        "current_weights": current_weights,
        "recommended_weights": recommended_weights,
        "factor_analysis": factor_analysis,
        "quality_floor": quality_floor,
        "regime_multipliers": regime_multipliers,
    }


# ── Recommendation CRUD ──────────────────────────────────────────

def create_recommendation(db: Session, strategy: str) -> WeightRecommendation | None:
    """
    Run analysis and persist a WeightRecommendation.
    Returns the recommendation or None if insufficient data.
    """
    analysis = analyze_factor_weights(db, strategy)
    if analysis is None:
        return None

    rec = WeightRecommendation(
        strategy=strategy,
        status="pending",
        n_trades_analyzed=analysis["n_trades"],
        overall_win_rate=analysis["overall_win_rate"],
        current_weights=analysis["current_weights"],
        recommended_weights=analysis["recommended_weights"],
        factor_analysis=analysis["factor_analysis"],
    )
    db.add(rec)
    db.commit()
    db.refresh(rec)

    logger.info(
        "Created recommendation #%d for %s (%d trades, WR=%.1f%%)",
        rec.id, strategy, rec.n_trades_analyzed, rec.overall_win_rate * 100,
    )
    return rec


def apply_recommendation(db: Session, rec_id: int) -> ActiveWeight:
    """
    Approve a recommendation and upsert ActiveWeight.

    Raises ValueError if recommendation not found or already processed.
    """
    rec = db.query(WeightRecommendation).filter(WeightRecommendation.id == rec_id).first()
    if rec is None:
        raise ValueError(f"Recommendation #{rec_id} not found")
    if rec.status != "pending":
        raise ValueError(f"Recommendation #{rec_id} is already {rec.status}")

    # Mark approved
    rec.status = "approved"
    rec.approved_at = datetime.utcnow()

    # Upsert ActiveWeight
    active = (
        db.query(ActiveWeight)
        .filter(ActiveWeight.strategy == rec.strategy)
        .first()
    )
    if active:
        active.weights = rec.recommended_weights
        active.version += 1
        active.activated_at = datetime.utcnow()
        active.recommendation_id = rec.id
    else:
        active = ActiveWeight(
            strategy=rec.strategy,
            weights=rec.recommended_weights,
            version=1,
            recommendation_id=rec.id,
        )
        db.add(active)

    db.commit()
    db.refresh(active)

    logger.info(
        "Applied recommendation #%d → ActiveWeight for %s (v%d)",
        rec.id, rec.strategy, active.version,
    )
    return active


def reject_recommendation(db: Session, rec_id: int, notes: str = "") -> WeightRecommendation:
    """Mark a recommendation as rejected with optional notes."""
    rec = db.query(WeightRecommendation).filter(WeightRecommendation.id == rec_id).first()
    if rec is None:
        raise ValueError(f"Recommendation #{rec_id} not found")
    if rec.status != "pending":
        raise ValueError(f"Recommendation #{rec_id} is already {rec.status}")

    rec.status = "rejected"
    rec.notes = notes
    db.commit()
    db.refresh(rec)
    return rec


def get_active_weights(db: Session, strategy: str) -> ActiveWeight | None:
    """
    Load learned ActiveWeight for a strategy.
    Returns None if no weights have been approved yet (caller should
    fall back to hardcoded defaults).
    """
    return (
        db.query(ActiveWeight)
        .filter(ActiveWeight.strategy == strategy)
        .first()
    )


def get_all_recommendations(
    db: Session,
    status: str | None = None,
) -> list[WeightRecommendation]:
    """List recommendations, optionally filtered by status."""
    query = db.query(WeightRecommendation).order_by(WeightRecommendation.id.desc())
    if status:
        query = query.filter(WeightRecommendation.status == status)
    return query.all()
