"""
Configuration for the outcome-driven quality recalibration system.

All tunable parameters for Bayesian factor analysis, weight clamping,
quality floor optimization, and signal decay.
"""

RECAL_CONFIG = {
    # Minimum closed trades with factor_scores before analysis is valid
    "min_observations": {"momentum": 30, "reversion": 20},

    # Bayesian shrinkage: k = bayesian_prior_weight * min_observations
    "bayesian_prior_weight": 0.3,

    # Weight clamps: prevent degenerate all-in-one-factor solutions
    "weight_clamp_min": 0.05,
    "weight_clamp_max": 0.50,

    # Quality floor search: test floors in this range to maximize
    # win_rate * trade_count balance
    "quality_floor_search_range": (40, 80),
    "quality_floor_step": 5,

    # Regime multiplier clamps: learned multipliers bounded to this range
    "regime_multiplier_clamp": (0.25, 1.25),

    # Signal decay: cancel stale pending trades
    "signal_decay": {"enabled": True, "max_pending_days": 3},

    # Automation gates
    "auto_analyze": True,    # auto-run analysis after sufficient data
    "auto_apply": False,     # human gate — never auto-apply weights
}
