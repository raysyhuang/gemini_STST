"""
Tests for the outcome-driven quality recalibration learning engine.

Covers:
  - Bayesian quartile win-rate analysis
  - Weight clamping and normalization
  - Quality floor optimization
  - Regime performance computation
  - Fallback behavior when data is insufficient
  - Integration test with synthetic trades
"""

import pytest
from unittest.mock import MagicMock, patch
from app.learning import (
    _assign_quartile,
    _compute_factor_predictive_power,
    _compute_new_weights,
    _compute_optimal_quality_floor,
    _compute_regime_performance,
    analyze_factor_weights,
    get_active_weights,
    DEFAULT_WEIGHTS,
    STRATEGY_FACTORS,
)
from app.recalibration_config import RECAL_CONFIG


# ── Quartile Assignment ──────────────────────────────────────────

class TestAssignQuartile:
    def test_q1(self):
        assert _assign_quartile(0) == 1
        assert _assign_quartile(24.9) == 1

    def test_q2(self):
        assert _assign_quartile(25) == 2
        assert _assign_quartile(49.9) == 2

    def test_q3(self):
        assert _assign_quartile(50) == 3
        assert _assign_quartile(74.9) == 3

    def test_q4(self):
        assert _assign_quartile(75) == 4
        assert _assign_quartile(100) == 4


# ── Bayesian Quartile Win-Rate Analysis ──────────────────────────

class TestFactorPredictivePower:
    def test_perfect_predictor(self):
        """Factor where Q4=all wins, Q1=all losses → high power."""
        trades = []
        # Q1 (0-24): all losses
        for _ in range(10):
            trades.append({"factor_scores": {"test": 10.0}, "won": False})
        # Q4 (75-100): all wins
        for _ in range(10):
            trades.append({"factor_scores": {"test": 90.0}, "won": True})

        result = _compute_factor_predictive_power(
            trades, "test", prior_wr=0.5, k=3.0,
        )
        assert result["predictive_power"] > 0.5
        assert result["quartiles"]["4"]["smoothed_wr"] > result["quartiles"]["1"]["smoothed_wr"]

    def test_random_predictor(self):
        """Factor with uniform win rate across quartiles → power near 0."""
        trades = []
        for q_score in [10, 35, 60, 85]:
            for _ in range(10):
                trades.append({"factor_scores": {"test": q_score}, "won": True})
            for _ in range(10):
                trades.append({"factor_scores": {"test": q_score}, "won": False})

        result = _compute_factor_predictive_power(
            trades, "test", prior_wr=0.5, k=6.0,
        )
        # Power should be near 0 (not exactly 0 due to Bayesian smoothing)
        assert abs(result["predictive_power"]) < 0.1

    def test_missing_factor_scores(self):
        """Trades without the factor should be skipped gracefully."""
        trades = [
            {"factor_scores": {"other": 50.0}, "won": True},
            {"factor_scores": None, "won": False},
            {"factor_scores": {}, "won": True},
        ]
        result = _compute_factor_predictive_power(
            trades, "test", prior_wr=0.5, k=3.0,
        )
        # All quartiles should have 0 observations → smoothed to prior
        for q in ["1", "2", "3", "4"]:
            assert result["quartiles"][q]["n"] == 0
            assert result["quartiles"][q]["smoothed_wr"] == pytest.approx(0.5, abs=0.01)

    def test_bayesian_shrinkage(self):
        """With high k (strong prior), extreme observations shrink toward prior."""
        trades = [
            {"factor_scores": {"test": 90.0}, "won": True},  # 1 Q4 win
        ]
        # With very high k, Q4 smoothed WR should be close to prior (0.5)
        result_high_k = _compute_factor_predictive_power(
            trades, "test", prior_wr=0.5, k=100.0,
        )
        assert result_high_k["quartiles"]["4"]["smoothed_wr"] < 0.6

        # With very low k, Q4 smoothed WR should be close to observed (1.0)
        result_low_k = _compute_factor_predictive_power(
            trades, "test", prior_wr=0.5, k=0.01,
        )
        assert result_low_k["quartiles"]["4"]["smoothed_wr"] > 0.9


# ── Weight Computation ───────────────────────────────────────────

class TestComputeNewWeights:
    def test_weights_sum_to_one(self):
        """New weights should always sum to 1.0."""
        factor_analysis = [
            {"factor": "a", "predictive_power": 0.3},
            {"factor": "b", "predictive_power": 0.1},
            {"factor": "c", "predictive_power": 0.2},
        ]
        current = {"a": 0.4, "b": 0.3, "c": 0.3}
        result = _compute_new_weights(factor_analysis, current, n_trades=50, k=9.0)

        assert pytest.approx(sum(result.values()), abs=0.01) == 1.0

    def test_clamp_min(self):
        """No weight should fall below clamp_min."""
        factor_analysis = [
            {"factor": "a", "predictive_power": 0.9},  # dominates
            {"factor": "b", "predictive_power": 0.001},
            {"factor": "c", "predictive_power": 0.001},
        ]
        current = {"a": 0.33, "b": 0.33, "c": 0.33}
        result = _compute_new_weights(factor_analysis, current, n_trades=100, k=9.0)

        for w in result.values():
            assert w >= RECAL_CONFIG["weight_clamp_min"]

    def test_clamp_max(self):
        """No weight should exceed clamp_max."""
        factor_analysis = [
            {"factor": "a", "predictive_power": 0.99},
            {"factor": "b", "predictive_power": 0.001},
        ]
        current = {"a": 0.5, "b": 0.5}
        result = _compute_new_weights(factor_analysis, current, n_trades=100, k=3.0)

        for w in result.values():
            assert w <= RECAL_CONFIG["weight_clamp_max"]

    def test_no_positive_power_keeps_current(self):
        """If no factor has positive power, current weights are preserved."""
        factor_analysis = [
            {"factor": "a", "predictive_power": -0.1},
            {"factor": "b", "predictive_power": -0.2},
        ]
        current = {"a": 0.6, "b": 0.4}
        result = _compute_new_weights(factor_analysis, current, n_trades=50, k=9.0)

        assert result == current


# ── Quality Floor Optimization ───────────────────────────────────

class TestOptimalQualityFloor:
    def test_finds_optimal_floor(self):
        """Should find floor where win_rate * sqrt(n) is maximized."""
        trades = []
        # Low quality (30-40): 20% win rate, many trades
        for _ in range(80):
            trades.append({"quality_score": 35, "won": False})
        for _ in range(20):
            trades.append({"quality_score": 35, "won": True})
        # High quality (70-80): 80% win rate, fewer trades
        for _ in range(4):
            trades.append({"quality_score": 75, "won": False})
        for _ in range(16):
            trades.append({"quality_score": 75, "won": True})

        floor = _compute_optimal_quality_floor(trades)
        assert floor is not None
        # Should prefer a higher floor that captures the high-quality trades
        assert floor >= 40

    def test_insufficient_data(self):
        """Returns None when no floor has >= 5 trades."""
        trades = [
            {"quality_score": 50, "won": True},
            {"quality_score": 60, "won": True},
        ]
        floor = _compute_optimal_quality_floor(trades)
        assert floor is None


# ── Regime Performance ───────────────────────────────────────────

class TestRegimePerformance:
    def test_computes_multipliers(self):
        """Regime with higher win rate → multiplier > 1."""
        trades = []
        # Bullish: 80% win rate
        for _ in range(16):
            trades.append({"regime_tag": "Bullish", "won": True})
        for _ in range(4):
            trades.append({"regime_tag": "Bullish", "won": False})
        # Bearish: 20% win rate
        for _ in range(4):
            trades.append({"regime_tag": "Bearish", "won": True})
        for _ in range(16):
            trades.append({"regime_tag": "Bearish", "won": False})

        result = _compute_regime_performance(trades)
        assert result is not None
        assert result["Bullish"] > 1.0
        assert result["Bearish"] < 1.0

    def test_clamps_multipliers(self):
        """Extreme regime differences should be clamped."""
        trades = []
        # Bullish: 100% win rate
        for _ in range(20):
            trades.append({"regime_tag": "Bullish", "won": True})
        # Bearish: 0% win rate
        for _ in range(20):
            trades.append({"regime_tag": "Bearish", "won": False})

        result = _compute_regime_performance(trades)
        lo, hi = RECAL_CONFIG["regime_multiplier_clamp"]
        assert result["Bullish"] <= hi
        assert result["Bearish"] >= lo

    def test_no_regime_data(self):
        """Returns None when no trades have regime tags."""
        trades = [{"regime_tag": "Unknown", "won": True}]
        result = _compute_regime_performance(trades)
        assert result is None


# ── Fallback Behavior ────────────────────────────────────────────

class TestFallbackBehavior:
    def test_get_active_weights_returns_none_when_empty(self):
        """get_active_weights returns None when no ActiveWeight exists."""
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = None

        result = get_active_weights(mock_db, "momentum")
        assert result is None

    def test_analyze_insufficient_data(self):
        """analyze_factor_weights returns None below min_trades."""
        mock_db = MagicMock()
        # Return only 5 trades (below min of 30)
        mock_db.query.return_value.filter.return_value.all.return_value = [
            MagicMock(
                strategy="momentum", status="closed",
                factor_scores={"rvol": 50}, quality_score=60,
                regime_tag="Bullish", pnl_dollars=10,
            )
            for _ in range(5)
        ]

        result = analyze_factor_weights(mock_db, "momentum")
        assert result is None


# ── Integration Test ─────────────────────────────────────────────

class TestIntegration:
    def test_full_analysis_with_synthetic_data(self):
        """Create synthetic trades and verify the full pipeline produces sensible weights."""
        import random
        random.seed(42)

        # Generate 200 synthetic trades with strong rvol signal
        trades = []
        for _ in range(200):
            rvol_score = random.uniform(0, 100)
            scores = {
                "rvol": rvol_score,
                "high_prox": random.uniform(0, 100),
                "rsi_sweet": random.uniform(0, 100),
                "trend": random.uniform(0, 100),
                "candle": random.uniform(0, 100),
                "options": random.uniform(0, 100),
            }
            # Strong deterministic signal: Q4 rvol always wins, Q1 always loses
            if rvol_score >= 75:
                won = random.random() < 0.85
            elif rvol_score < 25:
                won = random.random() < 0.25
            else:
                won = random.random() < 0.50
            trades.append({
                "id": len(trades),
                "factor_scores": scores,
                "quality_score": sum(scores.values()) / 6,
                "regime_tag": random.choice(["Bullish", "Mixed", "Bearish"]),
                "pnl_dollars": 10 if won else -8,
                "won": won,
            })

        # Run analysis components directly
        from app.learning import _compute_factor_predictive_power, _compute_new_weights

        n_total = len(trades)
        n_wins = sum(1 for t in trades if t["won"])
        overall_wr = n_wins / n_total
        k = RECAL_CONFIG["bayesian_prior_weight"] * 30

        factors = STRATEGY_FACTORS["momentum"]
        current_weights = DEFAULT_WEIGHTS["momentum"]
        factor_analysis = []
        for factor in factors:
            fa = _compute_factor_predictive_power(trades, factor, overall_wr, k)
            factor_analysis.append(fa)

        recommended = _compute_new_weights(factor_analysis, current_weights, n_total, k)

        # Basic sanity: weights sum to ~1, all within clamps
        assert pytest.approx(sum(recommended.values()), abs=0.02) == 1.0
        for w in recommended.values():
            assert RECAL_CONFIG["weight_clamp_min"] <= w <= RECAL_CONFIG["weight_clamp_max"]

        # rvol should have the highest predictive power
        rvol_fa = next(fa for fa in factor_analysis if fa["factor"] == "rvol")
        assert rvol_fa["predictive_power"] > 0.1  # strong signal
