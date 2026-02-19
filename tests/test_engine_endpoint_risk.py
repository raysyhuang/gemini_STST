from app.engine_endpoint import _compute_momentum_risk_params


def test_momentum_risk_params_keep_min_rr():
    stop, target = _compute_momentum_risk_params(entry_price=100.0, atr_pct=12.0)
    assert stop is not None and target is not None
    risk = 100.0 - stop
    reward = target - 100.0
    assert risk > 0
    assert reward / risk >= 1.5
