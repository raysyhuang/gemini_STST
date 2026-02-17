"""Engine endpoint — standardized /api/engine/results for Gemini STST.

Queries screener_signals and reversion_signals from Postgres,
maps to EngineResultPayload contract.

Also provides /api/pipeline/run POST for authenticated daily trigger.
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime

from fastapi import APIRouter, HTTPException, Header
from pydantic import BaseModel
from typing import Optional

from app.database import SessionLocal
from app.models import Ticker, ScreenerSignal, ReversionSignal

logger = logging.getLogger(__name__)

router = APIRouter(tags=["engine"])


class EnginePick(BaseModel):
    ticker: str
    strategy: str
    entry_price: float
    stop_loss: float | None = None
    target_price: float | None = None
    confidence: float
    holding_period_days: int
    thesis: str | None = None
    risk_factors: list[str] = []
    raw_score: float | None = None
    metadata: dict = {}


class EngineResultPayload(BaseModel):
    engine_name: str
    engine_version: str
    run_date: str
    run_timestamp: str
    regime: str | None = None
    picks: list[EnginePick]
    candidates_screened: int
    pipeline_duration_s: float | None = None
    status: str = "success"


def _get_regime_label(db) -> str:
    """Get current market regime from DB."""
    try:
        from app.main import _get_market_regime
        regime = _get_market_regime(db)
        return regime.get("regime", "Unknown").lower()
    except Exception:
        return None


@router.get("/api/engine/results")
async def get_engine_results():
    """Return today's screening results in standardized format."""
    db = SessionLocal()
    try:
        today = date.today()
        picks: list[dict] = []

        # Momentum signals
        momentum_query = (
            db.query(ScreenerSignal, Ticker)
            .join(Ticker, ScreenerSignal.ticker_id == Ticker.id)
            .filter(ScreenerSignal.date == today)
            .order_by(ScreenerSignal.quality_score.desc().nullslast())
            .all()
        )

        for signal, ticker in momentum_query:
            confidence = signal.quality_score or 50.0  # quality_score is 0-100, use directly
            picks.append(EnginePick(
                ticker=ticker.symbol,
                strategy="momentum",
                entry_price=signal.trigger_price or 0,
                stop_loss=None,  # Gemini STST uses trailing stop, not fixed
                target_price=round(signal.trigger_price * 1.10, 2) if signal.trigger_price else None,
                confidence=confidence,
                holding_period_days=10,  # Tuned momentum hold
                thesis=f"RVOL={signal.rvol_at_trigger:.1f}x, ATR%={signal.atr_pct_at_trigger:.1f}%"
                if signal.rvol_at_trigger and signal.atr_pct_at_trigger
                else None,
                risk_factors=[],
                raw_score=signal.quality_score,
                metadata={
                    "rvol": signal.rvol_at_trigger,
                    "atr_pct": signal.atr_pct_at_trigger,
                    "rsi_14": signal.rsi_14,
                    "options_sentiment": signal.options_sentiment,
                    "confluence": signal.confluence,
                },
            ))

        # Reversion signals
        reversion_query = (
            db.query(ReversionSignal, Ticker)
            .join(Ticker, ReversionSignal.ticker_id == Ticker.id)
            .filter(ReversionSignal.date == today)
            .order_by(ReversionSignal.quality_score.desc().nullslast())
            .all()
        )

        for signal, ticker in reversion_query:
            confidence = signal.quality_score or 50.0
            picks.append(EnginePick(
                ticker=ticker.symbol,
                strategy="mean_reversion",
                entry_price=signal.trigger_price or 0,
                stop_loss=round(signal.trigger_price * 0.95, 2) if signal.trigger_price else None,
                target_price=round(signal.trigger_price * 1.10, 2) if signal.trigger_price else None,
                confidence=confidence,
                holding_period_days=3,  # Tuned reversion hold
                thesis=f"RSI2={signal.rsi2_at_trigger:.1f}, DD3d={signal.drawdown_3d_pct:.1f}%"
                if signal.rsi2_at_trigger and signal.drawdown_3d_pct
                else None,
                risk_factors=[],
                raw_score=signal.quality_score,
                metadata={
                    "rsi2": signal.rsi2_at_trigger,
                    "drawdown_3d_pct": signal.drawdown_3d_pct,
                    "sma_distance_pct": signal.sma_distance_pct,
                    "options_sentiment": signal.options_sentiment,
                    "confluence": signal.confluence,
                },
            ))

        regime = _get_regime_label(db)
        total_screened = len(momentum_query) + len(reversion_query)

        return EngineResultPayload(
            engine_name="gemini_stst",
            engine_version="7.0",
            run_date=str(today),
            run_timestamp=datetime.utcnow().isoformat(),
            regime=regime,
            picks=picks,
            candidates_screened=total_screened,
            status="success",
        )
    finally:
        db.close()


@router.post("/api/pipeline/run")
async def trigger_pipeline(
    x_engine_key: Optional[str] = Header(None),
):
    """Trigger the full screening pipeline (authenticated).

    Called by GitHub Actions cron job to run the daily screening.
    """
    expected_key = os.environ.get("ENGINE_API_KEY", "")
    if expected_key and x_engine_key != expected_key:
        raise HTTPException(403, "Invalid API key")

    try:
        from app.screener import run_screener
        from app.mean_reversion import run_mean_reversion_screener

        db = SessionLocal()
        try:
            # Run both screeners
            momentum_count = run_screener(db)
            reversion_count = run_mean_reversion_screener(db)

            return {
                "status": "success",
                "momentum_signals": momentum_count,
                "reversion_signals": reversion_count,
                "date": str(date.today()),
            }
        finally:
            db.close()

    except Exception as e:
        logger.error("Pipeline run failed: %s", e, exc_info=True)
        raise HTTPException(500, f"Pipeline failed: {e}")
