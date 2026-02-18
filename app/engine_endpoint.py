"""Engine endpoint — standardized /api/engine/results for Gemini STST.

Queries screener_signals and reversion_signals from Postgres,
maps to EngineResultPayload contract.

Also provides /api/pipeline/run POST for authenticated daily trigger.
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime
from threading import Lock
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Header, BackgroundTasks
from pydantic import BaseModel
from typing import Optional

from app.database import SessionLocal
from app.models import Ticker, ScreenerSignal, ReversionSignal

logger = logging.getLogger(__name__)

router = APIRouter(tags=["engine"])
_pipeline_state_lock = Lock()
_pipeline_state: dict = {
    "status": "idle",  # idle | running | succeeded | failed
    "run_id": None,
    "started_at": None,
    "finished_at": None,
    "error": None,
}


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


def _run_pipeline_job(run_id: str) -> None:
    """Execute the daily screeners outside the request lifecycle."""
    with _pipeline_state_lock:
        _pipeline_state["status"] = "running"
        _pipeline_state["run_id"] = run_id
        _pipeline_state["started_at"] = datetime.utcnow().isoformat()
        _pipeline_state["finished_at"] = None
        _pipeline_state["error"] = None

    try:
        from app.screener import run_screener
        from app.mean_reversion import run_reversion_screener

        run_screener()
        run_reversion_screener()

        with _pipeline_state_lock:
            _pipeline_state["status"] = "succeeded"
            _pipeline_state["finished_at"] = datetime.utcnow().isoformat()
            _pipeline_state["error"] = None
        logger.info("Pipeline job %s completed successfully", run_id)
    except Exception as e:
        with _pipeline_state_lock:
            _pipeline_state["status"] = "failed"
            _pipeline_state["finished_at"] = datetime.utcnow().isoformat()
            _pipeline_state["error"] = str(e)
        logger.error("Pipeline job %s failed: %s", run_id, e, exc_info=True)


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


@router.post("/api/pipeline/run", status_code=202)
async def trigger_pipeline(
    background_tasks: BackgroundTasks,
    x_engine_key: Optional[str] = Header(None),
):
    """Trigger the full screening pipeline (authenticated).

    Called by GitHub Actions cron job to run the daily screening.
    """
    expected_key = os.environ.get("ENGINE_API_KEY", "")
    if expected_key and x_engine_key != expected_key:
        raise HTTPException(403, "Invalid API key")

    with _pipeline_state_lock:
        if _pipeline_state["status"] == "running":
            return {
                "status": "accepted",
                "message": "Pipeline already running",
                "run_id": _pipeline_state["run_id"],
                "date": str(date.today()),
            }

    run_id = f"gem-{uuid4().hex[:8]}"
    background_tasks.add_task(_run_pipeline_job, run_id)
    logger.info("Accepted pipeline run request: %s", run_id)
    return {
        "status": "accepted",
        "message": "Pipeline scheduled",
        "run_id": run_id,
        "date": str(date.today()),
    }


@router.get("/api/pipeline/status")
async def pipeline_status():
    """Return last known pipeline execution state."""
    with _pipeline_state_lock:
        return dict(_pipeline_state)
