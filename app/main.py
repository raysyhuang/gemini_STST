"""
QuantScreener — FastAPI application entry point.

Endpoints:
  GET /api/screener/today   → Today's momentum signals + Finnhub news
  GET /api/backtest/{ticker} → VectorBT backtest results + equity curve

Static files:
  /static/*  → serves static/
  /          → serves static/index.html
"""

import asyncio
import logging
import threading
from contextlib import asynccontextmanager
from datetime import date
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.database import init_db, SessionLocal
from app.models import Ticker, ScreenerSignal
from app.schemas import (
    BacktestResultResponse,
    MarketRegimeResponse,
    NewsArticle,
    ReversionScreenerResponse,
    ReversionSignalResponse,
    ScreenerResponse,
    SignalResponse,
)
from app.news_fetcher import fetch_news

logger = logging.getLogger(__name__)

# Pre-warm vectorbt import in a background thread so the app boots fast
# (avoids H20 boot timeout) but the module is ready before first request.
_vbt_ready = threading.Event()


def _preload_vectorbt():
    import app.backtester  # noqa: F401 – triggers vectorbt/plotly import
    _vbt_ready.set()
    logger.info("vectorbt pre-loaded in background thread.")


threading.Thread(target=_preload_vectorbt, daemon=True).start()

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"


# ------------------------------------------------------------------
# Lifespan: run init_db once at startup
# ------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    logger.info("Database tables verified.")
    yield


app = FastAPI(
    title="QuantScreener API",
    version="1.0.0",
    lifespan=lifespan,
)

# -- CORS (allow the JS frontend to call the API) --
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -- Static files --
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------

@app.get("/")
async def root():
    """Serve the frontend dashboard."""
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/screener/today", response_model=ScreenerResponse)
async def screener_today():
    """
    Return today's screener signals from Postgres, enriched with
    the 3 most recent Finnhub news headlines per ticker.
    """
    db = SessionLocal()
    try:
        today = date.today()

        # Pull today's signals joined with ticker info
        rows = (
            db.query(ScreenerSignal, Ticker)
            .join(Ticker, ScreenerSignal.ticker_id == Ticker.id)
            .filter(ScreenerSignal.date == today)
            .order_by(ScreenerSignal.atr_pct_at_trigger.desc())
            .all()
        )

        # Build signal list
        signals: list[dict] = []
        for signal, ticker in rows:
            signals.append({
                "ticker": ticker.symbol,
                "company_name": ticker.company_name or "",
                "date": signal.date,
                "trigger_price": signal.trigger_price,
                "rvol_at_trigger": signal.rvol_at_trigger,
                "atr_pct_at_trigger": signal.atr_pct_at_trigger,
                "options_sentiment": signal.options_sentiment,
                "put_call_ratio": signal.put_call_ratio,
                "news": [],  # populated below
            })

        # Determine market regime from DB data
        regime = _get_market_regime(db)

    finally:
        db.close()

    # -- Enrich with Finnhub news (async, concurrent) --
    if signals:
        news_tasks = [fetch_news(s["ticker"], limit=3) for s in signals]
        news_results = await asyncio.gather(*news_tasks)
        for sig, articles in zip(signals, news_results):
            sig["news"] = articles

    return ScreenerResponse(
        date=today,
        regime=MarketRegimeResponse(**regime),
        signals=[SignalResponse(**s) for s in signals],
    )


@app.get("/api/reversion/today", response_model=ReversionScreenerResponse)
async def reversion_today():
    """
    Return today's mean-reversion (oversold bounce) signals.
    Criteria: RSI(2) < 10, 3-day drawdown >= 15%, Close > SMA-200.
    """
    from app.mean_reversion import run_reversion_screener

    result = await asyncio.to_thread(run_reversion_screener)

    return ReversionScreenerResponse(
        date=result["date"],
        signals=[ReversionSignalResponse(
            ticker=s["symbol"],
            company_name=s.get("company_name", ""),
            date=s["date"],
            trigger_price=s["trigger_price"],
            rsi2=s["rsi2"],
            drawdown_3d_pct=s["drawdown_3d_pct"],
            sma_distance_pct=s["sma_distance_pct"],
            options_sentiment=s.get("options_sentiment"),
            put_call_ratio=s.get("put_call_ratio"),
        ) for s in result["signals"]],
    )


@app.get("/api/backtest/{ticker}", response_model=BacktestResultResponse)
async def backtest_ticker(ticker: str, strategy: str = "momentum"):
    """
    Run (or retrieve) the VectorBT backtest for a single ticker.

    Query params:
      - strategy: "momentum" (default) or "reversion"

    Returns win rate, profit factor, max drawdown, and the equity curve
    formatted for TradingView Lightweight Charts.
    """
    symbol = ticker.upper()
    if strategy not in ("momentum", "reversion"):
        raise HTTPException(status_code=400, detail="strategy must be 'momentum' or 'reversion'")

    # Verify the ticker exists
    db = SessionLocal()
    try:
        tkr = db.query(Ticker).filter(Ticker.symbol == symbol).first()
        if not tkr:
            raise HTTPException(status_code=404, detail=f"Ticker '{symbol}' not found")
    finally:
        db.close()

    # Wait for the background vectorbt pre-load to finish (max 120s)
    if not _vbt_ready.wait(timeout=120):
        raise HTTPException(status_code=503, detail="Backtester is still loading, try again shortly")

    from app.backtester import run_single_ticker_backtest

    # Run the backtest (CPU-bound, offload to thread)
    result = await asyncio.to_thread(
        run_single_ticker_backtest, symbol, strategy_type=strategy,
    )

    if result is None:
        raise HTTPException(
            status_code=404,
            detail=f"Insufficient data to backtest '{symbol}'",
        )

    return BacktestResultResponse(**result)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _get_market_regime(db) -> dict:
    """Load SPY + QQQ recent data from DB and compute regime."""
    from app.models import DailyMarketData
    from datetime import timedelta
    import pandas as pd

    regime = {"spy_above_sma20": None, "qqq_above_sma20": None, "regime": "Unknown"}
    cutoff = date.today() - timedelta(days=60)

    for symbol, key in [("SPY", "spy_above_sma20"), ("QQQ", "qqq_above_sma20")]:
        tkr = db.query(Ticker).filter(Ticker.symbol == symbol).first()
        if not tkr:
            continue
        rows = (
            db.query(DailyMarketData)
            .filter(DailyMarketData.ticker_id == tkr.id, DailyMarketData.date >= cutoff)
            .order_by(DailyMarketData.date.asc())
            .all()
        )
        if len(rows) < 20:
            continue
        closes = pd.Series([r.close for r in rows])
        sma20 = closes.rolling(20).mean().iloc[-1]
        regime[key] = bool(closes.iloc[-1] > sma20)

    spy = regime["spy_above_sma20"]
    qqq = regime["qqq_above_sma20"]
    if spy is True and qqq is True:
        regime["regime"] = "Bullish"
    elif spy is False and qqq is False:
        regime["regime"] = "Bearish"
    elif spy is not None and qqq is not None:
        regime["regime"] = "Mixed"

    return regime
