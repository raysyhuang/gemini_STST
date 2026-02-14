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
    ScreenerResponse,
    SignalResponse,
)
from app.news_fetcher import fetch_news
# NOTE: backtester import is LAZY to avoid vectorbt/plotly loading at boot
# (prevents Heroku H20 boot timeout). Imported inside the endpoint handler.

logger = logging.getLogger(__name__)

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


@app.get("/api/backtest/{ticker}", response_model=BacktestResultResponse)
async def backtest_ticker(ticker: str):
    """
    Run (or retrieve) the VectorBT backtest for a single ticker.
    Returns win rate, profit factor, max drawdown, and the equity curve
    formatted for TradingView Lightweight Charts.
    """
    symbol = ticker.upper()

    # Verify the ticker exists
    db = SessionLocal()
    try:
        tkr = db.query(Ticker).filter(Ticker.symbol == symbol).first()
        if not tkr:
            raise HTTPException(status_code=404, detail=f"Ticker '{symbol}' not found")
    finally:
        db.close()

    # Lazy import to avoid vectorbt/plotly loading at boot time
    from app.backtester import run_single_ticker_backtest

    # Run the backtest (CPU-bound, offload to thread)
    result = await asyncio.to_thread(run_single_ticker_backtest, symbol)

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
