"""
Daily momentum screener.

Applies the QuantScreener filter chain to the latest market data:
  1. Price  > $5.00
  2. ADV    > 1,500,000  (20-day average daily volume)
  3. ATR%   > 8%         (projected weekly volatility)
  4. RVOL   > 2.0        (relative volume vs 20-day average)

Also checks the SPY/QQQ market regime and flags a Bearish warning.
Results are written to the screener_signals table in Postgres.
"""

import logging
from datetime import date, timedelta

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import Ticker, DailyMarketData, ScreenerSignal
from app.indicators import add_all_indicators, check_market_regime
from app.data_fetcher import fetch_ohlcv_batch, upsert_tickers, bulk_upsert_ohlcv

logger = logging.getLogger(__name__)

# PRD filter thresholds
MIN_PRICE = 5.0
MIN_ADV = 1_500_000
MIN_ATR_PCT = 8.0
MIN_RVOL = 2.0

# We need at least 30 trading days of history to compute 20-day indicators reliably
LOOKBACK_CALENDAR_DAYS = 60


def _load_ohlcv_for_ticker(db: Session, ticker_id: int, since: date) -> pd.DataFrame:
    """Pull OHLCV rows for a single ticker from Postgres into a DataFrame."""
    rows = (
        db.query(DailyMarketData)
        .filter(
            DailyMarketData.ticker_id == ticker_id,
            DailyMarketData.date >= since,
        )
        .order_by(DailyMarketData.date.asc())
        .all()
    )
    if not rows:
        return pd.DataFrame()

    data = [
        {
            "date": r.date,
            "open": r.open,
            "high": r.high,
            "low": r.low,
            "close": r.close,
            "volume": r.volume,
        }
        for r in rows
    ]
    return pd.DataFrame(data)


def run_screener(screen_date: date | None = None) -> dict:
    """
    Execute the full screener for a given date (defaults to today).

    Returns:
        {
            "date": date,
            "regime": { ... },
            "signals": [ { ticker, trigger_price, rvol, atr_pct }, ... ],
        }
    """
    import gc

    if screen_date is None:
        screen_date = date.today()

    lookback_start = screen_date - timedelta(days=LOOKBACK_CALENDAR_DAYS)

    db = SessionLocal()
    try:
        # --- Market Regime Check (SPY + QQQ) ---
        spy_ticker = db.query(Ticker).filter(Ticker.symbol == "SPY").first()
        qqq_ticker = db.query(Ticker).filter(Ticker.symbol == "QQQ").first()

        regime_info = {"regime": "Unknown", "spy_above_sma20": None, "qqq_above_sma20": None}
        if spy_ticker and qqq_ticker:
            spy_df = _load_ohlcv_for_ticker(db, spy_ticker.id, lookback_start)
            qqq_df = _load_ohlcv_for_ticker(db, qqq_ticker.id, lookback_start)
            if len(spy_df) >= 20 and len(qqq_df) >= 20:
                regime_info = check_market_regime(spy_df, qqq_df)

        if regime_info["regime"] == "Bearish":
            logger.warning("BEARISH REGIME detected — SPY & QQQ below 20-day SMA")

        # --- Screen all active tickers ---
        all_tickers = db.query(Ticker).filter(Ticker.is_active.is_(True)).all()
        logger.info("Screening %d active tickers for %s", len(all_tickers), screen_date)

        signals: list[dict] = []

        for tkr in all_tickers:
            df = _load_ohlcv_for_ticker(db, tkr.id, lookback_start)
            if df.empty or len(df) < 20:
                continue

            df = add_all_indicators(df)
            latest = df.iloc[-1]

            # Make sure the latest row is actually on or near the screen_date
            # (within a few days to handle weekends / holidays)
            if (screen_date - latest["date"]).days > 5:
                continue

            # --- Apply filter chain ---
            if latest["close"] <= MIN_PRICE:
                continue
            if pd.isna(latest["adv_20"]) or latest["adv_20"] <= MIN_ADV:
                continue
            if pd.isna(latest["atr_pct"]) or latest["atr_pct"] <= MIN_ATR_PCT:
                continue
            if pd.isna(latest["rvol"]) or latest["rvol"] <= MIN_RVOL:
                continue

            signals.append({
                "ticker_id": tkr.id,
                "symbol": tkr.symbol,
                "company_name": tkr.company_name,
                "date": latest["date"],
                "trigger_price": round(float(latest["close"]), 2),
                "rvol_at_trigger": round(float(latest["rvol"]), 2),
                "atr_pct_at_trigger": round(float(latest["atr_pct"]), 1),
            })

        logger.info("Screener found %d signals on %s", len(signals), screen_date)

        # --- Persist signals to Postgres ---
        _save_signals(db, signals)

    finally:
        db.close()
        gc.collect()

    return {
        "date": screen_date,
        "regime": regime_info,
        "signals": signals,
    }


def _save_signals(db: Session, signals: list[dict]) -> None:
    """Upsert screener signals into Postgres."""
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    if not signals:
        return

    values = [
        {
            "ticker_id": s["ticker_id"],
            "date": s["date"],
            "trigger_price": s["trigger_price"],
            "rvol_at_trigger": s["rvol_at_trigger"],
            "atr_pct_at_trigger": s["atr_pct_at_trigger"],
        }
        for s in signals
    ]

    stmt = pg_insert(ScreenerSignal).values(values)
    stmt = stmt.on_conflict_do_update(
        constraint="uq_signal_ticker_date",
        set_={
            "trigger_price": stmt.excluded.trigger_price,
            "rvol_at_trigger": stmt.excluded.rvol_at_trigger,
            "atr_pct_at_trigger": stmt.excluded.atr_pct_at_trigger,
        },
    )
    db.execute(stmt)
    db.commit()
    logger.info("Saved %d signals to Postgres", len(values))
