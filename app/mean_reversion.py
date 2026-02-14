"""
Mean Reversion (Oversold Bounce) screener.

Applies the following filter chain to the latest market data:
  1. Price  > $5.00
  2. ADV    > 1,500,000  (20-day average daily volume)
  3. RSI(2) < 10         (deeply oversold on 2-period RSI)
  4. 3-Day Drawdown >= 15%  (stock fell at least 15% over 3 sessions)
  5. Close  > SMA-200       (long-term uptrend intact — not a broken stock)

The strategy targets rubber-band snaps: quality names that fell hard
and fast into short-term oversold territory, with the expectation
of a mean-reversion bounce within 3-5 days.
"""

import gc
import logging
from datetime import date, timedelta

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import Ticker, DailyMarketData, ReversionSignal
from app.indicators import compute_rsi, compute_sma, compute_adv

logger = logging.getLogger(__name__)

# Filter thresholds
MIN_PRICE = 5.0
MIN_ADV = 1_500_000
MAX_RSI2 = 10.0
MIN_DRAWDOWN_3D = 0.15   # 15% decline over 3 sessions

# Need at least 200 trading days for SMA-200; load extra buffer
LOOKBACK_CALENDAR_DAYS = 300


def _load_all_ohlcv(db: Session, ticker_ids: list[int], since: date) -> pd.DataFrame:
    """Batch-load OHLCV for ALL ticker_ids in a single SQL query."""
    if not ticker_ids:
        return pd.DataFrame()

    stmt = text("""
        SELECT ticker_id, date, open, high, low, close, volume
        FROM daily_market_data
        WHERE ticker_id = ANY(:ids)
          AND date >= :since
        ORDER BY ticker_id, date ASC
    """)
    result = db.execute(stmt, {"ids": ticker_ids, "since": since})
    rows = result.fetchall()
    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(
        rows,
        columns=["ticker_id", "date", "open", "high", "low", "close", "volume"],
    )


def _save_reversion_signals(db: Session, signals: list[dict]) -> None:
    """Upsert reversion signals into Postgres."""
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    if not signals:
        return

    values = [
        {
            "ticker_id": s["ticker_id"],
            "date": s["date"],
            "trigger_price": s["trigger_price"],
            "rsi2_at_trigger": s["rsi2"],
            "drawdown_3d_pct": s["drawdown_3d_pct"],
            "sma_distance_pct": s["sma_distance_pct"],
            "options_sentiment": s.get("options_sentiment"),
            "put_call_ratio": s.get("put_call_ratio"),
        }
        for s in signals
    ]

    stmt = pg_insert(ReversionSignal).values(values)
    stmt = stmt.on_conflict_do_update(
        constraint="uq_reversion_signal_ticker_date",
        set_={
            "trigger_price": stmt.excluded.trigger_price,
            "rsi2_at_trigger": stmt.excluded.rsi2_at_trigger,
            "drawdown_3d_pct": stmt.excluded.drawdown_3d_pct,
            "sma_distance_pct": stmt.excluded.sma_distance_pct,
            "options_sentiment": stmt.excluded.options_sentiment,
            "put_call_ratio": stmt.excluded.put_call_ratio,
        },
    )
    db.execute(stmt)
    db.commit()
    logger.info("Saved %d reversion signals to Postgres", len(values))


def run_reversion_screener(screen_date: date | None = None) -> dict:
    """
    Execute the mean-reversion screener for a given date.

    Returns:
        {
            "date": date,
            "signals": [
                {
                    "ticker_id": int,
                    "symbol": str,
                    "company_name": str,
                    "date": date,
                    "trigger_price": float,
                    "rsi2": float,
                    "drawdown_3d_pct": float,
                    "sma_distance_pct": float,
                },
                ...
            ],
        }
    """
    if screen_date is None:
        screen_date = date.today()

    lookback_start = screen_date - timedelta(days=LOOKBACK_CALENDAR_DAYS)

    db = SessionLocal()
    try:
        # Load all active tickers
        all_tickers = db.query(Ticker).filter(Ticker.is_active.is_(True)).all()
        ticker_map = {t.id: t for t in all_tickers}
        ticker_ids = list(ticker_map.keys())
        logger.info("Reversion screener: %d active tickers for %s", len(all_tickers), screen_date)

        # Batch load ALL OHLCV in one query
        all_ohlcv = _load_all_ohlcv(db, ticker_ids, lookback_start)
        logger.info("Loaded %d OHLCV rows for reversion screener", len(all_ohlcv))

    except Exception:
        db.close()
        raise

    if all_ohlcv.empty:
        return {"date": screen_date, "signals": []}

    # Screen each ticker using in-memory grouped data
    signals: list[dict] = []

    for tid, group_df in all_ohlcv.groupby("ticker_id"):
        tkr = ticker_map.get(tid)
        if tkr is None:
            continue

        # Need at least 200 rows for SMA-200
        if len(group_df) < 200:
            continue

        df = group_df[["date", "open", "high", "low", "close", "volume"]].copy()
        df = df.sort_values("date").reset_index(drop=True)

        # Compute indicators
        df["rsi2"] = compute_rsi(df, period=2)
        df["sma_200"] = compute_sma(df, column="close", period=200)
        df["adv_20"] = compute_adv(df, period=20)

        # 3-day drawdown: (close today / close 3 days ago) - 1
        df["close_3d_ago"] = df["close"].shift(3)
        df["drawdown_3d"] = (df["close"] / df["close_3d_ago"]) - 1.0

        latest = df.iloc[-1]

        # Make sure the latest row is near the screen_date
        if (screen_date - latest["date"]).days > 5:
            continue

        # --- Apply filter chain ---

        # 1. Price > $5
        if latest["close"] <= MIN_PRICE:
            continue

        # 2. ADV > 1.5M
        if pd.isna(latest["adv_20"]) or latest["adv_20"] <= MIN_ADV:
            continue

        # 3. RSI(2) < 10
        if pd.isna(latest["rsi2"]) or latest["rsi2"] >= MAX_RSI2:
            continue

        # 4. 3-day drawdown >= 15%
        if pd.isna(latest["drawdown_3d"]) or latest["drawdown_3d"] > -MIN_DRAWDOWN_3D:
            continue

        # 5. Close > SMA-200 (long-term uptrend intact)
        if pd.isna(latest["sma_200"]) or latest["close"] <= latest["sma_200"]:
            continue

        # SMA distance: how far below the 20-day SMA (rubber-band stretch)
        sma_20 = df["close"].rolling(20).mean().iloc[-1]
        sma_distance_pct = round(((latest["close"] / sma_20) - 1.0) * 100, 1) if not pd.isna(sma_20) else 0.0

        signals.append({
            "ticker_id": tkr.id,
            "symbol": tkr.symbol,
            "company_name": tkr.company_name,
            "date": latest["date"],
            "trigger_price": round(float(latest["close"]), 2),
            "rsi2": round(float(latest["rsi2"]), 1),
            "drawdown_3d_pct": round(float(latest["drawdown_3d"]) * 100, 1),
            "sma_distance_pct": sma_distance_pct,
        })

    logger.info("Reversion screener found %d signals on %s", len(signals), screen_date)

    # Sort by RSI ascending (most oversold first)
    signals.sort(key=lambda s: s["rsi2"])

    # Persist signals to Postgres
    try:
        _save_reversion_signals(db, signals)
    finally:
        db.close()

    gc.collect()

    return {
        "date": screen_date,
        "signals": signals,
    }
