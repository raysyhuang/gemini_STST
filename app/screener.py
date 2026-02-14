"""
Daily momentum screener.

Applies the QuantScreener filter chain to the latest market data:
  1. Price  > $5.00
  2. ADV    > 1,500,000  (20-day average daily volume)
  3. ATR%   > 8%         (projected weekly volatility)
  4. RVOL   > 2.0        (relative volume vs 20-day average)
  5. Trend Alignment: Close > SMA_20  (don't buy falling knives)
  6. Green Candle: Close > Open       (buyers maintained control)

Also checks the SPY/QQQ market regime and flags a Bearish warning.
Results are written to the screener_signals table in Postgres.
"""

import logging
from datetime import date, timedelta

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import Ticker, DailyMarketData, ScreenerSignal
from app.indicators import add_all_indicators, check_market_regime

logger = logging.getLogger(__name__)

# PRD filter thresholds
MIN_PRICE = 5.0
MIN_ADV = 1_500_000
MIN_ATR_PCT = 8.0
MIN_RVOL = 2.0

# Confluence detection lookback
CONFLUENCE_LOOKBACK_DAYS = 14

# We need at least 30 trading days of history to compute 20-day indicators reliably
LOOKBACK_CALENDAR_DAYS = 60

# Signal cooldown: suppress repeats for 5 trading days
COOLDOWN_CALENDAR_DAYS = 7  # ~5 trading days


def _load_all_ohlcv(db: Session, ticker_ids: list[int], since: date) -> pd.DataFrame:
    """
    Batch-load OHLCV for ALL ticker_ids in a single SQL query.
    Returns a long-format DataFrame with columns:
        ticker_id, date, open, high, low, close, volume
    """
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


def _load_recent_signal_tickers(db: Session, since: date) -> set[int]:
    """
    Return the set of ticker_ids that already fired a signal
    within the cooldown window. Used for 5-day deduplication.
    """
    stmt = text("""
        SELECT DISTINCT ticker_id
        FROM screener_signals
        WHERE date >= :since
    """)
    result = db.execute(stmt, {"since": since})
    return {row[0] for row in result.fetchall()}


def run_screener(
    screen_date: date | None = None,
    earnings_blacklist: set[str] | None = None,
) -> dict:
    """
    Execute the full screener for a given date (defaults to today).

    Args:
        screen_date: The date to screen (defaults to today).
        earnings_blacklist: Set of symbols with earnings within the
            7-day hold window. These are skipped to avoid binary event risk.

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
    if earnings_blacklist is None:
        earnings_blacklist = set()

    lookback_start = screen_date - timedelta(days=LOOKBACK_CALENDAR_DAYS)
    cooldown_start = screen_date - timedelta(days=COOLDOWN_CALENDAR_DAYS)

    db = SessionLocal()
    try:
        # --- Load all active tickers ---
        all_tickers = db.query(Ticker).filter(Ticker.is_active.is_(True)).all()
        ticker_map = {t.id: t for t in all_tickers}
        ticker_ids = list(ticker_map.keys())
        logger.info("Screening %d active tickers for %s", len(all_tickers), screen_date)

        # --- P1 FIX: Batch load ALL OHLCV in one query ---
        all_ohlcv = _load_all_ohlcv(db, ticker_ids, lookback_start)
        logger.info("Loaded %d OHLCV rows in single batch query", len(all_ohlcv))

        # --- Market Regime Check (SPY + QQQ) ---
        regime_info = {"regime": "Unknown", "spy_above_sma20": None, "qqq_above_sma20": None}
        spy_tkr = next((t for t in all_tickers if t.symbol == "SPY"), None)
        qqq_tkr = next((t for t in all_tickers if t.symbol == "QQQ"), None)
        if spy_tkr and qqq_tkr and not all_ohlcv.empty:
            spy_df = all_ohlcv[all_ohlcv["ticker_id"] == spy_tkr.id].copy()
            qqq_df = all_ohlcv[all_ohlcv["ticker_id"] == qqq_tkr.id].copy()
            if len(spy_df) >= 20 and len(qqq_df) >= 20:
                regime_info = check_market_regime(spy_df, qqq_df)

        if regime_info["regime"] == "Bearish":
            logger.warning("BEARISH REGIME detected — SPY & QQQ below 20-day SMA")

        # --- P1 FIX: Load cooldown set (tickers that signaled recently) ---
        cooldown_tickers = _load_recent_signal_tickers(db, cooldown_start)
        logger.info("Cooldown: %d tickers signaled in last %d days",
                     len(cooldown_tickers), COOLDOWN_CALENDAR_DAYS)

        # --- Screen each ticker using in-memory grouped data ---
        signals: list[dict] = []
        skipped_cooldown = 0
        skipped_earnings = 0

        for tid, group_df in all_ohlcv.groupby("ticker_id"):
            tkr = ticker_map.get(tid)
            if tkr is None:
                continue

            # Need at least 20 rows for indicator computation
            if len(group_df) < 20:
                continue

            # Signal cooldown: skip if this ticker fired recently
            if tid in cooldown_tickers:
                skipped_cooldown += 1
                continue

            # Earnings blacklist: skip if earnings within hold window
            if tkr.symbol in earnings_blacklist:
                skipped_earnings += 1
                continue

            df = group_df[["date", "open", "high", "low", "close", "volume"]].copy()
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

            # 5. Trend Alignment: Close must be above SMA-20 (no falling knives)
            if pd.isna(latest["sma_20"]) or latest["close"] <= latest["sma_20"]:
                continue

            # 6. Green Candle: Close > Open (buyers maintained control today)
            if latest["close"] <= latest["open"]:
                continue

            # Compute momentum quality score (0-100)
            quality = _compute_momentum_quality(latest)

            signals.append({
                "ticker_id": tkr.id,
                "symbol": tkr.symbol,
                "company_name": tkr.company_name,
                "date": latest["date"],
                "trigger_price": round(float(latest["close"]), 2),
                "rvol_at_trigger": round(float(latest["rvol"]), 2),
                "atr_pct_at_trigger": round(float(latest["atr_pct"]), 1),
                "quality_score": quality,
                "confluence": False,  # set later by _detect_confluence
            })

        # Sort by quality score descending (strongest first)
        signals.sort(key=lambda s: s["quality_score"], reverse=True)

        logger.info(
            "Screener found %d signals on %s (skipped: %d cooldown, %d earnings)",
            len(signals), screen_date, skipped_cooldown, skipped_earnings,
        )

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


def _compute_momentum_quality(latest: pd.Series) -> float:
    """
    Compute a 0-100 composite quality score for a momentum signal.

    Components (weighted):
      - RVOL strength:   35%  (2.0 → 0, 5.0 → 100)
      - ATR% strength:   25%  (8% → 0, 20% → 100)
      - Trend strength:  20%  (close/SMA20 - 1: 0% → 0, 5% → 100)
      - Candle strength:  20%  (body%: 0% → 0, 3% → 100)
    """
    def _clamp(val):
        return max(0.0, min(100.0, val))

    rvol = float(latest["rvol"])
    atr_pct = float(latest["atr_pct"])
    close = float(latest["close"])
    open_ = float(latest["open"])
    sma20 = float(latest["sma_20"])

    rvol_score = _clamp((rvol - 2.0) / 3.0 * 100)
    atr_score = _clamp((atr_pct - 8.0) / 12.0 * 100)
    trend_score = _clamp(((close / sma20) - 1) / 0.05 * 100)
    candle_score = _clamp((close - open_) / open_ / 0.03 * 100)

    quality = (
        rvol_score * 0.35
        + atr_score * 0.25
        + trend_score * 0.20
        + candle_score * 0.20
    )
    return round(quality, 1)


def _detect_confluence(
    db: Session,
    momentum_signals: list[dict],
    reversion_signals: list[dict],
    lookback_days: int = CONFLUENCE_LOOKBACK_DAYS,
) -> None:
    """
    Flag signals whose ticker appears in both strategies recently.

    Mutates signal dicts in-place, setting confluence=True where a
    momentum signal ticker had a reversion signal in the last N days
    (or vice versa). This "bounce-to-breakout" pattern indicates
    high conviction.
    """
    cutoff = date.today() - timedelta(days=lookback_days)

    # Recent reversion tickers from DB
    recent_rev_rows = db.execute(
        text("SELECT DISTINCT ticker_id FROM reversion_signals WHERE date >= :since"),
        {"since": cutoff},
    )
    recent_rev = {row[0] for row in recent_rev_rows}

    # Recent momentum tickers from DB
    recent_mom_rows = db.execute(
        text("SELECT DISTINCT ticker_id FROM screener_signals WHERE date >= :since"),
        {"since": cutoff},
    )
    recent_mom = {row[0] for row in recent_mom_rows}

    # Also include today's signals (not yet persisted)
    today_rev = {s["ticker_id"] for s in reversion_signals}
    today_mom = {s["ticker_id"] for s in momentum_signals}

    all_rev = recent_rev | today_rev
    all_mom = recent_mom | today_mom

    # Flag momentum signals that overlap with recent reversion
    for sig in momentum_signals:
        if sig["ticker_id"] in all_rev:
            sig["confluence"] = True

    # Flag reversion signals that overlap with recent momentum
    for sig in reversion_signals:
        if sig["ticker_id"] in all_mom:
            sig["confluence"] = True


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
            "options_sentiment": s.get("options_sentiment"),
            "put_call_ratio": s.get("put_call_ratio"),
            "quality_score": s.get("quality_score"),
            "confluence": s.get("confluence", False),
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
            "options_sentiment": stmt.excluded.options_sentiment,
            "put_call_ratio": stmt.excluded.put_call_ratio,
            "quality_score": stmt.excluded.quality_score,
            "confluence": stmt.excluded.confluence,
        },
    )
    db.execute(stmt)
    db.commit()
    logger.info("Saved %d signals to Postgres", len(values))


# ------------------------------------------------------------------
# Full daily pipeline (screener + news enrichment + Telegram alert)
# ------------------------------------------------------------------

async def run_daily_pipeline(screen_date: date | None = None) -> dict:
    """
    End-to-end daily pipeline called by the cron job:
      1. Fetch earnings calendar to build blacklist (P2)
      2. Run the screener (with cooldown + earnings exclusion)
      3. Fetch Finnhub news for each signal
      4. Send Telegram alert
    Returns the screener result dict.
    """
    import asyncio
    from app.news_fetcher import fetch_news, fetch_earnings_blacklist
    from app.notifier import send_telegram_alert

    if screen_date is None:
        screen_date = date.today()

    # Step 1: Build earnings blacklist from Finnhub calendar
    # We need ALL active symbols to check, so load them first
    db = SessionLocal()
    try:
        all_tickers = db.query(Ticker).filter(Ticker.is_active.is_(True)).all()
        all_symbols = [t.symbol for t in all_tickers]
    finally:
        db.close()

    earnings_blacklist = await fetch_earnings_blacklist(
        all_symbols, from_date=screen_date, hold_days=7,
    )

    # Step 2: Run the momentum screener with cooldown + earnings exclusion
    result = run_screener(screen_date, earnings_blacklist=earnings_blacklist)
    signals = result["signals"]

    # Step 2b: Run the mean-reversion screener
    from app.mean_reversion import run_reversion_screener
    reversion_result = run_reversion_screener(screen_date)

    # Step 2c: Detect dual-strategy confluence (bounce-to-breakout)
    rev_signals = reversion_result.get("signals", [])
    db = SessionLocal()
    try:
        _detect_confluence(db, signals, rev_signals)
    finally:
        db.close()

    # Step 3: Fetch options sentiment for ALL signal tickers (momentum + reversion)
    from app.options_flow import fetch_options_sentiment_batch

    all_signal_symbols = list({s["symbol"] for s in signals} | {s["symbol"] for s in rev_signals})
    options_map = await fetch_options_sentiment_batch(all_signal_symbols)

    # Attach options data to momentum signals
    for sig in signals:
        flow = options_map.get(sig["symbol"], {})
        sig["options_sentiment"] = flow.get("sentiment")
        sig["put_call_ratio"] = flow.get("put_call_ratio")

    # Attach options data to reversion signals
    for sig in rev_signals:
        flow = options_map.get(sig["symbol"], {})
        sig["options_sentiment"] = flow.get("sentiment")
        sig["put_call_ratio"] = flow.get("put_call_ratio")

    # Re-persist momentum signals with options data
    db = SessionLocal()
    try:
        _save_signals(db, signals)
    finally:
        db.close()

    # Re-persist reversion signals with options data
    from app.mean_reversion import _save_reversion_signals
    db = SessionLocal()
    try:
        _save_reversion_signals(db, rev_signals)
    finally:
        db.close()

    # Step 4: Fetch news for all momentum signals concurrently
    news_map: dict[str, list[dict]] = {}
    if signals:
        tasks = [fetch_news(s["symbol"], limit=3) for s in signals]
        news_results = await asyncio.gather(*tasks)
        for sig, articles in zip(signals, news_results):
            news_map[sig["symbol"]] = articles

    # Step 5: Send unified Telegram notification (momentum + reversion)
    await send_telegram_alert(result, news_map, reversion_result=reversion_result)

    # Step 6: Paper Trading — record signals, fill pending, check stops
    try:
        from app.paper_tracker import (
            create_pending_trades,
            fill_pending_trades,
            check_open_trades,
        )

        db = SessionLocal()
        try:
            create_pending_trades(db, signals, "momentum")
            create_pending_trades(db, rev_signals, "reversion")
            fill_pending_trades(db)
            check_open_trades(db, screen_date)
        finally:
            db.close()
    except Exception:
        logger.exception("Paper trading step failed")

    return result


# ------------------------------------------------------------------
# CLI entry point: python -m app.screener
# ------------------------------------------------------------------

if __name__ == "__main__":
    import asyncio

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )

    async def _main():
        # 1. Run data pipeline first (fetch latest OHLCV from Polygon)
        #    Use years_back=0 (90 calendar days) for the daily screener run
        #    to keep memory usage within Heroku's 512 MB limit.
        from app.data_fetcher import run_full_data_pipeline

        logger.info("=== Starting data fetch pipeline (90 days) ===")
        await run_full_data_pipeline(years_back=0)

        # 2. Run screener + news + Telegram
        logger.info("=== Starting daily screener pipeline ===")
        result = await run_daily_pipeline()

        regime = result["regime"]["regime"]
        n = len(result["signals"])
        logger.info("=== Done — Regime: %s | Signals: %d ===", regime, n)

        for s in result["signals"]:
            conf = " *CONFLUENCE*" if s.get("confluence") else ""
            logger.info(
                "  %s  $%.2f  RVOL=%.2f  ATR%%=%.1f  Q=%.0f%s",
                s["symbol"], s["trigger_price"],
                s["rvol_at_trigger"], s["atr_pct_at_trigger"],
                s.get("quality_score", 0), conf,
            )

    asyncio.run(_main())
