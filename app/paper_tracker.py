"""
Paper Trading Tracker — simulated trade lifecycle management.

Automatically records every signal as a paper trade and manages the
full lifecycle: pending → open → closed.

Trade flow:
  1. Signal fires → create_pending_trades() → status=pending
  2. Next pipeline run:
     a. fill_pending_trades()  → fetch T+1 open → status=open
     b. check_open_trades()    → stop hit? time exit? → status=closed
  3. get_paper_metrics() / get_paper_trades() for API consumption

Constants match the backtester exactly.
"""

import logging
from collections import defaultdict
from datetime import date, timedelta

import numpy as np
from sqlalchemy import func, distinct, asc
from sqlalchemy.orm import Session

from app.models import PaperTrade, DailyMarketData, Ticker, ScreenerSignal, ReversionSignal
from app.indicators import compute_atr_pct

logger = logging.getLogger(__name__)

# ── Constants (match backtester) ──────────────────────────────────
MOMENTUM_HOLD_DAYS = 10              # tuned from 7 → 10 (sweep rank #1)
REVERSION_HOLD_DAYS = 5
REVERSION_STOP = 0.05       # 5% hard stop-loss
SLIPPAGE = 0.002             # 20 bps
FEES = 0.001                 # 0.1% each leg
POSITION_SIZE = 1000.0       # $1,000 per trade (legacy flat sizing)

# Volatility-scaled sizing
ACCOUNT_SIZE = 10_000
TARGET_RISK = 0.01           # 1% risk per trade
MIN_SIZE = 0.05              # 5% floor
MAX_SIZE = 0.20              # 20% cap

# Chandelier trailing stop ATR multiplier
MOMENTUM_STOP_MULT = 3.5            # tuned from 2.0 → 3.5 (sweep rank #1)

# Exit strategy: profit targets + hold extension
MOMENTUM_PROFIT_TARGET = 0.10    # +10% → take profit
REVERSION_PROFIT_TARGET = 0.05   # +5% → take profit
EXTENDED_MOMENTUM_HOLD = 14      # high-quality: 14 days (was 10, scaled with new hold)
EXTENDED_REVERSION_HOLD = 7      # high-quality: 7 days (was 5)
QUALITY_EXTENSION_THRESHOLD = 70  # Q >= 70 to qualify for extension

# Signal quality gate
MOMENTUM_QUALITY_FLOOR = 60      # skip signals with Q < 60 (sweep rank #1)

# Regime-aware sizing + filtering
REGIME_MULTIPLIERS = {"Bullish": 1.0, "Mixed": 0.75, "Bearish": 0.50}
QUALITY_SIZE_MULTIPLIERS = {70: 1.25, 40: 1.0, 0: 0.75}  # Q>=70 → 1.25x, Q>=40 → 1x, else 0.75x
SKIP_BEARISH_REGIME = True           # skip momentum trades entirely in Bearish regime (sweep rank #1)


# ── 1. Create Pending Trades ─────────────────────────────────────

def create_pending_trades(
    db: Session,
    signals: list[dict],
    strategy: str,
    regime: str = "Unknown",
) -> int:
    """
    Create pending paper trades from screener signals.

    Deduplicates by (ticker_id, signal_date, strategy) using the
    unique constraint. Skips signals that already have a paper trade.

    Args:
        regime: Market regime string ("Bullish", "Mixed", "Bearish", "Unknown").
            Used to scale position size down in adverse regimes.

    Returns the number of new trades created.
    """
    created = 0
    for sig in signals:
        ticker_id = sig.get("ticker_id")
        signal_date = sig.get("date")
        if not ticker_id or not signal_date:
            continue

        # Quality floor: skip low-quality momentum signals
        if strategy == "momentum":
            q = sig.get("quality_score", 0) or 0
            if q < MOMENTUM_QUALITY_FLOOR:
                continue
            # Regime gate: skip momentum trades in Bearish regime
            if SKIP_BEARISH_REGIME and regime == "Bearish":
                continue

        # Check for existing trade (dedup)
        existing = (
            db.query(PaperTrade)
            .filter(
                PaperTrade.ticker_id == ticker_id,
                PaperTrade.signal_date == signal_date,
                PaperTrade.strategy == strategy,
            )
            .first()
        )
        if existing:
            continue

        # Volatility-scaled base size
        atr_pct = sig.get("atr_pct_at_trigger", 10.0)
        if atr_pct and atr_pct > 0:
            scaled_frac = TARGET_RISK / (atr_pct / 100.0)
        else:
            scaled_frac = 0.10  # fallback

        # Regime multiplier
        regime_mult = REGIME_MULTIPLIERS.get(regime, 0.75)

        # Quality multiplier
        q = sig.get("quality_score", 0) or 0
        if q >= 70:
            quality_mult = 1.25
        elif q >= 40:
            quality_mult = 1.0
        else:
            quality_mult = 0.75

        scaled_frac = min(max(scaled_frac * regime_mult * quality_mult, MIN_SIZE), MAX_SIZE)
        pos_size = round(ACCOUNT_SIZE * scaled_frac, 2)

        trade = PaperTrade(
            ticker_id=ticker_id,
            strategy=strategy,
            signal_date=signal_date,
            position_size=pos_size,
            quality_score=sig.get("quality_score"),
            status="pending",
        )
        db.add(trade)
        created += 1

    if created:
        db.commit()
        logger.info("Created %d pending %s paper trades", created, strategy)

    return created


# ── 2. Fill Pending Trades ────────────────────────────────────────

def fill_pending_trades(db: Session) -> int:
    """
    Fill pending trades with T+1 open price + slippage.

    For each pending trade, fetch the first DailyMarketData row
    after signal_date to get the entry price. Compute stop level
    and planned exit date.

    Returns the number of trades filled.
    """
    pending = (
        db.query(PaperTrade)
        .filter(PaperTrade.status == "pending")
        .all()
    )
    if not pending:
        return 0

    filled = 0
    for trade in pending:
        # Get first trading day after signal_date
        next_day = (
            db.query(DailyMarketData)
            .filter(
                DailyMarketData.ticker_id == trade.ticker_id,
                DailyMarketData.date > trade.signal_date,
            )
            .order_by(DailyMarketData.date.asc())
            .first()
        )
        if not next_day:
            continue  # No data yet — keep pending

        # Entry at T+1 open + slippage
        entry_price = round(next_day.open * (1 + SLIPPAGE), 4)
        shares = round(trade.position_size / entry_price, 4)

        trade.entry_date = next_day.date
        trade.entry_price = entry_price
        trade.shares = shares
        trade.highest_high_since_entry = next_day.high

        # Compute stop level
        if trade.strategy == "momentum":
            trade.stop_level = _compute_chandelier_stop(
                db, trade.ticker_id, trade.entry_date, next_day.high,
            )
        else:
            # Reversion: 5% hard stop
            trade.stop_level = round(entry_price * (1 - REVERSION_STOP), 4)

        # Planned exit date: count forward N trading days from entry
        hold_days = (
            MOMENTUM_HOLD_DAYS if trade.strategy == "momentum"
            else REVERSION_HOLD_DAYS
        )
        trade.planned_exit_date = _get_nth_trading_day(
            db, trade.ticker_id, trade.entry_date, hold_days,
        )

        trade.status = "open"
        filled += 1

    if filled:
        db.commit()
        logger.info("Filled %d pending trades → open", filled)

    return filled


def _compute_chandelier_stop(
    db: Session,
    ticker_id: int,
    entry_date: date,
    highest_high: float,
) -> float:
    """
    Compute Chandelier trailing stop for momentum trades.

    stop = highest_high * (1 - 2 * ATR% / (sqrt(5) * 100))

    where ATR% is the weekly-projected ATR percentage from compute_atr_pct().
    """
    import pandas as pd

    # Load ~30 trading days of data ending at entry_date for ATR calculation
    lookback_start = entry_date - timedelta(days=60)
    rows = (
        db.query(DailyMarketData)
        .filter(
            DailyMarketData.ticker_id == ticker_id,
            DailyMarketData.date >= lookback_start,
            DailyMarketData.date <= entry_date,
        )
        .order_by(DailyMarketData.date.asc())
        .all()
    )
    if len(rows) < 15:
        # Fallback: 10% stop if insufficient data
        return round(highest_high * 0.90, 4)

    df = pd.DataFrame([
        {"high": r.high, "low": r.low, "close": r.close}
        for r in rows
    ])
    atr_pct_series = compute_atr_pct(df)
    atr_pct = atr_pct_series.iloc[-1]

    if np.isnan(atr_pct):
        return round(highest_high * 0.90, 4)

    # Chandelier: trail distance = MOMENTUM_STOP_MULT * daily_atr_frac
    # daily_atr_frac = ATR% / (sqrt(5) * 100)
    trail_frac = MOMENTUM_STOP_MULT * atr_pct / (np.sqrt(5) * 100.0)
    stop = highest_high * (1 - trail_frac)
    return round(stop, 4)


def _get_nth_trading_day(
    db: Session,
    ticker_id: int,
    from_date: date,
    n: int,
) -> date:
    """
    Get the Nth trading day after from_date for a given ticker,
    based on actual dates available in daily_market_data.
    """
    rows = (
        db.query(DailyMarketData.date)
        .filter(
            DailyMarketData.ticker_id == ticker_id,
            DailyMarketData.date > from_date,
        )
        .order_by(DailyMarketData.date.asc())
        .limit(n)
        .all()
    )
    if rows:
        return rows[-1][0]
    # Fallback: calendar days approximation
    return from_date + timedelta(days=int(n * 1.5))


# ── 3. Check Open Trades ─────────────────────────────────────────

def check_open_trades(db: Session, check_date: date | None = None) -> int:
    """
    Check open trades for stop hits, profit targets, and time exits.

    Priority order:
      1. Stop hit: today.low <= stop_level → exit at stop_level
      2. Profit target: today.high >= entry * (1+target) → exit at target price
      3. Momentum trailing update: today.high > highest_high → recalc stop
      4. Time exit: today >= planned_exit_date → exit at close * (1 - slippage)
         (with quality-based hold extension for Q >= 70 profitable trades)

    Returns the number of trades closed.
    """
    if check_date is None:
        check_date = date.today()

    open_trades = (
        db.query(PaperTrade)
        .filter(PaperTrade.status == "open")
        .all()
    )
    if not open_trades:
        return 0

    closed = 0
    for trade in open_trades:
        # Get today's market data for this ticker
        today_data = (
            db.query(DailyMarketData)
            .filter(
                DailyMarketData.ticker_id == trade.ticker_id,
                DailyMarketData.date == check_date,
            )
            .first()
        )
        if not today_data:
            continue

        # 1. Stop hit check
        if trade.stop_level and today_data.low <= trade.stop_level:
            exit_reason = (
                "trailing_stop" if trade.strategy == "momentum"
                else "stop_loss"
            )
            _close_trade(trade, trade.stop_level, check_date, exit_reason)
            closed += 1
            continue

        # 2. Profit target check
        if trade.entry_price:
            target = MOMENTUM_PROFIT_TARGET if trade.strategy == "momentum" else REVERSION_PROFIT_TARGET
            if today_data.high >= trade.entry_price * (1 + target):
                exit_price = round(trade.entry_price * (1 + target), 4)
                _close_trade(trade, exit_price, check_date, "profit_target")
                closed += 1
                continue

        # 3. Momentum trailing stop update
        if (
            trade.strategy == "momentum"
            and today_data.high > (trade.highest_high_since_entry or 0)
        ):
            trade.highest_high_since_entry = today_data.high
            trade.stop_level = _compute_chandelier_stop(
                db, trade.ticker_id, trade.entry_date,
                today_data.high,
            )

        # 4. Time exit — with quality-based hold extension
        if trade.planned_exit_date and check_date >= trade.planned_exit_date:
            # High-quality + profitable → extend hold period once
            if (
                trade.exit_reason != "extended"  # not already extended
                and (trade.quality_score or 0) >= QUALITY_EXTENSION_THRESHOLD
                and trade.entry_price
                and today_data.close > trade.entry_price
            ):
                ext_days = EXTENDED_MOMENTUM_HOLD if trade.strategy == "momentum" else EXTENDED_REVERSION_HOLD
                trade.planned_exit_date = _get_nth_trading_day(db, trade.ticker_id, trade.entry_date, ext_days)
                trade.exit_reason = "extended"  # flag to prevent double extension
                continue

            exit_price = round(today_data.close * (1 - SLIPPAGE), 4)
            _close_trade(trade, exit_price, check_date, "time_exit")
            closed += 1

    if closed:
        db.commit()
        logger.info("Closed %d open trades on %s", closed, check_date)

    # Commit trailing stop updates even if no trades closed
    db.commit()
    return closed


def _close_trade(
    trade: PaperTrade,
    exit_price: float,
    exit_date: date,
    reason: str,
) -> None:
    """Close a trade and compute PnL."""
    trade.exit_price = exit_price
    trade.actual_exit_date = exit_date
    trade.exit_reason = reason
    trade.status = "closed"

    if trade.entry_price and trade.shares:
        gross_pnl = (exit_price - trade.entry_price) * trade.shares
        # Fees: 0.1% on entry + 0.1% on exit
        entry_fees = trade.entry_price * trade.shares * FEES
        exit_fees = exit_price * trade.shares * FEES
        trade.pnl_dollars = round(gross_pnl - entry_fees - exit_fees, 2)
        trade.pnl_pct = round(
            (trade.pnl_dollars / trade.position_size) * 100, 2,
        )


# ── 4. Get Paper Metrics ─────────────────────────────────────────

def get_paper_metrics(db: Session) -> dict:
    """
    Compute aggregate performance metrics across all paper trades.

    Returns a dict matching PaperMetricsResponse schema.
    """
    closed_trades = (
        db.query(PaperTrade)
        .filter(PaperTrade.status == "closed")
        .all()
    )
    open_count = (
        db.query(func.count(PaperTrade.id))
        .filter(PaperTrade.status == "open")
        .scalar()
    ) or 0

    total_closed = len(closed_trades)
    total_trades = total_closed + open_count

    if total_closed == 0:
        return {
            "total_trades": total_trades,
            "open_trades": open_count,
            "closed_trades": 0,
            "win_rate": 0.0,
            "profit_factor": 0.0,
            "avg_return_pct": 0.0,
            "total_pnl": 0.0,
            "avg_hold_days": 0.0,
            "best_trade_pct": 0.0,
            "worst_trade_pct": 0.0,
            "momentum": {"total_trades": 0, "win_rate": 0.0, "avg_return_pct": 0.0, "total_pnl": 0.0},
            "reversion": {"total_trades": 0, "win_rate": 0.0, "avg_return_pct": 0.0, "total_pnl": 0.0},
        }

    winners = [t for t in closed_trades if (t.pnl_dollars or 0) > 0]
    losers = [t for t in closed_trades if (t.pnl_dollars or 0) <= 0]

    win_rate = round(len(winners) / total_closed * 100, 1) if total_closed else 0.0
    total_pnl = round(sum(t.pnl_dollars or 0 for t in closed_trades), 2)
    avg_return = round(
        sum(t.pnl_pct or 0 for t in closed_trades) / total_closed, 2,
    )

    gross_profit = sum(t.pnl_dollars for t in winners) if winners else 0
    gross_loss = abs(sum(t.pnl_dollars for t in losers)) if losers else 0
    profit_factor = (
        round(gross_profit / gross_loss, 2) if gross_loss > 0 else 0.0
    )

    # Hold days
    hold_days_list = []
    for t in closed_trades:
        if t.entry_date and t.actual_exit_date:
            hold_days_list.append((t.actual_exit_date - t.entry_date).days)
    avg_hold = round(sum(hold_days_list) / len(hold_days_list), 1) if hold_days_list else 0.0

    pnl_pcts = [t.pnl_pct or 0 for t in closed_trades]
    best_pct = max(pnl_pcts) if pnl_pcts else 0.0
    worst_pct = min(pnl_pcts) if pnl_pcts else 0.0

    # Strategy breakdown
    momentum_breakdown = _strategy_breakdown(
        [t for t in closed_trades if t.strategy == "momentum"],
    )
    reversion_breakdown = _strategy_breakdown(
        [t for t in closed_trades if t.strategy == "reversion"],
    )

    return {
        "total_trades": total_trades,
        "open_trades": open_count,
        "closed_trades": total_closed,
        "win_rate": win_rate,
        "profit_factor": profit_factor,
        "avg_return_pct": avg_return,
        "total_pnl": total_pnl,
        "avg_hold_days": avg_hold,
        "best_trade_pct": best_pct,
        "worst_trade_pct": worst_pct,
        "momentum": momentum_breakdown,
        "reversion": reversion_breakdown,
    }


def _strategy_breakdown(trades: list) -> dict:
    """Compute metrics for a subset of trades (single strategy)."""
    n = len(trades)
    if n == 0:
        return {"total_trades": 0, "win_rate": 0.0, "avg_return_pct": 0.0, "total_pnl": 0.0}

    winners = [t for t in trades if (t.pnl_dollars or 0) > 0]
    return {
        "total_trades": n,
        "win_rate": round(len(winners) / n * 100, 1),
        "avg_return_pct": round(sum(t.pnl_pct or 0 for t in trades) / n, 2),
        "total_pnl": round(sum(t.pnl_dollars or 0 for t in trades), 2),
    }


# ── 5. Get Paper Trades ──────────────────────────────────────────

def get_paper_trades(db: Session, status: str | None = None) -> list[dict]:
    """
    Query paper trades with optional status filter, joined with Ticker
    for the symbol. Returns a list of dicts ready for the API response.
    """
    query = (
        db.query(PaperTrade, Ticker.symbol)
        .join(Ticker, PaperTrade.ticker_id == Ticker.id)
    )

    if status and status != "all":
        query = query.filter(PaperTrade.status == status)

    query = query.order_by(PaperTrade.signal_date.desc())
    rows = query.all()

    result = []
    for trade, symbol in rows:
        hold_days = None
        if trade.entry_date and trade.actual_exit_date:
            hold_days = (trade.actual_exit_date - trade.entry_date).days
        elif trade.entry_date:
            hold_days = (date.today() - trade.entry_date).days

        result.append({
            "id": trade.id,
            "ticker": symbol,
            "strategy": trade.strategy,
            "signal_date": trade.signal_date,
            "entry_date": trade.entry_date,
            "entry_price": trade.entry_price,
            "shares": trade.shares,
            "position_size": trade.position_size,
            "quality_score": trade.quality_score,
            "stop_level": trade.stop_level,
            "planned_exit_date": trade.planned_exit_date,
            "actual_exit_date": trade.actual_exit_date,
            "exit_price": trade.exit_price,
            "exit_reason": trade.exit_reason,
            "pnl_dollars": trade.pnl_dollars,
            "pnl_pct": trade.pnl_pct,
            "status": trade.status,
            "hold_days": hold_days,
        })

    return result


# ── 6. Regime Map for Backfill ─────────────────────────────────

def _build_regime_map(db: Session, trading_dates: list[date]) -> dict[date, str]:
    """
    Pre-compute market regime (Bullish/Mixed/Bearish) for each trading date.

    Loads SPY and QQQ close prices once, then computes SMA-20 regime
    for every date in trading_dates.
    """
    import pandas as pd
    from app.models import Ticker, DailyMarketData

    regime_map: dict[date, str] = {}
    if not trading_dates:
        return regime_map

    # Load SPY and QQQ tickers
    spy_tkr = db.query(Ticker).filter(Ticker.symbol == "SPY").first()
    qqq_tkr = db.query(Ticker).filter(Ticker.symbol == "QQQ").first()
    if not spy_tkr or not qqq_tkr:
        return regime_map

    # Load close prices with buffer for SMA-20 computation
    buffer_start = min(trading_dates) - timedelta(days=45)
    for tkr_obj, label in [(spy_tkr, "spy"), (qqq_tkr, "qqq")]:
        rows = (
            db.query(DailyMarketData.date, DailyMarketData.close)
            .filter(
                DailyMarketData.ticker_id == tkr_obj.id,
                DailyMarketData.date >= buffer_start,
            )
            .order_by(DailyMarketData.date.asc())
            .all()
        )
        df = pd.DataFrame(rows, columns=["date", "close"])
        df["sma_20"] = df["close"].rolling(20).mean()
        df["above_sma20"] = df["close"] > df["sma_20"]
        if label == "spy":
            spy_lookup = dict(zip(df["date"], df["above_sma20"]))
        else:
            qqq_lookup = dict(zip(df["date"], df["above_sma20"]))

    for d in trading_dates:
        spy_above = spy_lookup.get(d)
        qqq_above = qqq_lookup.get(d)
        if spy_above is True and qqq_above is True:
            regime_map[d] = "Bullish"
        elif spy_above is False and qqq_above is False:
            regime_map[d] = "Bearish"
        elif spy_above is not None and qqq_above is not None:
            regime_map[d] = "Mixed"
        else:
            regime_map[d] = "Unknown"

    return regime_map


# ── 7. Backfill Paper Trades ────────────────────────────────────

def backfill_paper_trades(db: Session) -> dict:
    """
    Backfill paper trades from historical screener_signals and reversion_signals.

    Replays the full trade lifecycle day-by-day: fill → check → create,
    matching the live pipeline ordering so trades created on day N stay
    pending until day N+1.

    Returns a summary dict matching BackfillResponse schema.
    """
    # 1. Clear existing paper trades
    db.query(PaperTrade).delete()
    db.commit()
    logger.info("Cleared existing paper trades for backfill.")

    # 2. Load all signals from both tables
    momentum_rows = db.query(ScreenerSignal).order_by(ScreenerSignal.date).all()
    reversion_rows = db.query(ReversionSignal).order_by(ReversionSignal.date).all()

    if not momentum_rows and not reversion_rows:
        return {
            "total_created": 0,
            "total_filled": 0,
            "total_closed": 0,
            "date_range": "N/A",
            "trading_days_processed": 0,
        }

    # 3. Build date-indexed signal map
    signal_dates = defaultdict(lambda: {"momentum": [], "reversion": []})

    for row in momentum_rows:
        signal_dates[row.date]["momentum"].append({
            "ticker_id": row.ticker_id,
            "date": row.date,
            "atr_pct_at_trigger": row.atr_pct_at_trigger,
            "quality_score": row.quality_score,
        })

    for row in reversion_rows:
        signal_dates[row.date]["reversion"].append({
            "ticker_id": row.ticker_id,
            "date": row.date,
            "atr_pct_at_trigger": 10.0,  # reversion_signals doesn't store atr_pct
            "quality_score": row.quality_score,
        })

    # 4. Get all trading dates from DB (min signal date to max + 30 days buffer)
    all_signal_dates = list(signal_dates.keys())
    min_date = min(all_signal_dates)
    max_date = max(all_signal_dates) + timedelta(days=45)  # calendar buffer for 30 trading days

    trading_dates = [
        row[0] for row in db.query(distinct(DailyMarketData.date))
        .filter(DailyMarketData.date >= min_date, DailyMarketData.date <= max_date)
        .order_by(asc(DailyMarketData.date))
        .all()
    ]

    if not trading_dates:
        return {
            "total_created": 0,
            "total_filled": 0,
            "total_closed": 0,
            "date_range": f"{min_date} to {max_date}",
            "trading_days_processed": 0,
        }

    # 5. Pre-compute regime for all trading dates
    regime_map = _build_regime_map(db, trading_dates)

    # 6. Iterate each trading date IN ORDER: fill → check → create
    total_created = 0
    total_filled = 0
    total_closed = 0

    for i, current_date in enumerate(trading_dates):
        # Progress logging every 50 dates
        if i > 0 and i % 50 == 0:
            logger.info(
                "Backfill progress: %d/%d trading days (created=%d, filled=%d, closed=%d)",
                i, len(trading_dates), total_created, total_filled, total_closed,
            )

        # Fill yesterday's pending → open
        total_filled += fill_pending_trades(db)

        # Check open trades for stops/time exits
        total_closed += check_open_trades(db, current_date)

        # Create new pending trades if signals exist for this date
        if current_date in signal_dates:
            signals = signal_dates[current_date]
            regime = regime_map.get(current_date, "Unknown")
            if signals["momentum"]:
                total_created += create_pending_trades(db, signals["momentum"], "momentum", regime=regime)
            if signals["reversion"]:
                total_created += create_pending_trades(db, signals["reversion"], "reversion", regime=regime)

    # Final pass: fill and close any remaining trades
    total_filled += fill_pending_trades(db)
    if trading_dates:
        total_closed += check_open_trades(db, trading_dates[-1])

    logger.info(
        "Backfill complete: created=%d, filled=%d, closed=%d over %d trading days",
        total_created, total_filled, total_closed, len(trading_dates),
    )

    actual_min = trading_dates[0] if trading_dates else min_date
    actual_max = trading_dates[-1] if trading_dates else max_date

    return {
        "total_created": total_created,
        "total_filled": total_filled,
        "total_closed": total_closed,
        "date_range": f"{actual_min} to {actual_max}",
        "trading_days_processed": len(trading_dates),
    }


# ── 8. Get Equity Curve ────────────────────────────────────────

def get_equity_curve(db: Session) -> list[dict]:
    """
    Compute cumulative equity curve from closed paper trades.

    Returns a list of {time: "YYYY-MM-DD", value: float} dicts matching
    the TradingView Lightweight Charts format used by the backtester.
    """
    closed_trades = (
        db.query(PaperTrade)
        .filter(PaperTrade.status == "closed", PaperTrade.actual_exit_date.isnot(None))
        .order_by(PaperTrade.actual_exit_date.asc())
        .all()
    )

    if not closed_trades:
        return []

    # Group PnL by exit date
    daily_pnl = defaultdict(float)
    for t in closed_trades:
        daily_pnl[t.actual_exit_date] += (t.pnl_dollars or 0)

    # Build cumulative equity curve starting from ACCOUNT_SIZE
    sorted_dates = sorted(daily_pnl.keys())
    cumulative = ACCOUNT_SIZE
    curve = []

    for d in sorted_dates:
        cumulative += daily_pnl[d]
        curve.append({
            "time": d.isoformat(),
            "value": round(cumulative, 2),
        })

    return curve


# ── CLI Entry Point ─────────────────────────────────────────────

if __name__ == "__main__":
    from app.database import SessionLocal as _SessionLocal
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    db = _SessionLocal()
    try:
        result = backfill_paper_trades(db)
        print(result)
    finally:
        db.close()
