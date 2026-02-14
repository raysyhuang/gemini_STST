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
from datetime import date, timedelta

import numpy as np
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models import PaperTrade, DailyMarketData, Ticker
from app.indicators import compute_atr_pct

logger = logging.getLogger(__name__)

# ── Constants (match backtester) ──────────────────────────────────
MOMENTUM_HOLD_DAYS = 7
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


# ── 1. Create Pending Trades ─────────────────────────────────────

def create_pending_trades(
    db: Session,
    signals: list[dict],
    strategy: str,
) -> int:
    """
    Create pending paper trades from screener signals.

    Deduplicates by (ticker_id, signal_date, strategy) using the
    unique constraint. Skips signals that already have a paper trade.

    Returns the number of new trades created.
    """
    created = 0
    for sig in signals:
        ticker_id = sig.get("ticker_id")
        signal_date = sig.get("date")
        if not ticker_id or not signal_date:
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

        # Compute vol-scaled position size from ATR%
        atr_pct = sig.get("atr_pct_at_trigger", 10.0)
        if atr_pct and atr_pct > 0:
            scaled_frac = min(max(TARGET_RISK / (atr_pct / 100.0), MIN_SIZE), MAX_SIZE)
        else:
            scaled_frac = 0.10  # fallback
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

    # Chandelier: trail distance = 2 * daily_atr_frac
    # daily_atr_frac = ATR% / (sqrt(5) * 100)
    trail_frac = 2.0 * atr_pct / (np.sqrt(5) * 100.0)
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
    Check open trades for stop hits and time exits.

    Priority order:
      1. Stop hit: today.low <= stop_level → exit at stop_level
      2. Momentum trailing update: today.high > highest_high → recalc stop
      3. Time exit: today >= planned_exit_date → exit at close * (1 - slippage)

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

        # 2. Momentum trailing stop update
        if (
            trade.strategy == "momentum"
            and today_data.high > (trade.highest_high_since_entry or 0)
        ):
            trade.highest_high_since_entry = today_data.high
            trade.stop_level = _compute_chandelier_stop(
                db, trade.ticker_id, trade.entry_date,
                today_data.high,
            )

        # 3. Time exit check
        if trade.planned_exit_date and check_date >= trade.planned_exit_date:
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
