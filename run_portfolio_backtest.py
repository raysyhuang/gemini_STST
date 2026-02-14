"""
Portfolio-level historical backtest: v1 (6-filter) vs v2 (9-filter) comparison.

Pre-loads ALL OHLCV data once, computes indicators per ticker, then for each
historical trading date applies both filter versions and simulates trades.

Usage:
    heroku run python run_portfolio_backtest.py
    python run_portfolio_backtest.py --months 6
"""

import argparse
import gc
import logging
import sys
from collections import defaultdict
from datetime import date, timedelta

import numpy as np
import pandas as pd
from sqlalchemy import text, distinct, asc

from app.database import SessionLocal, init_db
from app.indicators import add_all_indicators, check_market_regime
from app.models import Ticker, DailyMarketData
from app.screener import (
    _apply_momentum_filters,
    _compute_momentum_quality,
    LOOKBACK_CALENDAR_DAYS,
)
from app.paper_tracker import (
    SLIPPAGE,
    FEES,
    ACCOUNT_SIZE,
    TARGET_RISK,
    MIN_SIZE,
    MAX_SIZE,
    MOMENTUM_HOLD_DAYS,
    MOMENTUM_PROFIT_TARGET,
    MOMENTUM_STOP_MULT,
    REGIME_MULTIPLIERS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)


def _compute_regime_for_date(spy_df: pd.DataFrame, qqq_df: pd.DataFrame, d: date) -> str:
    """Compute regime from pre-loaded SPY/QQQ DataFrames for a given date."""
    spy_sub = spy_df[spy_df["date"] <= d].tail(25)
    qqq_sub = qqq_df[qqq_df["date"] <= d].tail(25)
    if len(spy_sub) < 20 or len(qqq_sub) < 20:
        return "Unknown"
    spy_sma20 = spy_sub["close"].rolling(20).mean().iloc[-1]
    qqq_sma20 = qqq_sub["close"].rolling(20).mean().iloc[-1]
    spy_above = spy_sub["close"].iloc[-1] > spy_sma20
    qqq_above = qqq_sub["close"].iloc[-1] > qqq_sma20
    if spy_above and qqq_above:
        return "Bullish"
    elif not spy_above and not qqq_above:
        return "Bearish"
    return "Mixed"


def _simulate_trade(
    entry_date: date,
    entry_open: float,
    ticker_ohlcv: pd.DataFrame,
    regime: str,
    quality: float,
    atr_pct: float,
) -> dict | None:
    """
    Simulate a single momentum trade from entry_date forward.

    Returns a dict with PnL info, or None if no data for entry.
    """
    entry_price = round(entry_open * (1 + SLIPPAGE), 4)
    if entry_price <= 0:
        return None

    # Volatility-scaled position size with regime + quality multipliers
    if atr_pct and atr_pct > 0:
        scaled_frac = TARGET_RISK / (atr_pct / 100.0)
    else:
        scaled_frac = 0.10
    regime_mult = REGIME_MULTIPLIERS.get(regime, 0.75)
    q_mult = 1.25 if quality >= 70 else (1.0 if quality >= 40 else 0.75)
    scaled_frac = min(max(scaled_frac * regime_mult * q_mult, MIN_SIZE), MAX_SIZE)
    pos_size = ACCOUNT_SIZE * scaled_frac
    shares = pos_size / entry_price

    # Get subsequent trading days
    future = ticker_ohlcv[ticker_ohlcv["date"] > entry_date].head(MOMENTUM_HOLD_DAYS)
    if future.empty:
        return None

    # Check each day for profit target or stop
    highest = entry_price
    profit_target_price = entry_price * (1 + MOMENTUM_PROFIT_TARGET)

    exit_price = None
    exit_reason = None
    exit_date = None
    hold_days = 0

    for _, row in future.iterrows():
        hold_days += 1
        # Profit target
        if row["high"] >= profit_target_price:
            exit_price = round(profit_target_price, 4)
            exit_reason = "profit_target"
            exit_date = row["date"]
            break
        # Trailing stop: MOMENTUM_STOP_MULT * ATR% / sqrt(5) trail from highest high
        if row["high"] > highest:
            highest = row["high"]
        if atr_pct and atr_pct > 0:
            trail_frac = MOMENTUM_STOP_MULT * atr_pct / (np.sqrt(5) * 100.0)
        else:
            trail_frac = 0.10
        stop = highest * (1 - trail_frac)
        if row["low"] <= stop:
            exit_price = round(stop, 4)
            exit_reason = "trailing_stop"
            exit_date = row["date"]
            break

    # Time exit at end of hold period
    if exit_price is None and not future.empty:
        last_row = future.iloc[-1]
        exit_price = round(float(last_row["close"]) * (1 - SLIPPAGE), 4)
        exit_reason = "time_exit"
        exit_date = last_row["date"]

    if exit_price is None:
        return None

    gross_pnl = (exit_price - entry_price) * shares
    entry_fees = entry_price * shares * FEES
    exit_fees = exit_price * shares * FEES
    net_pnl = gross_pnl - entry_fees - exit_fees
    pnl_pct = (net_pnl / pos_size) * 100

    return {
        "entry_price": entry_price,
        "exit_price": exit_price,
        "exit_reason": exit_reason,
        "exit_date": exit_date,
        "pnl_dollars": round(net_pnl, 2),
        "pnl_pct": round(pnl_pct, 2),
        "pos_size": round(pos_size, 2),
        "hold_days": hold_days,
    }


def run_portfolio_backtest(months: int = 6) -> dict:
    """
    Run the full v1-vs-v2 portfolio backtest.

    Returns comparison summary dict.
    """
    init_db()
    db = SessionLocal()
    try:
        return _run_backtest_impl(db, months)
    finally:
        db.close()
        gc.collect()


def _run_backtest_impl(db, months: int) -> dict:
    end_date = date.today()
    start_date = end_date - timedelta(days=months * 30)
    lookback_start = start_date - timedelta(days=LOOKBACK_CALENDAR_DAYS)

    # 1. Load all active tickers
    all_tickers = db.query(Ticker).filter(Ticker.is_active.is_(True)).all()
    ticker_map = {t.id: t for t in all_tickers}
    ticker_ids = list(ticker_map.keys())
    logger.info("Backtest: %d active tickers, date range %s to %s", len(ticker_ids), start_date, end_date)

    # 2. Load ALL OHLCV in one query
    stmt = text("""
        SELECT ticker_id, date, open, high, low, close, volume
        FROM daily_market_data
        WHERE ticker_id = ANY(:ids)
          AND date >= :since
        ORDER BY ticker_id, date ASC
    """)
    result = db.execute(stmt, {"ids": ticker_ids, "since": lookback_start})
    rows = result.fetchall()
    if not rows:
        logger.error("No OHLCV data found")
        return {"error": "No data"}

    all_ohlcv = pd.DataFrame(
        rows,
        columns=["ticker_id", "date", "open", "high", "low", "close", "volume"],
    )
    logger.info("Loaded %d OHLCV rows", len(all_ohlcv))

    # 3. Pre-compute indicators per ticker
    logger.info("Computing indicators per ticker...")
    indicator_dfs: dict[int, pd.DataFrame] = {}
    for tid, group_df in all_ohlcv.groupby("ticker_id"):
        if len(group_df) < 20:
            continue
        df = group_df[["date", "open", "high", "low", "close", "volume"]].copy()
        df = add_all_indicators(df)
        indicator_dfs[tid] = df
    logger.info("Computed indicators for %d tickers", len(indicator_dfs))

    # 4. Load SPY/QQQ for regime computation
    spy_tkr = next((t for t in all_tickers if t.symbol == "SPY"), None)
    qqq_tkr = next((t for t in all_tickers if t.symbol == "QQQ"), None)
    spy_df = indicator_dfs.get(spy_tkr.id, pd.DataFrame()) if spy_tkr else pd.DataFrame()
    qqq_df = indicator_dfs.get(qqq_tkr.id, pd.DataFrame()) if qqq_tkr else pd.DataFrame()

    # 5. Get all trading dates in the backtest window
    trading_dates = sorted(
        all_ohlcv[
            (all_ohlcv["date"] >= start_date) & (all_ohlcv["date"] <= end_date)
        ]["date"].unique()
    )
    logger.info("Backtest window: %d trading dates", len(trading_dates))

    # 6. Per-date screening and trade simulation
    v1_trades: list[dict] = []
    v2_trades: list[dict] = []
    v1_signal_count = 0
    v2_signal_count = 0

    for i, screen_date in enumerate(trading_dates):
        if i > 0 and i % 25 == 0:
            logger.info("Backtest progress: %d/%d dates (v1=%d, v2=%d signals)",
                        i, len(trading_dates), v1_signal_count, v2_signal_count)

        regime = _compute_regime_for_date(spy_df, qqq_df, screen_date)

        for tid, df in indicator_dfs.items():
            # Get latest row on or before screen_date
            mask = df["date"] <= screen_date
            if mask.sum() == 0:
                continue
            latest = df[mask].iloc[-1]

            # Must be within 5 days of screen_date
            if (screen_date - latest["date"]).days > 5:
                continue

            # Apply v1 filters
            if _apply_momentum_filters(latest, "v1"):
                v1_signal_count += 1
                # Simulate trade: need T+1 open
                future_rows = df[df["date"] > screen_date]
                if not future_rows.empty:
                    entry_open = float(future_rows.iloc[0]["open"])
                    quality = _compute_momentum_quality(latest)
                    atr_pct = float(latest["atr_pct"]) if not pd.isna(latest["atr_pct"]) else 10.0
                    trade = _simulate_trade(
                        future_rows.iloc[0]["date"], entry_open, df, regime, quality, atr_pct,
                    )
                    if trade:
                        trade["ticker_id"] = tid
                        trade["signal_date"] = screen_date
                        trade["version"] = "v1"
                        v1_trades.append(trade)

            # Apply v2 filters
            if _apply_momentum_filters(latest, "v2"):
                v2_signal_count += 1
                future_rows = df[df["date"] > screen_date]
                if not future_rows.empty:
                    entry_open = float(future_rows.iloc[0]["open"])
                    quality = _compute_momentum_quality(latest)
                    atr_pct = float(latest["atr_pct"]) if not pd.isna(latest["atr_pct"]) else 10.0
                    trade = _simulate_trade(
                        future_rows.iloc[0]["date"], entry_open, df, regime, quality, atr_pct,
                    )
                    if trade:
                        trade["ticker_id"] = tid
                        trade["signal_date"] = screen_date
                        trade["version"] = "v2"
                        v2_trades.append(trade)

    # 7. Compute comparison summary
    summary = _build_comparison(v1_trades, v2_trades, v1_signal_count, v2_signal_count, months)
    return summary


def _build_comparison(
    v1_trades: list[dict],
    v2_trades: list[dict],
    v1_signal_count: int,
    v2_signal_count: int,
    months: int,
) -> dict:
    """Build v1 vs v2 comparison metrics."""
    def _metrics(trades: list[dict], signal_count: int) -> dict:
        if not trades:
            return {
                "signal_count": signal_count,
                "trade_count": 0,
                "win_rate": 0.0,
                "avg_return_pct": 0.0,
                "total_pnl": 0.0,
                "profit_factor": 0.0,
                "avg_hold_days": 0.0,
                "exit_reasons": {},
            }

        winners = [t for t in trades if t["pnl_dollars"] > 0]
        losers = [t for t in trades if t["pnl_dollars"] <= 0]
        win_rate = round(len(winners) / len(trades) * 100, 1)
        avg_ret = round(sum(t["pnl_pct"] for t in trades) / len(trades), 2)
        total_pnl = round(sum(t["pnl_dollars"] for t in trades), 2)

        gross_profit = sum(t["pnl_dollars"] for t in winners)
        gross_loss = abs(sum(t["pnl_dollars"] for t in losers)) if losers else 0
        pf = round(gross_profit / gross_loss, 2) if gross_loss > 0 else 0.0

        avg_hold = round(sum(t["hold_days"] for t in trades) / len(trades), 1)

        exit_reasons: dict[str, int] = {}
        for t in trades:
            r = t.get("exit_reason", "unknown")
            exit_reasons[r] = exit_reasons.get(r, 0) + 1

        return {
            "signal_count": signal_count,
            "trade_count": len(trades),
            "win_rate": win_rate,
            "avg_return_pct": avg_ret,
            "total_pnl": total_pnl,
            "profit_factor": pf,
            "avg_hold_days": avg_hold,
            "exit_reasons": exit_reasons,
        }

    v1_m = _metrics(v1_trades, v1_signal_count)
    v2_m = _metrics(v2_trades, v2_signal_count)

    logger.info("=" * 60)
    logger.info("PORTFOLIO BACKTEST RESULTS (%d months)", months)
    logger.info("=" * 60)
    for label, m in [("v1 (6-filter)", v1_m), ("v2 (9-filter)", v2_m)]:
        logger.info(
            "%s: %d signals → %d trades | Win %.1f%% | Avg %.2f%% | PnL $%.2f | PF %.2f | Hold %.1fd",
            label, m["signal_count"], m["trade_count"], m["win_rate"],
            m["avg_return_pct"], m["total_pnl"], m["profit_factor"], m["avg_hold_days"],
        )
        logger.info("  Exit reasons: %s", m["exit_reasons"])
    logger.info("=" * 60)

    return {
        "months": months,
        "v1": v1_m,
        "v2": v2_m,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Portfolio backtest: v1 vs v2 filter comparison")
    parser.add_argument("--months", type=int, default=6, help="Months of history (1-12)")
    args = parser.parse_args()

    months = max(1, min(12, args.months))
    result = run_portfolio_backtest(months)

    print("\n--- Summary ---")
    for version in ["v1", "v2"]:
        m = result[version]
        print(f"  {version}: {m['signal_count']} signals → {m['trade_count']} trades | "
              f"Win {m['win_rate']}% | Avg {m['avg_return_pct']}% | PnL ${m['total_pnl']} | PF {m['profit_factor']}")
