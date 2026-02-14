"""
Parameter sweep for momentum strategy exit mechanics.

Pre-loads ALL OHLCV data once, collects v2 signals, then sweeps across
parameter combinations to find the most profitable exit configuration.

Usage:
    python run_tune_backtest.py
    python run_tune_backtest.py --months 6
    python run_tune_backtest.py --months 3 --top 20
"""

import argparse
import gc
import logging
import sys
import itertools
from collections import defaultdict
from datetime import date, timedelta

import numpy as np
import pandas as pd
from sqlalchemy import text

from app.database import SessionLocal, init_db
from app.indicators import add_all_indicators, check_market_regime
from app.models import Ticker
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
    REGIME_MULTIPLIERS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)

# ── Parameter grid ───────────────────────────────────────────────
PARAM_GRID = {
    "stop_mult":     [2.0, 2.5, 3.0, 3.5],
    "profit_target": [0.05, 0.07, 0.08, 0.10],
    "quality_floor": [0, 40, 50, 60],
    "hold_days":     [5, 7, 10],
    "skip_bearish":  [False, True],
}


# ── Parametric trade simulation ──────────────────────────────────

def _simulate_trade_parametric(
    entry_date: date,
    entry_open: float,
    ticker_ohlcv: pd.DataFrame,
    regime: str,
    quality: float,
    atr_pct: float,
    *,
    stop_mult: float,
    profit_target: float,
    hold_days: int,
) -> dict | None:
    """
    Simulate a single momentum trade with configurable exit parameters.

    Same logic as _simulate_trade in run_portfolio_backtest.py but accepts
    stop_mult, profit_target, and hold_days as arguments instead of using
    hardcoded constants.
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
    future = ticker_ohlcv[ticker_ohlcv["date"] > entry_date].head(hold_days)
    if future.empty:
        return None

    # Check each day for profit target or stop
    highest = entry_price
    profit_target_price = entry_price * (1 + profit_target)

    exit_price = None
    exit_reason = None
    exit_date = None
    actual_hold = 0

    for _, row in future.iterrows():
        actual_hold += 1
        # Profit target
        if row["high"] >= profit_target_price:
            exit_price = round(profit_target_price, 4)
            exit_reason = "profit_target"
            exit_date = row["date"]
            break
        # Trailing stop: stop_mult * ATR% / sqrt(5) trail from highest high
        if row["high"] > highest:
            highest = row["high"]
        if atr_pct and atr_pct > 0:
            trail_frac = stop_mult * atr_pct / (np.sqrt(5) * 100.0)
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
        "hold_days": actual_hold,
    }


# ── Data loading & signal collection ─────────────────────────────

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


def _collect_signals(db, months: int) -> list[dict]:
    """
    Load data, compute indicators, screen for v2 signals, and collect
    all the context needed to re-simulate trades with any parameters.

    Returns a list of signal dicts, each containing:
        ticker_id, signal_date, entry_open, entry_date, regime, quality,
        atr_pct, ticker_ohlcv (reference to the full DataFrame for that ticker)
    """
    end_date = date.today()
    start_date = end_date - timedelta(days=months * 30)
    lookback_start = start_date - timedelta(days=LOOKBACK_CALENDAR_DAYS)

    # 1. Load all active tickers
    all_tickers = db.query(Ticker).filter(Ticker.is_active.is_(True)).all()
    ticker_map = {t.id: t for t in all_tickers}
    ticker_ids = list(ticker_map.keys())
    logger.info("Tuner: %d active tickers, date range %s to %s", len(ticker_ids), start_date, end_date)

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
        return []

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

    # 6. Collect v2 signals with all context needed for parametric simulation
    signals = []
    signal_count = 0

    for i, screen_date in enumerate(trading_dates):
        if i > 0 and i % 25 == 0:
            logger.info("Signal collection progress: %d/%d dates (%d signals so far)",
                        i, len(trading_dates), signal_count)

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

            # Apply v2 filters only
            if not _apply_momentum_filters(latest, "v2"):
                continue

            signal_count += 1

            # Get T+1 entry data
            future_rows = df[df["date"] > screen_date]
            if future_rows.empty:
                continue

            entry_open = float(future_rows.iloc[0]["open"])
            entry_date = future_rows.iloc[0]["date"]
            quality = _compute_momentum_quality(latest)
            atr_pct = float(latest["atr_pct"]) if not pd.isna(latest["atr_pct"]) else 10.0

            signals.append({
                "ticker_id": tid,
                "signal_date": screen_date,
                "entry_date": entry_date,
                "entry_open": entry_open,
                "regime": regime,
                "quality": quality,
                "atr_pct": atr_pct,
                "ticker_ohlcv": df,  # reference, not copy
            })

    logger.info("Collected %d tradeable signals from %d v2 filter hits", len(signals), signal_count)
    return signals


# ── Parameter sweep engine ───────────────────────────────────────

def _compute_metrics(trades: list[dict]) -> dict:
    """Compute summary metrics from a list of trade results."""
    if not trades:
        return {
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
        "trade_count": len(trades),
        "win_rate": win_rate,
        "avg_return_pct": avg_ret,
        "total_pnl": total_pnl,
        "profit_factor": pf,
        "avg_hold_days": avg_hold,
        "exit_reasons": exit_reasons,
    }


def run_parameter_sweep(months: int = 6, top_n: int = 30) -> list[dict]:
    """
    Run the full parameter sweep.

    1. Collect all v2 signals once (slow — DB + indicators)
    2. For each parameter combo, re-simulate from signal list (fast — just math)
    3. Return ranked results sorted by profit factor
    """
    init_db()
    db = SessionLocal()
    try:
        signals = _collect_signals(db, months)
    finally:
        db.close()
        gc.collect()

    if not signals:
        logger.error("No signals collected — nothing to sweep")
        return []

    # Build all parameter combinations
    combos = list(itertools.product(
        PARAM_GRID["stop_mult"],
        PARAM_GRID["profit_target"],
        PARAM_GRID["quality_floor"],
        PARAM_GRID["hold_days"],
        PARAM_GRID["skip_bearish"],
    ))
    logger.info("Sweeping %d parameter combinations over %d signals...", len(combos), len(signals))

    results = []

    for idx, (stop_mult, profit_target, quality_floor, hold_days, skip_bearish) in enumerate(combos):
        if (idx + 1) % 50 == 0:
            logger.info("Sweep progress: %d/%d combos", idx + 1, len(combos))

        trades = []
        for sig in signals:
            # Quality floor filter: skip signals below threshold
            if sig["quality"] < quality_floor:
                continue

            # Regime skip: skip bearish regime trades entirely
            if skip_bearish and sig["regime"] == "Bearish":
                continue

            trade = _simulate_trade_parametric(
                sig["entry_date"],
                sig["entry_open"],
                sig["ticker_ohlcv"],
                sig["regime"],
                sig["quality"],
                sig["atr_pct"],
                stop_mult=stop_mult,
                profit_target=profit_target,
                hold_days=hold_days,
            )
            if trade:
                trades.append(trade)

        metrics = _compute_metrics(trades)
        metrics["stop_mult"] = stop_mult
        metrics["profit_target"] = profit_target
        metrics["quality_floor"] = quality_floor
        metrics["hold_days"] = hold_days
        metrics["skip_bearish"] = skip_bearish
        results.append(metrics)

    # Sort by profit factor descending, then by total PnL
    results.sort(key=lambda r: (r["profit_factor"], r["total_pnl"]), reverse=True)

    # ── Print results table ──────────────────────────────────────
    print()
    print(f"{'=' * 120}")
    print(f"PARAMETER SWEEP RESULTS ({months} months, v2 filters, {len(signals)} signals)")
    print(f"{'=' * 120}")
    print(
        f"{'Rank':<5} {'StopM':>5} {'Target':>7} {'QFloor':>7} {'Hold':>5} "
        f"{'SkipBr':>7} {'Trades':>7} {'WinRate':>8} {'AvgRet%':>8} "
        f"{'PnL$':>9} {'PF':>6} {'AvgHold':>8}  Exit Breakdown"
    )
    print("-" * 120)

    for rank, r in enumerate(results[:top_n], 1):
        exit_str = ", ".join(f"{k}={v}" for k, v in sorted(r["exit_reasons"].items()))
        print(
            f"{rank:<5} {r['stop_mult']:>5.1f} {r['profit_target']:>7.0%} "
            f"{r['quality_floor']:>7} {r['hold_days']:>5} "
            f"{str(r['skip_bearish']):>7} {r['trade_count']:>7} "
            f"{r['win_rate']:>7.1f}% {r['avg_return_pct']:>+7.2f}% "
            f"{r['total_pnl']:>+9.2f} {r['profit_factor']:>6.2f} "
            f"{r['avg_hold_days']:>7.1f}d  {exit_str}"
        )

    print(f"{'=' * 120}")

    # ── Highlight the baseline (current production config) ───────
    baseline = next(
        (r for r in results
         if r["stop_mult"] == 2.0
         and r["profit_target"] == 0.10
         and r["quality_floor"] == 0
         and r["hold_days"] == 7
         and r["skip_bearish"] is False),
        None,
    )
    if baseline:
        bl_rank = results.index(baseline) + 1
        print(f"\nBASELINE (current production): Rank {bl_rank}/{len(results)} | "
              f"PF {baseline['profit_factor']:.2f} | PnL ${baseline['total_pnl']:+.2f} | "
              f"WinRate {baseline['win_rate']:.1f}% | {baseline['trade_count']} trades")

    best = results[0]
    print(f"\nBEST: StopMult={best['stop_mult']}, Target={best['profit_target']:.0%}, "
          f"QFloor={best['quality_floor']}, Hold={best['hold_days']}, "
          f"SkipBearish={best['skip_bearish']}")
    print(f"      PF {best['profit_factor']:.2f} | PnL ${best['total_pnl']:+.2f} | "
          f"WinRate {best['win_rate']:.1f}% | {best['trade_count']} trades")

    if baseline and best["profit_factor"] > baseline["profit_factor"]:
        pf_delta = best["profit_factor"] - baseline["profit_factor"]
        pnl_delta = best["total_pnl"] - baseline["total_pnl"]
        print(f"      Improvement: PF +{pf_delta:.2f}, PnL ${pnl_delta:+.2f}")

    print()
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parameter sweep for momentum strategy tuning")
    parser.add_argument("--months", type=int, default=6, help="Months of history (1-12)")
    parser.add_argument("--top", type=int, default=30, help="Number of top results to display")
    args = parser.parse_args()

    months = max(1, min(12, args.months))
    run_parameter_sweep(months, top_n=args.top)
