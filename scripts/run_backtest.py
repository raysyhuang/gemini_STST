"""Backtest runner for Gemini STST — CLI wrapper around paper_tracker.

Replays all historical signals through the paper trading system and
outputs a standardized JSON report for cross-engine comparison.

Usage:
    python scripts/run_backtest.py
    python scripts/run_backtest.py --start 2025-01-01 --end 2026-02-17
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import numpy as np

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.database import SessionLocal, init_db
from app.paper_tracker import (
    backfill_paper_trades,
    get_paper_metrics,
    get_paper_trades,
    get_equity_curve,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)


def run_backtest(
    start_date: str | None = None,
    end_date: str | None = None,
    skip_backfill: bool = False,
) -> dict:
    """Run backtest and produce standardized JSON output.

    Steps:
    1. Call backfill_paper_trades() to replay all historical signals
    2. Call get_paper_metrics() for aggregate stats
    3. Call get_paper_trades() for trade-level detail
    4. Call get_equity_curve() for cumulative PnL
    5. Format into standardized JSON
    """
    start_time = time.monotonic()
    logger.info("=" * 60)
    logger.info("Gemini STST Backtest Runner")
    logger.info("=" * 60)

    init_db()
    db = SessionLocal()

    try:
        # 1. Backfill paper trades from historical signals
        if not skip_backfill:
            logger.info("Backfilling paper trades from historical signals...")
            backfill_result = backfill_paper_trades(db)
            logger.info(
                "Backfill complete: created=%d, filled=%d, closed=%d, days=%d",
                backfill_result["total_created"],
                backfill_result["total_filled"],
                backfill_result["total_closed"],
                backfill_result["trading_days_processed"],
            )
        else:
            backfill_result = {"note": "skipped"}

        # 2. Get aggregate metrics
        metrics = get_paper_metrics(db)

        # 3. Get all closed trades
        all_trades = get_paper_trades(db, status="closed")

        # 4. Get equity curve
        equity = get_equity_curve(db)

        # Apply date filtering if specified
        if start_date:
            sd = date.fromisoformat(start_date)
            all_trades = [
                t for t in all_trades
                if t.get("signal_date") and t["signal_date"] >= sd
            ]
        if end_date:
            ed = date.fromisoformat(end_date)
            all_trades = [
                t for t in all_trades
                if t.get("signal_date") and t["signal_date"] <= ed
            ]

        # Compute standardized summary from filtered trades
        summary = _compute_summary(all_trades, metrics)

        # Format trades for standardized output
        formatted_trades = []
        for t in all_trades:
            formatted_trades.append({
                "ticker": t.get("ticker"),
                "signal_date": str(t.get("signal_date")) if t.get("signal_date") else None,
                "entry_date": str(t.get("entry_date")) if t.get("entry_date") else None,
                "entry_price": t.get("entry_price"),
                "exit_date": str(t.get("actual_exit_date")) if t.get("actual_exit_date") else None,
                "exit_price": t.get("exit_price"),
                "exit_reason": t.get("exit_reason"),
                "pnl_pct": t.get("pnl_pct"),
                "hold_days": t.get("hold_days"),
                "strategy": t.get("strategy"),
                "quality_score": t.get("quality_score"),
            })

        # Format equity curve
        formatted_curve = []
        for point in equity:
            formatted_curve.append({
                "date": point.get("time"),
                "cumulative_pnl_pct": round(
                    (point.get("value", 10000) - 10000) / 100, 2
                ),
            })

        # Determine date range
        if formatted_trades:
            actual_start = min(
                t["signal_date"] for t in formatted_trades if t["signal_date"]
            )
            actual_end = max(
                t["signal_date"] for t in formatted_trades if t["signal_date"]
            )
        else:
            actual_start = start_date or "N/A"
            actual_end = end_date or "N/A"

        # Strategy breakdown
        by_strategy = {}
        for strat in ("momentum", "reversion"):
            strat_trades = [t for t in all_trades if t.get("strategy") == strat]
            if strat_trades:
                pnls = [t.get("pnl_pct", 0) or 0 for t in strat_trades]
                wins = [p for p in pnls if p > 0]
                by_strategy[strat] = {
                    "trades": len(strat_trades),
                    "win_rate": round(len(wins) / len(strat_trades), 4) if strat_trades else 0,
                    "avg_return_pct": round(float(np.mean(pnls)), 2) if pnls else 0,
                }

        elapsed = time.monotonic() - start_time

        report = {
            "engine": "gemini-stst",
            "run_date": str(date.today()),
            "date_range": {"start": str(actual_start), "end": str(actual_end)},
            "config": {
                "holding_period_days": 10,
                "max_positions": None,
                "slippage_bps": 20,
                "strategies": ["momentum", "reversion"],
            },
            "summary": summary,
            "by_strategy": by_strategy,
            "backfill": backfill_result,
            "trades": formatted_trades,
            "equity_curve": formatted_curve,
            "elapsed_s": round(elapsed, 2),
        }

        # Save report
        output_dir = Path("backtest_results")
        output_dir.mkdir(parents=True, exist_ok=True)
        report_path = output_dir / f"gemini_backtest_{actual_start}_{actual_end}.json"
        report_path.write_text(json.dumps(report, indent=2, default=str))

        logger.info(
            "Backtest complete in %.1fs: %d trades, win_rate=%.1f%%, report=%s",
            elapsed, summary.get("total_trades", 0),
            summary.get("win_rate", 0) * 100, report_path,
        )

        return report

    finally:
        db.close()


def _compute_summary(trades: list[dict], raw_metrics: dict) -> dict:
    """Compute standardized summary from trades and raw metrics."""
    if not trades:
        return {
            "total_trades": 0, "win_rate": 0, "avg_return_pct": 0,
            "sharpe": 0, "sortino": 0, "max_drawdown_pct": 0,
            "profit_factor": 0, "expectancy_pct": 0, "calmar": 0,
            "avg_hold_days": 0,
        }

    pnls = [t.get("pnl_pct", 0) or 0 for t in trades]
    arr = np.array(pnls)
    wins = arr[arr > 0]
    losses = arr[arr <= 0]

    win_rate = len(wins) / len(arr) if len(arr) > 0 else 0
    avg_return = float(np.mean(arr))

    std = float(np.std(arr, ddof=1)) if len(arr) > 1 else 1.0
    sharpe = avg_return / std * np.sqrt(50) if std > 0 else 0

    downside = arr[arr < 0]
    ds_std = float(np.std(downside, ddof=1)) if len(downside) > 1 else 1.0
    sortino = avg_return / ds_std * np.sqrt(50) if ds_std > 0 else 0

    cumulative = np.cumsum(arr)
    running_max = np.maximum.accumulate(cumulative)
    drawdowns = running_max - cumulative
    max_dd = float(np.max(drawdowns)) if len(drawdowns) > 0 else 0

    total_gains = float(np.sum(wins)) if len(wins) > 0 else 0
    total_losses_abs = abs(float(np.sum(losses))) if len(losses) > 0 else 0
    pf = total_gains / total_losses_abs if total_losses_abs > 0 else (
        float("inf") if total_gains > 0 else 0
    )

    avg_win = float(np.mean(wins)) if len(wins) > 0 else 0
    avg_loss = float(np.mean(losses)) if len(losses) > 0 else 0
    expectancy = avg_win * win_rate + avg_loss * (1 - win_rate)

    calmar = float(np.sum(arr)) / max_dd if max_dd > 0 else 0

    hold_days = [t.get("hold_days", 0) or 0 for t in trades]
    avg_hold = float(np.mean(hold_days)) if hold_days else 0

    return {
        "total_trades": len(arr),
        "win_rate": round(float(win_rate), 4),
        "avg_return_pct": round(avg_return, 2),
        "sharpe": round(float(sharpe), 2),
        "sortino": round(float(sortino), 2),
        "max_drawdown_pct": round(-max_dd, 2),
        "profit_factor": round(float(pf), 2) if pf != float("inf") else 999.0,
        "expectancy_pct": round(float(expectancy), 2),
        "calmar": round(float(calmar), 2),
        "avg_hold_days": round(avg_hold, 1),
    }


def main():
    parser = argparse.ArgumentParser(description="Gemini STST Backtest Runner")
    parser.add_argument("--start", type=str, help="Start date filter (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="End date filter (YYYY-MM-DD)")
    parser.add_argument(
        "--skip-backfill", action="store_true",
        help="Skip backfill and use existing paper trades",
    )
    args = parser.parse_args()

    report = run_backtest(
        start_date=args.start,
        end_date=args.end,
        skip_backfill=args.skip_backfill,
    )
    print(json.dumps(report.get("summary", {}), indent=2))


if __name__ == "__main__":
    main()
