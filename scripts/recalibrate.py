#!/usr/bin/env python3
"""
CLI for the outcome-driven quality recalibration system.

Usage:
  python scripts/recalibrate.py analyze                     # both strategies
  python scripts/recalibrate.py analyze --strategy momentum  # just momentum
  python scripts/recalibrate.py show                         # pending recommendations
  python scripts/recalibrate.py show --id 3                  # detail for #3
  python scripts/recalibrate.py approve --id 3               # apply recommendation
  python scripts/recalibrate.py reject --id 3 --notes "..."
  python scripts/recalibrate.py status                       # active vs hardcoded
"""

import argparse
import json
import logging
import sys
from pathlib import Path

# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.database import SessionLocal
from app.learning import (
    analyze_factor_weights,
    create_recommendation,
    apply_recommendation,
    reject_recommendation,
    get_active_weights,
    get_all_recommendations,
    DEFAULT_WEIGHTS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
logger = logging.getLogger(__name__)


def cmd_analyze(args):
    """Run factor analysis and create recommendations."""
    strategies = [args.strategy] if args.strategy else ["momentum", "reversion"]
    db = SessionLocal()
    try:
        for strategy in strategies:
            print(f"\n{'='*60}")
            print(f"  Analyzing: {strategy}")
            print(f"{'='*60}")

            analysis = analyze_factor_weights(db, strategy)
            if analysis is None:
                print(f"  Insufficient data for {strategy}. Skipping.")
                continue

            print(f"  Trades analyzed: {analysis['n_trades']}")
            print(f"  Overall win rate: {analysis['overall_win_rate']:.1%}")
            print()

            # Factor analysis table
            print(f"  {'Factor':<16} {'Power':>8}  {'Q1 WR':>8}  {'Q4 WR':>8}  {'Cur Wt':>8}  {'New Wt':>8}")
            print(f"  {'-'*16} {'-'*8}  {'-'*8}  {'-'*8}  {'-'*8}  {'-'*8}")
            for fa in analysis["factor_analysis"]:
                f = fa["factor"]
                cur = analysis["current_weights"].get(f, 0)
                new = analysis["recommended_weights"].get(f, 0)
                q1_wr = fa["quartiles"]["1"]["smoothed_wr"]
                q4_wr = fa["quartiles"]["4"]["smoothed_wr"]
                print(f"  {f:<16} {fa['predictive_power']:>8.4f}  {q1_wr:>8.3f}  {q4_wr:>8.3f}  {cur:>8.2%}  {new:>8.2%}")

            if analysis.get("quality_floor"):
                print(f"\n  Optimal quality floor: {analysis['quality_floor']}")
            if analysis.get("regime_multipliers"):
                print(f"  Regime multipliers: {analysis['regime_multipliers']}")

            # Create recommendation
            rec = create_recommendation(db, strategy)
            if rec:
                print(f"\n  Created recommendation #{rec.id} (status: pending)")
                print(f"  Run: python scripts/recalibrate.py approve --id {rec.id}")
    finally:
        db.close()


def cmd_show(args):
    """Show recommendations."""
    db = SessionLocal()
    try:
        if args.id:
            from app.models import WeightRecommendation
            rec = db.query(WeightRecommendation).filter(WeightRecommendation.id == args.id).first()
            if not rec:
                print(f"Recommendation #{args.id} not found.")
                return
            print(f"\nRecommendation #{rec.id}")
            print(f"  Strategy:    {rec.strategy}")
            print(f"  Status:      {rec.status}")
            print(f"  Trades:      {rec.n_trades_analyzed}")
            print(f"  Win Rate:    {rec.overall_win_rate:.1%}")
            print(f"  Created:     {rec.created_at}")
            if rec.approved_at:
                print(f"  Approved:    {rec.approved_at}")
            if rec.notes:
                print(f"  Notes:       {rec.notes}")
            print(f"\n  Current weights:")
            for k, v in (rec.current_weights or {}).items():
                print(f"    {k:<16} {v:.2%}")
            print(f"\n  Recommended weights:")
            for k, v in (rec.recommended_weights or {}).items():
                print(f"    {k:<16} {v:.2%}")
            if rec.factor_analysis:
                print(f"\n  Factor analysis:")
                print(f"  {json.dumps(rec.factor_analysis, indent=2)}")
        else:
            recs = get_all_recommendations(db, status=args.status)
            if not recs:
                print("No recommendations found.")
                return
            print(f"\n{'ID':>4}  {'Strategy':<12}  {'Status':<10}  {'Trades':>7}  {'Win Rate':>9}  {'Created'}")
            print(f"{'-'*4}  {'-'*12}  {'-'*10}  {'-'*7}  {'-'*9}  {'-'*20}")
            for rec in recs:
                print(
                    f"{rec.id:>4}  {rec.strategy:<12}  {rec.status:<10}  "
                    f"{rec.n_trades_analyzed:>7}  {rec.overall_win_rate:>8.1%}  "
                    f"{rec.created_at}"
                )
    finally:
        db.close()


def cmd_approve(args):
    """Approve and apply a recommendation."""
    if not args.id:
        print("Error: --id is required for approve.")
        return
    db = SessionLocal()
    try:
        active = apply_recommendation(db, args.id)
        print(f"Approved recommendation #{args.id}")
        print(f"Active weights for {active.strategy} (v{active.version}):")
        for k, v in (active.weights or {}).items():
            print(f"  {k:<16} {v:.2%}")
    except ValueError as e:
        print(f"Error: {e}")
    finally:
        db.close()


def cmd_reject(args):
    """Reject a recommendation."""
    if not args.id:
        print("Error: --id is required for reject.")
        return
    db = SessionLocal()
    try:
        rec = reject_recommendation(db, args.id, notes=args.notes or "")
        print(f"Rejected recommendation #{rec.id}")
    except ValueError as e:
        print(f"Error: {e}")
    finally:
        db.close()


def cmd_status(args):
    """Show active weights vs hardcoded defaults."""
    db = SessionLocal()
    try:
        for strategy in ["momentum", "reversion"]:
            print(f"\n{'='*50}")
            print(f"  {strategy.upper()}")
            print(f"{'='*50}")

            active = get_active_weights(db, strategy)
            defaults = DEFAULT_WEIGHTS[strategy]

            if active:
                print(f"  Source: ActiveWeight v{active.version} (rec #{active.recommendation_id})")
                print(f"  Activated: {active.activated_at}")
                if active.quality_floor:
                    print(f"  Quality floor: {active.quality_floor}")
                if active.regime_multipliers:
                    print(f"  Regime multipliers: {active.regime_multipliers}")
                print(f"\n  {'Factor':<16} {'Active':>8}  {'Default':>8}  {'Delta':>8}")
                print(f"  {'-'*16} {'-'*8}  {'-'*8}  {'-'*8}")
                for f in defaults:
                    a = active.weights.get(f, 0)
                    d = defaults[f]
                    delta = a - d
                    print(f"  {f:<16} {a:>8.2%}  {d:>8.2%}  {delta:>+8.2%}")
            else:
                print(f"  Source: hardcoded defaults (no active weights)")
                for f, w in defaults.items():
                    print(f"    {f:<16} {w:.2%}")
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(
        description="Gemini STST — Quality Recalibration CLI",
    )
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # analyze
    p_analyze = subparsers.add_parser("analyze", help="Run factor analysis")
    p_analyze.add_argument("--strategy", choices=["momentum", "reversion"])

    # show
    p_show = subparsers.add_parser("show", help="Show recommendations")
    p_show.add_argument("--id", type=int)
    p_show.add_argument("--status", choices=["pending", "approved", "rejected"])

    # approve
    p_approve = subparsers.add_parser("approve", help="Approve a recommendation")
    p_approve.add_argument("--id", type=int, required=True)

    # reject
    p_reject = subparsers.add_parser("reject", help="Reject a recommendation")
    p_reject.add_argument("--id", type=int, required=True)
    p_reject.add_argument("--notes", type=str, default="")

    # status
    subparsers.add_parser("status", help="Show active vs default weights")

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        return

    commands = {
        "analyze": cmd_analyze,
        "show": cmd_show,
        "approve": cmd_approve,
        "reject": cmd_reject,
        "status": cmd_status,
    }
    commands[args.command](args)


if __name__ == "__main__":
    main()
