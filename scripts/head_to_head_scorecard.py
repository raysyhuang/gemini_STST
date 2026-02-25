#!/usr/bin/env python3
"""
Strict head-to-head evaluator for Gemini STST vs KooCore-D.

Method:
1) Build date-aligned pick sets for both apps on the SAME scan dates.
2) Use KooCore-D's compute_hit10_backtest engine for BOTH apps.
3) Report normalized metrics and a winner by primary KPI (hit rate).
"""

from __future__ import annotations

import argparse
import math
import os
import sys
from collections import defaultdict
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any

from sqlalchemy import text

# Local app imports (Gemini STST)
from app.database import SessionLocal


def _dedup_keep_order(items: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        t = str(item).strip().upper()
        if not t or t in seen:
            continue
        out.append(t)
        seen.add(t)
    return out


def _load_gemini_picks(koocore_date_set: set[str]):
    """
    Build DatePicks-like objects for Gemini from DB signals.
    - weekly_top5 slot is mapped to momentum screener_signals.
    - pro30 slot is mapped to reversion_signals.
    """
    db = SessionLocal()
    try:
        momentum_rows = db.execute(text("""
            SELECT s.date, t.symbol
            FROM screener_signals s
            JOIN tickers t ON t.id = s.ticker_id
            ORDER BY s.date ASC, s.quality_score DESC NULLS LAST, t.symbol ASC
        """)).fetchall()

        reversion_rows = db.execute(text("""
            SELECT r.date, t.symbol
            FROM reversion_signals r
            JOIN tickers t ON t.id = r.ticker_id
            ORDER BY r.date ASC, r.quality_score DESC NULLS LAST, t.symbol ASC
        """)).fetchall()
    finally:
        db.close()

    by_date: dict[str, dict[str, list[str]]] = defaultdict(
        lambda: {"momentum": [], "reversion": []}
    )

    for d, sym in momentum_rows:
        ds = d.isoformat() if hasattr(d, "isoformat") else str(d)
        if ds in koocore_date_set:
            by_date[ds]["momentum"].append(str(sym).upper())

    for d, sym in reversion_rows:
        ds = d.isoformat() if hasattr(d, "isoformat") else str(d)
        if ds in koocore_date_set:
            by_date[ds]["reversion"].append(str(sym).upper())

    return by_date


def _confidence_95(hit_rate: float | None, n: int) -> float | None:
    if hit_rate is None or n <= 0:
        return None
    return 1.96 * math.sqrt((hit_rate * (1 - hit_rate)) / n)


def _fmt_pct(v: float | None) -> str:
    if v is None:
        return "NA"
    return f"{v * 100:.2f}%"


def _summarize(perf_by_component, label: str) -> dict[str, Any]:
    row = perf_by_component[perf_by_component["component"] == "all"].iloc[0]
    hit_rate = None if row["hit_rate"] != row["hit_rate"] else float(row["hit_rate"])
    n = int(row["n"])
    ci = _confidence_95(hit_rate, n)
    return {
        "label": label,
        "n": n,
        "hit_rate": hit_rate,
        "ci95": ci,
        "hit_rate_text": _fmt_pct(hit_rate),
        "ci95_text": "NA" if ci is None else f"+/- {ci * 100:.2f}%",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Strict head-to-head scorecard")
    parser.add_argument(
        "--koocore-root",
        default="/Users/rayhuang/Documents/Python Project/KooCore-D",
        help="Absolute path to KooCore-D project root",
    )
    parser.add_argument(
        "--koocore-outputs-root",
        default="/Users/rayhuang/Documents/Python Project/KooCore-D/outputs",
        help="KooCore outputs root containing YYYY-MM-DD folders",
    )
    parser.add_argument("--start", default=None, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", default=None, help="End date YYYY-MM-DD")
    parser.add_argument("--forward-days", type=int, default=7, help="Forward trading days")
    parser.add_argument("--threshold", type=float, default=10.0, help="Hit threshold percent")
    parser.add_argument(
        "--no-threads",
        action="store_true",
        help="Disable threaded yfinance download in evaluator",
    )
    args = parser.parse_args()

    koocore_root = Path(args.koocore_root).resolve()
    if not koocore_root.exists():
        raise SystemExit(f"KooCore root not found: {koocore_root}")

    sys.path.insert(0, str(koocore_root))
    from src.features.performance.backtest import (  # type: ignore
        DatePicks,
        compute_hit10_backtest,
        load_picks_in_range,
    )

    k_picks = load_picks_in_range(
        outputs_root=args.koocore_outputs_root,
        start_date=args.start,
        end_date=args.end,
    )
    if not k_picks:
        raise SystemExit("No KooCore picks found in the requested range.")

    k_dates = {p.date_str for p in k_picks}
    gem_by_date = _load_gemini_picks(k_dates)

    # Build Gemini DatePicks only on shared dates.
    g_picks = []
    for d in sorted(k_dates):
        payload = gem_by_date.get(d, {"momentum": [], "reversion": []})
        momentum = _dedup_keep_order(payload["momentum"])
        reversion = _dedup_keep_order(payload["reversion"])
        combined = _dedup_keep_order(momentum + reversion)
        if not combined:
            continue
        g_picks.append(
            DatePicks(
                date_str=d,
                weekly_top5=momentum,
                pro30=reversion,
                movers=[],
                combined=combined,
                sources={"weekly_top5": "gemini:screener_signals", "pro30": "gemini:reversion_signals", "movers": "n/a"},
            )
        )

    # Restrict KooCore to dates where Gemini has picks too (strict shared date set).
    shared_dates = {p.date_str for p in g_picks}
    if not shared_dates:
        raise SystemExit("No shared dates with Gemini picks were found.")
    k_picks = [p for p in k_picks if p.date_str in shared_dates]
    g_picks = [p for p in g_picks if p.date_str in shared_dates]

    print(f"Shared dates: {len(shared_dates)} ({min(shared_dates)} to {max(shared_dates)})")
    print(f"KooCore date picks: {len(k_picks)}")
    print(f"Gemini date picks: {len(g_picks)}")

    k_detail, k_by_date, k_by_component, _ = compute_hit10_backtest(
        k_picks,
        outputs_root=args.koocore_outputs_root,
        forward_trading_days=args.forward_days,
        hit_threshold_pct=args.threshold,
        use_high=True,
        exclude_entry_day=True,
        auto_adjust=False,
        threads=not args.no_threads,
    )
    g_detail, g_by_date, g_by_component, _ = compute_hit10_backtest(
        g_picks,
        outputs_root="/tmp",  # no feature join needed for Gemini picks
        forward_trading_days=args.forward_days,
        hit_threshold_pct=args.threshold,
        use_high=True,
        exclude_entry_day=True,
        auto_adjust=False,
        threads=not args.no_threads,
    )

    k_sum = _summarize(k_by_component, "KooCore-D")
    g_sum = _summarize(g_by_component, "Gemini STST")

    print("\nStrict Scorecard (same metric engine)")
    print(f"- KooCore-D: n={k_sum['n']}, hit={k_sum['hit_rate_text']}, CI95={k_sum['ci95_text']}")
    print(f"- Gemini STST: n={g_sum['n']}, hit={g_sum['hit_rate_text']}, CI95={g_sum['ci95_text']}")

    winner = None
    if k_sum["hit_rate"] is not None and g_sum["hit_rate"] is not None:
        winner = "KooCore-D" if k_sum["hit_rate"] > g_sum["hit_rate"] else "Gemini STST"

    print(f"\nWinner (primary KPI: hit rate): {winner or 'NA'}")

    # Optional machine-readable dump for reuse.
    payload = {
        "run_date": date.today().isoformat(),
        "settings": {
            "start": args.start,
            "end": args.end,
            "forward_days": args.forward_days,
            "threshold": args.threshold,
            "shared_dates_count": len(shared_dates),
            "shared_date_min": min(shared_dates),
            "shared_date_max": max(shared_dates),
        },
        "koocore": k_sum,
        "gemini": g_sum,
        "winner": winner,
        "rows": {
            "koocore_perf_detail_rows": int(len(k_detail)),
            "gemini_perf_detail_rows": int(len(g_detail)),
            "koocore_perf_by_date_rows": int(len(k_by_date)),
            "gemini_perf_by_date_rows": int(len(g_by_date)),
        },
    }

    out_path = Path("/tmp/head_to_head_scorecard.json")
    out_path.write_text(str(payload), encoding="utf-8")
    print(f"\nSaved raw summary to: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
