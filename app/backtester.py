"""
VectorBT backtesting engine with memory-safe batch processing.

Strategy:
  - Entry  : RVOL > 2.0  AND  ATR% > 8.0
  - Exit   : 7 trading days after entry (time-based)
  - Stop   : 3% hard stop-loss (sl_stop=0.03) — exits early if triggered

Architecture:
  - Tickers are processed in batches of 500 to stay under Heroku RAM limits.
  - Each batch: pivot data → run vbt simulation → extract metrics → gc.collect().
  - Results are returned as a list of per-ticker metric dicts.
"""

import gc
import logging
from datetime import date, timedelta

import numpy as np
import pandas as pd
import vectorbt as vbt
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import Ticker, DailyMarketData
from app.indicators import compute_atr_pct, compute_rvol

logger = logging.getLogger(__name__)

BATCH_SIZE = 500
HOLD_DAYS = 7
STOP_LOSS = 0.03   # 3% hard stop-loss — STRICT REQUIREMENT
FEES = 0.001        # 0.1% round-trip for slippage/commissions


# ------------------------------------------------------------------
# Data loading helpers
# ------------------------------------------------------------------

def _load_batch_data(
    db: Session,
    ticker_ids: list[int],
    from_date: date,
    to_date: date,
) -> pd.DataFrame:
    """
    Load OHLCV for a batch of ticker_ids into a single DataFrame.
    Returns columns: ticker_id, date, open, high, low, close, volume.
    """
    stmt = text("""
        SELECT ticker_id, date, open, high, low, close, volume
        FROM daily_market_data
        WHERE ticker_id = ANY(:ids)
          AND date BETWEEN :start AND :end
        ORDER BY date ASC
    """)
    result = db.execute(stmt, {"ids": ticker_ids, "start": from_date, "end": to_date})
    rows = result.fetchall()
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=["ticker_id", "date", "open", "high", "low", "close", "volume"])
    df["date"] = pd.to_datetime(df["date"])
    return df


def _pivot_column(df: pd.DataFrame, column: str, id_to_symbol: dict[int, str]) -> pd.DataFrame:
    """Pivot a long DataFrame into a wide matrix: index=date, columns=ticker symbols."""
    pivot = df.pivot_table(index="date", columns="ticker_id", values=column)
    pivot.columns = [id_to_symbol.get(c, str(c)) for c in pivot.columns]
    return pivot


# ------------------------------------------------------------------
# Single-batch backtest
# ------------------------------------------------------------------

def _run_batch(
    price_df: pd.DataFrame,
    rvol_df: pd.DataFrame,
    atr_pct_df: pd.DataFrame,
) -> list[dict]:
    """
    Run the VectorBT simulation on a batch of tickers (wide DataFrames).

    Returns a list of per-ticker metric dicts.
    """
    # Align all DataFrames to the same date index (drop leading NaN rows
    # from indicator rolling windows so shapes match for vectorbt broadcast)
    common_idx = price_df.dropna(how="all").index \
        .intersection(rvol_df.dropna(how="all").index) \
        .intersection(atr_pct_df.dropna(how="all").index)
    price_df = price_df.loc[common_idx]
    rvol_df = rvol_df.loc[common_idx]
    atr_pct_df = atr_pct_df.loc[common_idx]

    if price_df.empty:
        return []

    # 1. Entry signals: RVOL > 2.0 AND ATR% > 8.0
    entries = (rvol_df > 2.0) & (atr_pct_df > 8.0)

    # 2. Time-based exits: 7 trading days after entry
    exits = entries.shift(HOLD_DAYS).fillna(False).infer_objects(copy=False)

    # 3. Run the portfolio simulation with 3% hard stop-loss
    portfolio = vbt.Portfolio.from_signals(
        close=price_df,
        entries=entries,
        exits=exits,
        sl_stop=STOP_LOSS,
        freq="1D",
        fees=FEES,
        init_cash=10_000,
        accumulate=False,     # One position at a time per ticker
    )

    # 4. Extract per-ticker metrics
    results: list[dict] = []
    single_col = len(price_df.columns) == 1

    for ticker_col in price_df.columns:
        try:
            # For a single-column portfolio, indexing by column name
            # still returns the right sub-portfolio in vbt 0.28+
            col_pf = portfolio if single_col else portfolio[ticker_col]

            stats = col_pf.stats()
            # stats may be a DataFrame for multi-col; extract the column's row
            if isinstance(stats, pd.DataFrame):
                stats = stats[ticker_col] if ticker_col in stats.columns else stats.iloc[:, 0]
            total_return = stats.get("Total Return [%]", 0.0)
            max_dd = stats.get("Max Drawdown [%]", 0.0)
            win_rate = stats.get("Win Rate [%]", 0.0)

            # Profit factor: gross profit / gross loss
            trades = col_pf.trades.records_readable
            if len(trades) > 0:
                profits = trades.loc[trades["PnL"] > 0, "PnL"].sum()
                losses = abs(trades.loc[trades["PnL"] < 0, "PnL"].sum())
                profit_factor = round(profits / losses, 2) if losses > 0 else float("inf")
            else:
                profit_factor = 0.0

            # Equity curve formatted for TradingView Lightweight Charts
            equity = col_pf.value()
            # .value() may return a DataFrame for single-column portfolios;
            # squeeze to a Series so we iterate (date, value) pairs.
            if isinstance(equity, pd.DataFrame):
                equity = equity.iloc[:, 0]
            equity_curve = [
                {
                    "time": pd.Timestamp(ts).strftime("%Y-%m-%d"),
                    "value": round(float(val), 2),
                }
                for ts, val in equity.items()
            ]

            def _safe(v, decimals=2):
                f = float(v)
                return round(f, decimals) if not np.isnan(f) else 0.0

            results.append({
                "ticker": ticker_col,
                "total_return_pct": _safe(total_return),
                "max_drawdown_pct": _safe(max_dd),
                "win_rate": _safe(win_rate, 1),
                "profit_factor": profit_factor,
                "total_trades": len(trades),
                "equity_curve": equity_curve,
            })
        except Exception as e:
            logger.warning("Metrics extraction failed for %s: %s", ticker_col, e)

    # 5. Cleanup
    del entries, exits, portfolio
    gc.collect()

    return results


# ------------------------------------------------------------------
# Per-ticker indicator computation on wide DataFrames
# ------------------------------------------------------------------

def _compute_wide_indicators(df: pd.DataFrame, id_to_symbol: dict) -> tuple:
    """
    Given a long-format DataFrame, compute per-ticker indicators and return
    wide (pivoted) DataFrames: price_df, rvol_df, atr_pct_df.
    """
    all_rvol = []
    all_atr_pct = []

    for tid, group in df.groupby("ticker_id"):
        group = group.sort_values("date").reset_index(drop=True)
        group["rvol"] = compute_rvol(group)
        group["atr_pct"] = compute_atr_pct(group)
        all_rvol.append(group[["date", "ticker_id", "rvol"]])
        all_atr_pct.append(group[["date", "ticker_id", "atr_pct"]])

    rvol_long = pd.concat(all_rvol, ignore_index=True)
    atr_long = pd.concat(all_atr_pct, ignore_index=True)

    # Merge indicators back into the main df for pivoting
    df = df.merge(rvol_long, on=["date", "ticker_id"], how="left")
    df = df.merge(atr_long, on=["date", "ticker_id"], how="left")

    price_df = _pivot_column(df, "close", id_to_symbol)
    rvol_df = _pivot_column(df, "rvol", id_to_symbol)
    atr_pct_df = _pivot_column(df, "atr_pct", id_to_symbol)

    return price_df, rvol_df, atr_pct_df


# ------------------------------------------------------------------
# Full backtest orchestrator
# ------------------------------------------------------------------

def run_full_backtest(years_back: int = 2) -> list[dict]:
    """
    Run the momentum backtest across all active tickers in batches of 500.

    Returns a flat list of per-ticker result dicts.
    """
    to_date = date.today()
    from_date = to_date - timedelta(days=365 * years_back)

    db = SessionLocal()
    try:
        all_tickers = db.query(Ticker).filter(Ticker.is_active.is_(True)).all()
        id_to_symbol = {t.id: t.symbol for t in all_tickers}
        ticker_ids = list(id_to_symbol.keys())
    finally:
        db.close()

    logger.info(
        "Starting backtest: %d tickers, %s → %s, batches of %d",
        len(ticker_ids), from_date, to_date, BATCH_SIZE,
    )

    all_results: list[dict] = []

    for i in range(0, len(ticker_ids), BATCH_SIZE):
        batch_ids = ticker_ids[i : i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        logger.info("Batch %d: processing %d tickers...", batch_num, len(batch_ids))

        db = SessionLocal()
        try:
            raw_df = _load_batch_data(db, batch_ids, from_date, to_date)
        finally:
            db.close()

        if raw_df.empty:
            logger.info("Batch %d: no data, skipping", batch_num)
            continue

        price_df, rvol_df, atr_pct_df = _compute_wide_indicators(raw_df, id_to_symbol)

        # Free the raw data before running the simulation
        del raw_df
        gc.collect()

        batch_results = _run_batch(price_df, rvol_df, atr_pct_df)
        all_results.extend(batch_results)
        logger.info("Batch %d: got metrics for %d tickers", batch_num, len(batch_results))

        # Memory safety: clear batch DataFrames
        del price_df, rvol_df, atr_pct_df
        gc.collect()

    logger.info("Backtest complete. Results for %d tickers.", len(all_results))
    return all_results


def run_single_ticker_backtest(symbol: str, years_back: int = 2) -> dict | None:
    """
    Run the backtest for a single ticker. Used by the /api/backtest/{ticker} endpoint.
    Returns a single result dict or None if insufficient data.
    """
    to_date = date.today()
    from_date = to_date - timedelta(days=365 * years_back)

    db = SessionLocal()
    try:
        tkr = db.query(Ticker).filter(Ticker.symbol == symbol).first()
        if not tkr:
            return None

        raw_df = _load_batch_data(db, [tkr.id], from_date, to_date)
    finally:
        db.close()

    if raw_df.empty or len(raw_df) < 30:
        return None

    id_to_symbol = {tkr.id: tkr.symbol}
    price_df, rvol_df, atr_pct_df = _compute_wide_indicators(raw_df, id_to_symbol)
    del raw_df

    results = _run_batch(price_df, rvol_df, atr_pct_df)
    return results[0] if results else None
