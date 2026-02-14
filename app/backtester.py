"""
VectorBT backtesting engine with memory-safe batch processing.

Dual-strategy support:
  Momentum:
    - Entry  : RVOL > 2.0  AND  ATR% > 8.0
    - Exit   : 7 trading days after entry (time-based)
    - Stop   : Chandelier trailing stop (2 * ATR via sl_trail=True)

  Mean Reversion:
    - Entry  : RSI(2) < 10  AND  3-day drawdown >= 15%
    - Exit   : 5 trading days after entry (time-based)
    - Stop   : 5% hard stop-loss

Both strategies execute at T+1 open with 20 bps slippage (no look-ahead bias).

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
from app.indicators import compute_atr_pct, compute_rvol, compute_rsi

logger = logging.getLogger(__name__)

BATCH_SIZE = 500
FEES = 0.001        # 0.1% round-trip commissions
SLIPPAGE = 0.002    # 20 bps per side — realistic for small-cap spreads
SIZE = 0.10         # 10% of equity per trade (fixed-fractional sizing)

# Momentum strategy
MOMENTUM_HOLD_DAYS = 7
# Trailing stop: sl_stop = 2 * ATR(14) / close with sl_trail=True (Chandelier exit)
# For ATR%=10% stocks, trail distance ≈ 8.9% from highest high since entry

# Mean Reversion strategy
REVERSION_HOLD_DAYS = 5
REVERSION_STOP = 0.05      # 5% hard stop-loss


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
    open_df: pd.DataFrame,
    rvol_df: pd.DataFrame,
    atr_pct_df: pd.DataFrame,
    strategy_type: str = "momentum",
    rsi2_df: pd.DataFrame | None = None,
    drawdown_3d_df: pd.DataFrame | None = None,
) -> list[dict]:
    """
    Run the VectorBT simulation on a batch of tickers (wide DataFrames).

    strategy_type:
      "momentum"  — RVOL > 2.0 AND ATR% > 8.0, 7-day hold, Chandelier trailing stop
      "reversion" — RSI(2) < 10 AND 3-day drawdown >= 15%, 5-day hold, 5% hard stop

    Returns a list of per-ticker metric dicts.
    """
    # Align all DataFrames to the same date index (drop leading NaN rows
    # from indicator rolling windows so shapes match for vectorbt broadcast)
    dfs_to_align = [price_df, open_df, atr_pct_df]
    if strategy_type == "momentum":
        dfs_to_align.append(rvol_df)
    if rsi2_df is not None:
        dfs_to_align.append(rsi2_df)
    if drawdown_3d_df is not None:
        dfs_to_align.append(drawdown_3d_df)

    common_idx = dfs_to_align[0].dropna(how="all").index
    for _df in dfs_to_align[1:]:
        common_idx = common_idx.intersection(_df.dropna(how="all").index)

    price_df = price_df.loc[common_idx]
    open_df = open_df.loc[common_idx]
    atr_pct_df = atr_pct_df.loc[common_idx]
    if strategy_type == "momentum":
        rvol_df = rvol_df.loc[common_idx]
    if rsi2_df is not None:
        rsi2_df = rsi2_df.loc[common_idx]
    if drawdown_3d_df is not None:
        drawdown_3d_df = drawdown_3d_df.loc[common_idx]

    if price_df.empty:
        return []

    # Build entry/exit signals and run portfolio based on strategy
    if strategy_type == "reversion":
        # Mean Reversion: RSI(2) < 10 AND 3-day drawdown >= 15%
        raw_entries = (rsi2_df < 10.0) & (drawdown_3d_df <= -0.15)
        entries = raw_entries.shift(1).fillna(False).astype(bool)
        exits = entries.shift(REVERSION_HOLD_DAYS).fillna(False).astype(bool)

        portfolio = vbt.Portfolio.from_signals(
            close=price_df,
            open=open_df,
            entries=entries,
            exits=exits,
            sl_stop=REVERSION_STOP,
            freq="1D",
            fees=FEES,
            slippage=SLIPPAGE,
            init_cash=10_000,
            size=SIZE,
            size_type="percent",
            accumulate=False,
        )
    else:
        # Momentum: RVOL > 2.0 AND ATR% > 8.0
        raw_entries = (rvol_df > 2.0) & (atr_pct_df > 8.0)
        entries = raw_entries.shift(1).fillna(False).astype(bool)
        exits = entries.shift(MOMENTUM_HOLD_DAYS).fillna(False).astype(bool)

        # Chandelier trailing stop: 2 * daily ATR as fraction of price
        # ATR% = (ATR/close) * sqrt(5) * 100  →  daily ATR/close = ATR%/(sqrt(5)*100)
        # sl_trail=True makes sl_stop trail from highest high since entry
        chandelier_df = 2.0 * atr_pct_df / (np.sqrt(5) * 100.0)

        portfolio = vbt.Portfolio.from_signals(
            close=price_df,
            open=open_df,
            entries=entries,
            exits=exits,
            sl_stop=chandelier_df,
            sl_trail=True,
            freq="1D",
            fees=FEES,
            slippage=SLIPPAGE,
            init_cash=10_000,
            size=SIZE,
            size_type="percent",
            accumulate=False,
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

def _compute_wide_indicators(
    df: pd.DataFrame,
    id_to_symbol: dict,
    strategy_type: str = "momentum",
) -> tuple:
    """
    Given a long-format DataFrame, compute per-ticker indicators and return
    wide (pivoted) DataFrames.

    For momentum:  returns (price_df, open_df, rvol_df, atr_pct_df)
    For reversion: returns (price_df, open_df, rvol_df, atr_pct_df, rsi2_df, drawdown_3d_df)
    """
    all_rvol = []
    all_atr_pct = []
    all_rsi2 = []
    all_drawdown_3d = []

    for tid, group in df.groupby("ticker_id"):
        group = group.sort_values("date").reset_index(drop=True)
        group["rvol"] = compute_rvol(group)
        group["atr_pct"] = compute_atr_pct(group)
        all_rvol.append(group[["date", "ticker_id", "rvol"]])
        all_atr_pct.append(group[["date", "ticker_id", "atr_pct"]])

        if strategy_type == "reversion":
            group["rsi2"] = compute_rsi(group, period=2)
            group["drawdown_3d"] = (group["close"] / group["close"].shift(3)) - 1.0
            all_rsi2.append(group[["date", "ticker_id", "rsi2"]])
            all_drawdown_3d.append(group[["date", "ticker_id", "drawdown_3d"]])

    rvol_long = pd.concat(all_rvol, ignore_index=True)
    atr_long = pd.concat(all_atr_pct, ignore_index=True)

    # Merge indicators back into the main df for pivoting
    df = df.merge(rvol_long, on=["date", "ticker_id"], how="left")
    df = df.merge(atr_long, on=["date", "ticker_id"], how="left")

    price_df = _pivot_column(df, "close", id_to_symbol)
    open_df = _pivot_column(df, "open", id_to_symbol)
    rvol_df = _pivot_column(df, "rvol", id_to_symbol)
    atr_pct_df = _pivot_column(df, "atr_pct", id_to_symbol)

    if strategy_type == "reversion":
        rsi2_long = pd.concat(all_rsi2, ignore_index=True)
        drawdown_long = pd.concat(all_drawdown_3d, ignore_index=True)
        df = df.merge(rsi2_long, on=["date", "ticker_id"], how="left")
        df = df.merge(drawdown_long, on=["date", "ticker_id"], how="left")
        rsi2_df = _pivot_column(df, "rsi2", id_to_symbol)
        drawdown_3d_df = _pivot_column(df, "drawdown_3d", id_to_symbol)
        return price_df, open_df, rvol_df, atr_pct_df, rsi2_df, drawdown_3d_df

    return price_df, open_df, rvol_df, atr_pct_df


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

        price_df, open_df, rvol_df, atr_pct_df = _compute_wide_indicators(raw_df, id_to_symbol)

        # Free the raw data before running the simulation
        del raw_df
        gc.collect()

        batch_results = _run_batch(price_df, open_df, rvol_df, atr_pct_df)
        all_results.extend(batch_results)
        logger.info("Batch %d: got metrics for %d tickers", batch_num, len(batch_results))

        # Memory safety: clear batch DataFrames
        del price_df, open_df, rvol_df, atr_pct_df
        gc.collect()

    logger.info("Backtest complete. Results for %d tickers.", len(all_results))
    return all_results


def run_single_ticker_backtest(
    symbol: str,
    years_back: int = 2,
    strategy_type: str = "momentum",
) -> dict | None:
    """
    Run the backtest for a single ticker. Used by the /api/backtest/{ticker} endpoint.

    strategy_type: "momentum" (default) or "reversion"
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

    if strategy_type == "reversion":
        price_df, open_df, rvol_df, atr_pct_df, rsi2_df, drawdown_3d_df = \
            _compute_wide_indicators(raw_df, id_to_symbol, strategy_type="reversion")
        del raw_df
        results = _run_batch(
            price_df, open_df, rvol_df, atr_pct_df,
            strategy_type="reversion",
            rsi2_df=rsi2_df, drawdown_3d_df=drawdown_3d_df,
        )
    else:
        price_df, open_df, rvol_df, atr_pct_df = _compute_wide_indicators(raw_df, id_to_symbol)
        del raw_df
        results = _run_batch(price_df, open_df, rvol_df, atr_pct_df)

    return results[0] if results else None
