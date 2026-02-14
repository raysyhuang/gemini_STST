# VectorBT Backtesting Logic & Engine

### 1. The Core Challenge: Vectorized Path Dependency
We need to simulate a strategy that holds a stock for exactly 7 days, but will abort the trade early if the price drops by 3%. To achieve this in a vectorized environment, we map out our `entries` and `exits` as boolean matrices, and pass the stop-loss as an explicit parameter to the portfolio engine.

### 2. Signal Generation Logic
* **Entries:** Create a boolean mask across the dataframe where `(RVOL > 2.0)` AND `(ATR_PCT > 8.0)`.
* **Exits (Time-Based):** Shift the entry boolean mask forward by 7 trading days using pandas (`entries.shift(7)`). If an entry occurs on day $T$, the exit signal triggers on day $T+7$.
* **Stop-Loss (Risk Management):** Handled natively by the `vbt.Portfolio.from_signals` function using the `sl_stop=0.03` parameter. If the 3% drop hits before the 7th day, the engine closes the position and ignores the subsequent time-based exit signal.

### 3. Batch Processing Architecture (Memory Safety)
To prevent Heroku from throwing an `R14 Memory Quota Exceeded` error when analyzing 2 years of data for over 6,000 stocks, you must build a batch processor.
1.  Query the Postgres `tickers` table to get all active symbols.
2.  Slice the symbols into chunks of 500.
3.  For each chunk, pull the `daily_market_data` into a pandas DataFrame, pivot it so tickers are columns and dates are rows, and run the `vectorbt` portfolio simulation.
4.  Extract the metrics (Win Rate, Total Return, Max Drawdown), save the results, and run `del df` and `gc.collect()` before moving to the next chunk.

### 4. Implementation Code Blueprint
Use this logical flow when constructing the backtesting module:

```python
import vectorbt as vbt
import pandas as pd
import gc

def run_batch_backtest(price_df, rvol_df, atr_df):
    # 1. Generate Boolean Entry Signals
    entries = (rvol_df > 2.0) & (atr_df > 8.0)
    
    # 2. Generate Time-Based Exits (7 days later)
    # Using shift(7) means if we buy on day T, we sell on day T+7
    exits = entries.shift(7).fillna(False)
    
    # 3. Run the VectorBT Portfolio Simulation
    # sl_stop=0.03 automatically handles the 3% hard stop-loss path dependency
    portfolio = vbt.Portfolio.from_signals(
        close=price_df,
        entries=entries,
        exits=exits,
        sl_stop=0.03,
        freq='1D',
        fees=0.001 # Simulate basic slippage/fees for realistic results
    )
    
    # 4. Extract Metrics
    stats = portfolio.stats()
    
    # 5. Clear memory strictly
    del entries, exits, portfolio
    gc.collect()
    
    return stats