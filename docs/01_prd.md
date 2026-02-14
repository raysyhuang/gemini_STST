# Product Requirements Document (PRD)
## Project: QuantScreener - Short-Term Momentum Dashboard

### 1. Objective
Build an automated, web-based quantitative stock screener and backtesting dashboard. The primary goal is to identify short-term momentum swing trades (7-10 day hold times) on the NYSE and NASDAQ, targeting a 7-10% return. 

### 2. Core Quantitative Strategy
To achieve a reliable hit rate for a 10% move in a week, the screener strictly filters for explosive momentum and manages downside risk aggressively.
* **Liquidity:** Price > $5.00 AND Average Daily Volume (ADV) > 1,500,000.
* **Volatility (ATR):** Projected weekly Average True Range (ATR) > 8%. The stock must mathematically possess the volatility to reach the target.
* **Momentum (RVOL):** Relative Volume (RVOL) > 2.0 on the trigger day, indicating institutional accumulation or a fresh catalyst.
* **Market Regime Filter:** The system must check the SPY/QQQ trends. If the broader market is trading below its 20-day SMA, the screener must flag a "Bearish Regime" warning.

### 3. Risk Management (Strict Enforcement)
* **Asymmetric Risk/Reward:** Capturing a 7-10% return consistently requires downside protection. The backtesting engine and the forward-looking logic MUST implement a strict hard stop-loss of 3%. 

### 4. Required Features
* **Daily Automated Screening:** Fetches daily OHLCV data using the Polygon API.
* **Historical Backtesting:** Simulates the strategy over a 2-3 year historical period using `vectorbt` to calculate Win Rate, Max Drawdown, and Profit Factor.
* **Interactive Dashboard:** A clean HTML/JS frontend to visualize the daily screener results, display backtest equity curves, and render interactive candlestick charts.