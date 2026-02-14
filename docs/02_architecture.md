# System Architecture

### 1. Tech Stack
* **Backend:** Python 3.10+ using `FastAPI`.
* **Frontend:** Vanilla HTML, CSS, and JavaScript (No React/Vue overhead).
* **Charting Library:** `Lightweight Charts` (by TradingView) via CDN for rendering candlestick and volume charts in JS.
* **Quantitative Engine:** `pandas`, `numpy`, and `TA-Lib` (for calculating ATR, RVOL, SMA).
* **Backtesting Engine:** `vectorbt` (Highly optimized library for backtesting DataFrames).
* **Data Providers:** Polygon.io API (Paid Tier - No rate limits) and Finnhub API (News/Catalysts).



### 2. Database & Deployment
* **Hosting:** Heroku (Upgraded Dyno Tier: Standard-2X or Performance-M for higher RAM allowances during backtesting).
* **Automation:** Heroku Scheduler (Cron job to run the screener daily).
* **Database:** **Heroku Postgres**. 
    * *CRITICAL RULE:* Do NOT use SQLite. Heroku utilizes an ephemeral filesystem. All daily screening results and logs must be written directly to the PostgreSQL database so they persist across dyno restarts.

### 3. Data Flow
1.  **Backend (Cron):** Python script fetches Polygon data via high-speed asynchronous requests, calculates indicators, filters the universe, and writes matches to Postgres.
2.  **Backend (API):** FastAPI serves REST endpoints (e.g., `/api/screener/latest`, `/api/backtest/results`).
3.  **Frontend (JS):** `app.js` fetches JSON from the FastAPI endpoints and dynamically renders the UI.