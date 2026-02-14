# AI Developer Instructions & Execution Plan

**ROLE:** You are an Expert Python Quant Developer and Full-Stack Engineer. 
**OPERATING RULE:** Do not generate the entire codebase at once. We will build this iteratively based on the phases below. Before writing code for a phase, briefly explain your approach and wait for my approval. Read `docs/01_prd.md` and `docs/02_architecture.md` before starting.

**CRITICAL ENGINEERING CONSTRAINTS:**
1.  **Data Fetching:** We are using a **Paid Polygon API tier** (no 5/min rate limits). You can use high-concurrency `asyncio` requests or the bulk `Aggregates` endpoints to fetch market data at maximum speed.
2.  **Memory Management:** While we have upgraded Heroku RAM, `pandas` and `vectorbt` can still cause memory spikes (`R14` errors). Process historical backtest data in reasonable batches (e.g., 500-1000 tickers at a time), run the simulation, save the metrics, and run `gc.collect()` before moving to the next batch.
3.  **No Terminal Flooding:** Never print raw DataFrames containing thousands of rows to the terminal. Only use `.head()`, `.tail()`, or print array lengths to protect context limits.

### Phase 1: Backend Foundation & API Integration
1.  Set up the Python virtual environment and `requirements.txt`.
2.  Create `.env` loading for credentials.
3.  Build the data acquisition module: Asynchronous functions to fetch 2 years of daily OHLCV data for all NYSE/NASDAQ tickers from Polygon at high speed.
4.  Set up the SQLAlchemy Postgres models.

### Phase 2: Quantitative Engine & Backtesting
1.  Build the indicator module using `pandas` to calculate RVOL, ATR (%), and the SPY Market Regime SMA.
2.  Implement the screening logic function.
3.  Implement the `vectorbt` backtesting module with batch processing to manage memory. Feed it the data, apply the buy signal (RVOL > 2, ATR > 8%), a 7-day time exit, and a strict 3% stop-loss. 

### Phase 3: FastAPI Endpoints
1.  Build the `main.py` FastAPI application.
2.  Create endpoints: `GET /api/screener/today` and `GET /api/backtest/{ticker}`.
3.  Configure CORS.

### Phase 4: Vanilla JS / HTML Frontend
1.  Create a `static/` directory with `index.html`, `style.css`, and `app.js`.
2.  Configure FastAPI to serve static files.
3.  In `app.js`, use `fetch()` to call the FastAPI endpoints.
4.  Integrate the TradingView Lightweight Charts library to render the backtest equity curves and daily candlestick charts.