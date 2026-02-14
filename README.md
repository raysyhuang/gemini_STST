# QuantScreener — Institutional-Grade Dual-Strategy Screener

An automated, memory-safe quantitative stock screener featuring two distinct models: **7-day Momentum Breakouts** (RVOL + ATR% explosion) and **3-day RSI(2) Oversold Mean Reversions** — with honest backtesting, real-time Telegram alerts, and a dark-themed trading dashboard.

**Live:** [geministst-a76526147b8c.herokuapp.com](https://geministst-a76526147b8c.herokuapp.com/)

---

## The Edge: Honest Math

Most retail screeners backtest on closing prices and pretend you can execute at the exact moment a signal fires. We don't.

**QuantScreener enforces institutional-grade execution realism:**

| Parameter | Value | Why It Matters |
|---|---|---|
| **Stop-Loss** | 3% hard stop (`sl_stop=0.03`) | Non-negotiable downside cap on every trade |
| **Position Sizing** | 10% of equity per trade | Fixed-fractional sizing prevents ruin |
| **Slippage** | 20 bps per side (`0.002`) | Realistic for small-cap bid/ask spreads |
| **Commissions** | 10 bps round-trip (`0.001`) | Accounts for broker execution costs |
| **Look-Ahead Bias** | Eliminated | Signals fire on day T close; backtest executes at day **T+1 open** |
| **Accumulation** | Disabled | One position per ticker at a time — no pyramiding |

The P0 look-ahead fix shifts all entry signals forward by 1 day and executes at the next morning's open price. This is the single most important integrity guarantee in the system — it means our backtests show what you would *actually* have earned, not what a time-traveler could have earned.

---

## Dual-Strategy Architecture

### Engine 1: Momentum Breakouts

Catches stocks exploding out of consolidation on high volume.

| Filter | Threshold | Purpose |
|---|---|---|
| Price | > $5.00 | Exclude penny stocks |
| ADV (20-day) | > 1,500,000 | Ensure institutional liquidity |
| ATR% (weekly) | > 8% | Projected volatility worth trading |
| RVOL | > 2.0x | Volume surge confirms conviction |
| Trend Alignment | Close > SMA-20 | Don't buy falling knives |
| Green Candle | Close > Open | Buyers maintained control today |

**Hold:** 7 trading days with 3% stop-loss.

### Engine 2: Oversold Mean Reversions

Targets rubber-band snaps — quality names that fell hard and fast.

| Filter | Threshold | Purpose |
|---|---|---|
| Price | > $5.00 | Exclude penny stocks |
| ADV (20-day) | > 1,500,000 | Ensure institutional liquidity |
| RSI(2) | < 10 | Deeply oversold (Larry Connors method) |
| 3-Day Drawdown | >= 15% | Sharp, rapid selloff |
| Close > SMA-200 | Required | Long-term uptrend intact — not a broken stock |

Sorted by RSI ascending (most oversold first).

---

## Features

- **Market Regime Filter** — SPY & QQQ must both trade above their 20-day SMA for a Bullish reading. Bearish regime triggers a caution banner on the dashboard.
- **Earnings Event Exclusion** — Finnhub `/calendar/earnings` API automatically blacklists any stock with an earnings report within the 7-day hold window. No binary event risk.
- **5-Day Signal Cooldown** — Once a ticker fires a momentum signal, it's suppressed for 5 trading days to prevent the same breakout from re-triggering.
- **Telegram Bot Alerts** — End-of-day summary with regime status, signal details (RVOL, ATR%), and top Finnhub news headlines per ticker.
- **TradingView Equity Curves** — Click any ticker to load its 2-year VectorBT backtest with an interactive Lightweight Charts equity plot.
- **Frontend Strategy Toggle** — Switch between Momentum Breakouts and Oversold Reversions directly from the dashboard sidebar.

---

## Dashboard

![Dashboard Screenshot](docs/dashboard-screenshot.png)
*Dark-themed trading dashboard with strategy toggle, regime banners, and TradingView equity curves.*

![Telegram Alert Screenshot](docs/telegram-alert-screenshot.png)
*Automated daily Telegram alert with signal details and news catalysts.*

---

## Architecture

```
                    Heroku Scheduler (daily cron)
                              |
                    +---------v----------+
                    | app/data_fetcher   |  Polygon.io API
                    | (async, batched)   |  ~5,000 tickers
                    +---------+----------+
                              |
                    +---------v----------+
                    |  Heroku Postgres   |  OHLCV + Signals
                    +---------+----------+
                              |
              +---------------+---------------+
              |                               |
    +---------v----------+          +---------v----------+
    |  app/screener      |          | app/mean_reversion |
    |  (Momentum)        |          | (RSI-2 Oversold)   |
    +---------+----------+          +---------+----------+
              |                               |
              +-------+-----------+-----------+
                      |           |
            +---------v--+  +----v-----------+
            | app/notifier|  | app/backtester |
            | (Telegram)  |  | (VectorBT)     |
            +-------------+  +-------+--------+
                                      |
                              +-------v--------+
                              | FastAPI + JS   |
                              | Dashboard      |
                              +----------------+
```

### Tech Stack

| Layer | Technology |
|---|---|
| API | FastAPI 0.110, Uvicorn |
| Database | Heroku Postgres, SQLAlchemy 2.0 |
| Data Source | Polygon.io (paid tier, no rate limits) |
| News & Earnings | Finnhub API |
| Indicators | Pure pandas (ATR, RVOL, RSI, SMA) |
| Backtesting | VectorBT 0.26.2 |
| Frontend | Vanilla JS, TradingView Lightweight Charts 3.8 |
| Alerts | Telegram Bot API |
| Hosting | Heroku (512 MB dyno) |

### Memory-Safe Design (512 MB Heroku Limit)

The entire system is designed to run within Heroku's 512 MB RAM constraint:

- **Batched Data Fetch:** Tickers processed in batches of 100 with `asyncio.Semaphore(20)` concurrency control
- **Consolidated SQL:** Single `ANY(:ids)` query loads all OHLCV data, replacing 5,000+ individual N+1 queries
- **Chunked Upserts:** Bulk inserts are chunked into 1,000-row statements to prevent memory spikes
- **Aggressive GC:** `gc.collect()` after every batch; intermediate DataFrames are explicitly `del`-ed
- **VectorBT Pre-Warming:** Background daemon thread imports vectorbt at boot to avoid H20 timeout, with `threading.Event` synchronization

---

## Quickstart

### Prerequisites

- Python 3.12+
- PostgreSQL (or Heroku Postgres)
- API keys: [Polygon.io](https://polygon.io/) (paid), [Finnhub](https://finnhub.io/) (free)
- Optional: [Telegram Bot](https://core.telegram.org/bots#creating-a-new-bot) for alerts

### 1. Clone & Install

```bash
git clone https://github.com/raysyhuang/gemini_STST.git
cd gemini_STST
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure Environment

Create a `.env` file in the project root:

```env
POLYGON_API_KEY=your_polygon_api_key
FINNHUB_API_KEY=your_finnhub_api_key
DATABASE_URL=postgresql://user:pass@host:5432/dbname
TELEGRAM_BOT_TOKEN=your_bot_token        # optional
TELEGRAM_CHAT_ID=your_chat_id            # optional
```

### 3. Initialize Database & Fetch Data

```bash
# Fetch all NYSE/NASDAQ tickers + 90 days of OHLCV
python -m app.screener
```

This runs the full pipeline: data fetch, screener, news enrichment, and Telegram alert.

### 4. Run the Dashboard

```bash
uvicorn app.main:app --reload --port 5000
```

Open [http://localhost:5000](http://localhost:5000) to view the dashboard.

### 5. Deploy to Heroku

```bash
heroku create your-app-name
heroku addons:create heroku-postgresql:essential-0
heroku config:set POLYGON_API_KEY=... FINNHUB_API_KEY=... TELEGRAM_BOT_TOKEN=... TELEGRAM_CHAT_ID=...
git push heroku main
```

Add the Heroku Scheduler add-on and configure a daily job:

```
python -m app.screener
```

Schedule it for **6:00 PM ET** (after market close) to run the full pipeline daily.

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | Dashboard (serves `index.html`) |
| `GET` | `/api/screener/today` | Today's momentum signals + Finnhub news |
| `GET` | `/api/reversion/today` | Today's mean-reversion signals |
| `GET` | `/api/backtest/{ticker}` | VectorBT backtest results + equity curve |

---

## Project Structure

```
gemini_STST/
├── app/
│   ├── __init__.py
│   ├── config.py            # Environment variables
│   ├── database.py          # SQLAlchemy engine & session
│   ├── models.py            # ORM: Ticker, DailyMarketData, ScreenerSignal
│   ├── indicators.py        # ATR, RVOL, RSI, SMA (pure pandas)
│   ├── data_fetcher.py      # Polygon.io async data pipeline
│   ├── screener.py          # Momentum breakout screener
│   ├── mean_reversion.py    # Oversold bounce screener
│   ├── backtester.py        # VectorBT simulation engine
│   ├── news_fetcher.py      # Finnhub news & earnings calendar
│   ├── notifier.py          # Telegram alert system
│   ├── schemas.py           # Pydantic response models
│   └── main.py              # FastAPI application
├── static/
│   ├── index.html           # Dashboard HTML
│   ├── app.js               # Frontend logic
│   └── style.css            # Dark trading theme
├── requirements.txt
├── Procfile
├── .python-version
└── .gitignore
```

---

## Disclaimer

This software is for **educational and research purposes only**. It is not financial advice. Past backtest performance does not guarantee future results. Always do your own due diligence before trading. The authors are not responsible for any financial losses incurred from using this tool.

---

Built with pandas, VectorBT, and honest math.
