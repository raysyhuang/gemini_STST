"""
Asynchronous Polygon.io data acquisition module.

Uses the paid-tier Polygon API (no rate limits) to fetch:
  1. All active NYSE/NASDAQ tickers
  2. 2 years of daily OHLCV data via the Aggregates endpoint

Architecture:
  - High-concurrency aiohttp requests controlled by asyncio.Semaphore
  - Batch processing (500 tickers/batch) to stay within Heroku memory limits
  - Results are bulk-inserted into Heroku Postgres via SQLAlchemy
"""

import asyncio
import logging
import ssl
from datetime import date, timedelta
from typing import Optional

import aiohttp
import certifi
import pandas as pd
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from app.config import POLYGON_API_KEY
from app.database import SessionLocal
from app.models import Ticker, DailyMarketData

logger = logging.getLogger(__name__)

POLYGON_BASE = "https://api.polygon.io"
MAX_CONCURRENCY = 20  # Keep memory pressure low on Heroku 512 MB dynos
BATCH_SIZE = 100      # Tickers per memory-safe batch

# Resolve macOS Python SSL cert issues
_ssl_ctx = ssl.create_default_context(cafile=certifi.where())


# ---------------------------------------------------------------------------
# 1. Fetch all active NYSE / NASDAQ tickers
# ---------------------------------------------------------------------------

# MIC code → exchange short name
# XNGS = NASDAQ Global Select (AAPL, MSFT, NVDA, GOOGL, META, TSLA, etc.)
# XNCM = NASDAQ Capital Market, XNMS = NASDAQ Global Market
_EXCHANGE_MAP: dict[str, str] = {
    "XNYS": "NYSE", "XNAS": "NASDAQ", "XASE": "AMEX",
    "ARCX": "NYSE", "BATS": "NASDAQ",
    "XNGS": "NASDAQ", "XNCM": "NASDAQ", "XNMS": "NASDAQ",
}

# Alphabet ranges to stay under Starter plan's ~1000 result cap per query
_TICKER_RANGES: list[tuple[str, str | None]] = [
    ("A", "D"), ("D", "G"), ("G", "J"), ("J", "M"),
    ("M", "P"), ("P", "S"), ("S", "V"), ("V", None),
]


async def _fetch_ticker_range(
    session: aiohttp.ClientSession,
    ticker_gte: str,
    ticker_lt: str | None,
) -> list[dict]:
    """Fetch one alphabetical slice of reference tickers."""
    tickers: list[dict] = []
    url = (
        f"{POLYGON_BASE}/v3/reference/tickers"
        f"?type=CS&market=stocks&active=true&limit=1000"
        f"&ticker.gte={ticker_gte}"
        f"&apiKey={POLYGON_API_KEY}"
    )
    if ticker_lt:
        url += f"&ticker.lt={ticker_lt}"

    while url:
        async with session.get(url) as resp:
            if resp.status != 200:
                logger.error("Ticker fetch failed (%s–%s): %s",
                             ticker_gte, ticker_lt, await resp.text())
                break
            data = await resp.json()

        for t in data.get("results", []):
            exchange_short = _EXCHANGE_MAP.get(t.get("primary_exchange", ""))
            if exchange_short in ("NYSE", "NASDAQ"):
                tickers.append({
                    "symbol": t["ticker"],
                    "exchange": exchange_short,
                    "company_name": t.get("name", ""),
                })

        url = data.get("next_url")
        if url:
            url = f"{url}&apiKey={POLYGON_API_KEY}"

    return tickers


async def fetch_all_tickers(session: aiohttp.ClientSession) -> list[dict]:
    """
    Paginate through Polygon's /v3/reference/tickers endpoint to retrieve
    every active stock on NYSE and NASDAQ.

    Queries 8 parallel alphabetical ranges so each stays under the Starter
    plan's ~1,000 result cap, giving full A–Z coverage.
    """
    chunks = await asyncio.gather(*(
        _fetch_ticker_range(session, gte, lt)
        for gte, lt in _TICKER_RANGES
    ))
    tickers = [t for chunk in chunks for t in chunk]
    logger.info("Fetched %d NYSE/NASDAQ tickers from Polygon", len(tickers))
    return tickers


def upsert_tickers(db: Session, tickers: list[dict]) -> None:
    """Bulk upsert tickers into Postgres, skipping duplicates."""
    if not tickers:
        return

    stmt = pg_insert(Ticker).values(tickers)
    stmt = stmt.on_conflict_do_update(
        index_elements=["symbol"],
        set_={
            "exchange": stmt.excluded.exchange,
            "company_name": stmt.excluded.company_name,
            "is_active": True,
        },
    )
    db.execute(stmt)
    db.commit()
    logger.info("Upserted %d tickers into Postgres", len(tickers))


# ---------------------------------------------------------------------------
# 2. Fetch daily OHLCV data (Aggregates endpoint)
# ---------------------------------------------------------------------------

async def _fetch_ohlcv_single(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    symbol: str,
    from_date: str,
    to_date: str,
) -> Optional[list[dict]]:
    """
    Fetch daily OHLCV for a single ticker from Polygon Aggregates.
    GET /v2/aggs/ticker/{ticker}/range/1/day/{from}/{to}
    """
    url = (
        f"{POLYGON_BASE}/v2/aggs/ticker/{symbol}/range/1/day"
        f"/{from_date}/{to_date}"
        f"?adjusted=true&sort=asc&limit=50000"
        f"&apiKey={POLYGON_API_KEY}"
    )

    async with semaphore:
        try:
            async with session.get(url) as resp:
                if resp.status != 200:
                    logger.warning("OHLCV fetch failed for %s: HTTP %d", symbol, resp.status)
                    return None
                data = await resp.json()
        except Exception as e:
            logger.warning("OHLCV fetch error for %s: %s", symbol, e)
            return None

    results = data.get("results")
    if not results:
        return None

    rows = []
    for bar in results:
        rows.append({
            "symbol": symbol,
            "date": pd.Timestamp(bar["t"], unit="ms").date(),
            "open": bar["o"],
            "high": bar["h"],
            "low": bar["l"],
            "close": bar["c"],
            "volume": bar["v"],
        })
    return rows


async def fetch_ohlcv_batch(
    symbols: list[str],
    from_date: str,
    to_date: str,
) -> list[dict]:
    """
    Fetch OHLCV for a batch of symbols concurrently.
    Returns a flat list of row dicts.
    """
    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
    all_rows: list[dict] = []

    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=_ssl_ctx)) as session:
        tasks = [
            _fetch_ohlcv_single(session, semaphore, sym, from_date, to_date)
            for sym in symbols
        ]
        results = await asyncio.gather(*tasks)

    for rows in results:
        if rows:
            all_rows.extend(rows)

    return all_rows


def bulk_upsert_ohlcv(db: Session, rows: list[dict], ticker_map: dict[str, int]) -> int:
    """
    Bulk upsert OHLCV rows into Postgres.
    ticker_map: {symbol: ticker_id}
    Returns the number of rows upserted.
    """
    if not rows:
        return 0

    values = []
    for r in rows:
        tid = ticker_map.get(r["symbol"])
        if tid is None:
            continue
        values.append({
            "ticker_id": tid,
            "date": r["date"],
            "open": r["open"],
            "high": r["high"],
            "low": r["low"],
            "close": r["close"],
            "volume": r["volume"],
        })

    if not values:
        return 0

    # Chunk the upsert to avoid building a single massive INSERT statement
    UPSERT_CHUNK = 1000
    for j in range(0, len(values), UPSERT_CHUNK):
        chunk = values[j : j + UPSERT_CHUNK]
        stmt = pg_insert(DailyMarketData).values(chunk)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_ticker_date",
            set_={
                "open": stmt.excluded.open,
                "high": stmt.excluded.high,
                "low": stmt.excluded.low,
                "close": stmt.excluded.close,
                "volume": stmt.excluded.volume,
            },
        )
        db.execute(stmt)
    db.commit()
    return len(values)


# ---------------------------------------------------------------------------
# 3. Orchestrator: full pipeline
# ---------------------------------------------------------------------------

async def run_full_data_pipeline(years_back: int = 2) -> None:
    """
    End-to-end pipeline:
      1. Fetch & upsert all NYSE/NASDAQ tickers
      2. Fetch 2 years of daily OHLCV in batches of 500
      3. Bulk upsert each batch into Postgres, then free memory
    """
    import gc

    to_date = date.today().isoformat()
    lookback_days = 365 * years_back if years_back > 0 else 90
    from_date = (date.today() - timedelta(days=lookback_days)).isoformat()

    # --- Step 1: Tickers ---
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=_ssl_ctx)) as session:
        raw_tickers = await fetch_all_tickers(session)

    db = SessionLocal()
    try:
        upsert_tickers(db, raw_tickers)

        # Build symbol -> ticker_id map
        all_tickers = db.query(Ticker).filter(Ticker.is_active.is_(True)).all()
        ticker_map = {t.symbol: t.id for t in all_tickers}
        symbols = list(ticker_map.keys())
    finally:
        db.close()

    logger.info(
        "Starting OHLCV fetch for %d symbols (%s to %s) in batches of %d",
        len(symbols), from_date, to_date, BATCH_SIZE,
    )

    # --- Step 2 & 3: Batch fetch + upsert ---
    total_rows = 0
    for i in range(0, len(symbols), BATCH_SIZE):
        batch = symbols[i : i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        logger.info("Batch %d: fetching %d symbols...", batch_num, len(batch))

        rows = await fetch_ohlcv_batch(batch, from_date, to_date)

        db = SessionLocal()
        try:
            count = bulk_upsert_ohlcv(db, rows, ticker_map)
            total_rows += count
            logger.info("Batch %d: upserted %d rows", batch_num, count)
        finally:
            db.close()

        # Memory safety: clear batch data and force garbage collection
        del rows
        gc.collect()

    logger.info("Pipeline complete. Total OHLCV rows upserted: %d", total_rows)
