"""
Options Flow Sentiment module.

Queries Polygon's Options Chain Snapshot API to compute a put/call ratio
for a given underlying ticker, then classifies sentiment:

  P/C < 0.7  → Bullish  (institutional call buying dominates)
  P/C > 1.0  → Bearish  (heavy put hedging / speculation)
  otherwise  → Neutral

The snapshot aggregates open interest across all listed contracts,
filtered to near-term expirations (≤30 days) for relevance.

Graceful degradation:
  - If the user's Polygon plan lacks an Options subscription (HTTP 403),
    or any other error occurs, returns {"sentiment": "Neutral", "put_call_ratio": None}.
  - This keeps the pipeline running regardless of API tier.
"""

import logging
import ssl
from datetime import date, timedelta

import aiohttp
import certifi

from app.config import POLYGON_API_KEY

logger = logging.getLogger(__name__)

_ssl_ctx = ssl.create_default_context(cafile=certifi.where())

# Sentiment thresholds (based on equity options norms)
BULLISH_THRESHOLD = 0.7   # P/C below this → call-heavy → bullish
BEARISH_THRESHOLD = 1.0   # P/C above this → put-heavy → bearish

# Only look at near-term options (≤30 days to expiry) for signal relevance
MAX_EXPIRY_DAYS = 30

# Pagination cap to avoid runaway API calls on names with thousands of strikes
MAX_PAGES = 4  # 4 pages × 250 = up to 1,000 contracts


def _classify_sentiment(put_call_ratio: float) -> str:
    """Map a numeric P/C ratio to a human-readable sentiment label."""
    if put_call_ratio < BULLISH_THRESHOLD:
        return "Bullish"
    elif put_call_ratio > BEARISH_THRESHOLD:
        return "Bearish"
    return "Neutral"


async def fetch_options_sentiment(
    symbol: str,
    session: aiohttp.ClientSession | None = None,
) -> dict:
    """
    Fetch the options chain snapshot for *symbol* from Polygon and compute
    the open-interest-weighted put/call ratio.

    Returns:
        {
            "sentiment": "Bullish" | "Bearish" | "Neutral",
            "put_call_ratio": float | None,
        }
    """
    neutral = {"sentiment": "Neutral", "put_call_ratio": None}

    if not POLYGON_API_KEY:
        return neutral

    today = date.today()
    max_expiry = today + timedelta(days=MAX_EXPIRY_DAYS)

    base_url = (
        f"https://api.polygon.io/v3/snapshot/options/{symbol}"
        f"?expiration_date.lte={max_expiry.isoformat()}"
        f"&limit=250"
        f"&apiKey={POLYGON_API_KEY}"
    )

    total_put_oi = 0
    total_call_oi = 0
    pages_fetched = 0

    owns_session = session is None
    if owns_session:
        connector = aiohttp.TCPConnector(ssl=_ssl_ctx)
        session = aiohttp.ClientSession(connector=connector)

    try:
        url = base_url
        while url and pages_fetched < MAX_PAGES:
            async with session.get(url) as resp:
                if resp.status == 403:
                    logger.info(
                        "Options API returned 403 for %s — plan may not include options data",
                        symbol,
                    )
                    return neutral

                if resp.status != 200:
                    logger.warning(
                        "Options API error %d for %s", resp.status, symbol,
                    )
                    return neutral

                data = await resp.json()

            results = data.get("results", [])
            for contract in results:
                oi = contract.get("open_interest", 0) or 0
                contract_type = (
                    contract.get("details", {}).get("contract_type", "")
                )
                if contract_type == "put":
                    total_put_oi += oi
                elif contract_type == "call":
                    total_call_oi += oi

            # Polygon paginates via next_url (already includes apiKey)
            url = data.get("next_url")
            if url and "apiKey" not in url:
                url += f"&apiKey={POLYGON_API_KEY}"
            pages_fetched += 1

    except Exception as e:
        logger.warning("Options flow fetch failed for %s: %s", symbol, e)
        return neutral
    finally:
        if owns_session:
            await session.close()

    # Need meaningful data on both sides to compute a ratio
    if total_call_oi == 0 and total_put_oi == 0:
        logger.debug("No options OI data for %s", symbol)
        return neutral

    if total_call_oi == 0:
        # All puts, no calls — extremely bearish
        ratio = 99.0
    else:
        ratio = round(total_put_oi / total_call_oi, 2)

    sentiment = _classify_sentiment(ratio)
    logger.info(
        "Options flow for %s: P/C=%.2f (%s) — puts=%d calls=%d",
        symbol, ratio, sentiment, total_put_oi, total_call_oi,
    )

    return {"sentiment": sentiment, "put_call_ratio": ratio}


async def fetch_options_sentiment_batch(
    symbols: list[str],
) -> dict[str, dict]:
    """
    Fetch options sentiment for multiple symbols concurrently.

    Returns a dict keyed by symbol:
        { "AAPL": {"sentiment": "Bullish", "put_call_ratio": 0.62}, ... }
    """
    import asyncio

    if not symbols:
        return {}

    connector = aiohttp.TCPConnector(ssl=_ssl_ctx, limit=10)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            fetch_options_sentiment(sym, session=session)
            for sym in symbols
        ]
        results = await asyncio.gather(*tasks)

    return {sym: res for sym, res in zip(symbols, results)}
