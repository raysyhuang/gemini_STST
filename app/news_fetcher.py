"""
Finnhub news & earnings calendar module.

- Fetches the top N most recent company news headlines (/company-news)
- Fetches upcoming earnings dates (/calendar/earnings) to avoid binary event risk
"""

import logging
import ssl
from datetime import date, datetime, timedelta

import aiohttp
import certifi

from app.config import FINNHUB_API_KEY

logger = logging.getLogger(__name__)

FINNHUB_BASE = "https://finnhub.io/api/v1"
_ssl_ctx = ssl.create_default_context(cafile=certifi.where())


async def fetch_news(symbol: str, limit: int = 3) -> list[dict]:
    """
    Return the *limit* most recent news articles for *symbol*.

    Each item: {"headline": str, "source": str, "url": str, "published": str}
    Returns an empty list on error or if no articles are found.
    """
    today = date.today().isoformat()
    week_ago = (date.today() - timedelta(days=7)).isoformat()

    url = (
        f"{FINNHUB_BASE}/company-news"
        f"?symbol={symbol}&from={week_ago}&to={today}"
        f"&token={FINNHUB_API_KEY}"
    )

    try:
        connector = aiohttp.TCPConnector(ssl=_ssl_ctx)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    logger.warning("Finnhub news fetch failed for %s: HTTP %d", symbol, resp.status)
                    return []
                data = await resp.json()
    except Exception as e:
        logger.warning("Finnhub news error for %s: %s", symbol, e)
        return []

    # Finnhub returns articles sorted by datetime desc already
    articles = []
    for item in data[:limit]:
        ts = item.get("datetime", 0)
        published = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M") if ts else ""
        articles.append({
            "headline": item.get("headline", ""),
            "source": item.get("source", ""),
            "url": item.get("url", ""),
            "published": published,
        })

    return articles


async def fetch_earnings_blacklist(
    symbols: list[str],
    from_date: date | None = None,
    hold_days: int = 7,
) -> set[str]:
    """
    Return the set of symbols that have an earnings report scheduled
    within the next *hold_days* trading days.

    Uses Finnhub's /calendar/earnings endpoint which accepts a date range
    and returns all earnings in that window.

    Symbols with upcoming earnings are excluded from the screener to
    avoid binary event risk during our hold period.
    """
    if not symbols or not FINNHUB_API_KEY:
        return set()

    if from_date is None:
        from_date = date.today()

    # Look ahead ~10 calendar days to cover 7 trading days
    to_date = from_date + timedelta(days=hold_days + 3)

    url = (
        f"{FINNHUB_BASE}/calendar/earnings"
        f"?from={from_date.isoformat()}&to={to_date.isoformat()}"
        f"&token={FINNHUB_API_KEY}"
    )

    try:
        connector = aiohttp.TCPConnector(ssl=_ssl_ctx)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    logger.warning("Finnhub earnings calendar fetch failed: HTTP %d", resp.status)
                    return set()
                data = await resp.json()
    except Exception as e:
        logger.warning("Finnhub earnings calendar error: %s", e)
        return set()

    # data = {"earningsCalendar": [{"symbol": "AAPL", "date": "2026-02-15", ...}, ...]}
    earnings_symbols = set()
    symbol_set = set(symbols)  # for O(1) lookup
    for entry in data.get("earningsCalendar", []):
        sym = entry.get("symbol", "")
        if sym in symbol_set:
            earnings_symbols.add(sym)

    logger.info(
        "Earnings blacklist: %d of %d screened symbols report within %d days",
        len(earnings_symbols), len(symbols), hold_days,
    )
    return earnings_symbols
