"""
Finnhub news enrichment module.

Fetches the top N most recent company news headlines for a given ticker
using the Finnhub /company-news endpoint.
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
