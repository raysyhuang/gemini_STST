"""FMP (Financial Modeling Prep) client — earnings calendar for Gemini STST.

Async aiohttp client matching Gemini STST's async patterns.
Supplements Finnhub's earnings calendar for redundant coverage.
"""

import logging
import ssl
from datetime import date, timedelta

import aiohttp
import certifi

from app.config import FMP_API_KEY

logger = logging.getLogger(__name__)

FMP_BASE = "https://financialmodelingprep.com/stable"
_ssl_ctx = ssl.create_default_context(cafile=certifi.where())


async def fetch_earnings_calendar(
    from_date: date | None = None,
    to_date: date | None = None,
) -> list[dict]:
    """Fetch upcoming earnings dates from FMP.

    Returns a list of dicts with 'symbol', 'date', 'time' keys.
    Returns [] when the API key is missing or the call fails.
    """
    if not FMP_API_KEY:
        return []

    if from_date is None:
        from_date = date.today()
    if to_date is None:
        to_date = from_date + timedelta(days=14)

    url = f"{FMP_BASE}/earnings-calendar"
    params = {"apikey": FMP_API_KEY, "from": str(from_date), "to": str(to_date)}

    try:
        connector = aiohttp.TCPConnector(ssl=_ssl_ctx)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    logger.warning("FMP earnings calendar HTTP %d", resp.status)
                    return []
                data = await resp.json()
                return data if isinstance(data, list) else []
    except Exception as e:
        logger.warning("FMP earnings calendar error: %s", e)
        return []


async def fetch_earnings_blacklist(
    symbols: list[str],
    from_date: date | None = None,
    hold_days: int = 7,
) -> set[str]:
    """Return the set of symbols with earnings within the hold period.

    Mirrors the Finnhub fetch_earnings_blacklist() function signature
    so both sources can be unioned.
    """
    if not symbols or not FMP_API_KEY:
        return set()

    if from_date is None:
        from_date = date.today()

    to_date = from_date + timedelta(days=hold_days + 3)
    calendar = await fetch_earnings_calendar(from_date, to_date)
    if not calendar:
        return set()

    symbol_set = {s.upper() for s in symbols}
    blacklisted: set[str] = set()
    cutoff = from_date + timedelta(days=hold_days)

    for entry in calendar:
        sym = entry.get("symbol", "").upper()
        if sym not in symbol_set:
            continue
        try:
            earn_date = date.fromisoformat(entry.get("date", ""))
            if earn_date <= cutoff:
                blacklisted.add(sym)
        except (ValueError, TypeError):
            continue

    logger.info(
        "FMP earnings blacklist: %d of %d symbols report within %d days",
        len(blacklisted), len(symbols), hold_days,
    )
    return blacklisted
