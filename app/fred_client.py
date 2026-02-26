"""FRED macro data client — VIX, yield curve for Gemini STST regime context.

Async aiohttp client matching Gemini STST's async patterns.
Falls back to yfinance for VIX when no FRED API key is available.
"""

import logging
import ssl
from datetime import date, timedelta

import aiohttp
import certifi

from app.config import FRED_API_KEY

logger = logging.getLogger(__name__)

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
_ssl_ctx = ssl.create_default_context(cafile=certifi.where())

SERIES = {
    "vix": "VIXCLS",
    "yield_spread": "T10Y2Y",  # 10Y-2Y spread
}


async def _fetch_series(series_id: str, lookback_days: int = 30) -> float | None:
    """Fetch the latest value of a FRED series. Returns None on failure."""
    if not FRED_API_KEY:
        return None

    from_date = date.today() - timedelta(days=lookback_days)
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": str(from_date),
        "observation_end": str(date.today()),
    }

    try:
        connector = aiohttp.TCPConnector(ssl=_ssl_ctx)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(FRED_BASE, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    logger.warning("FRED %s HTTP %d", series_id, resp.status)
                    return None
                data = await resp.json(content_type=None)
                observations = data.get("observations", [])
                for obs in reversed(observations):
                    val = obs.get("value", ".")
                    if val != ".":
                        return float(val)
                return None
    except Exception as e:
        logger.warning("FRED %s error: %s", series_id, e)
        return None


async def fetch_vix() -> float | None:
    """Get latest VIX from FRED, falling back to yfinance."""
    val = await _fetch_series(SERIES["vix"])
    if val is not None:
        return val

    # yfinance fallback (run sync in thread)
    try:
        import asyncio
        from concurrent.futures import ThreadPoolExecutor

        import yfinance as yf

        def _yf_vix():
            tk = yf.Ticker("^VIX")
            hist = tk.history(period="5d")
            if not hist.empty:
                return float(hist["Close"].iloc[-1])
            return None

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(ThreadPoolExecutor(max_workers=1), _yf_vix)
    except Exception:
        return None


async def fetch_yield_spread() -> float | None:
    """Get 10Y-2Y yield spread from FRED."""
    return await _fetch_series(SERIES["yield_spread"])


async def get_macro_regime() -> dict:
    """Return macro regime context for screener enrichment.

    Returns a dict with:
        vix, yield_spread, regime_label ("calm", "elevated", "stress")
    Always returns a dict — never raises.
    """
    vix = await fetch_vix()
    spread = await fetch_yield_spread()

    if vix is not None and vix > 30:
        regime_label = "stress"
    elif vix is not None and vix > 20:
        regime_label = "elevated"
    else:
        regime_label = "calm"

    result = {
        "vix": vix,
        "yield_spread": spread,
        "yield_curve_inverted": spread is not None and spread < 0,
        "regime_label": regime_label,
    }
    logger.info(
        "FRED macro regime: VIX=%s spread=%s label=%s",
        f"{vix:.1f}" if vix else "N/A",
        f"{spread:.2f}" if spread else "N/A",
        regime_label,
    )
    return result
