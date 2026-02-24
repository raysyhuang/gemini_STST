"""
Shared SQL pre-filter for active tickers.

Eliminates tickers that will certainly fail the price/volume filters
before the expensive OHLCV batch load, reducing query size by 60-70%.
"""

import logging
from datetime import date, timedelta

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def prefilter_active_tickers(
    db: Session,
    screen_date: date,
    min_price: float = 5.0,
    min_volume_proxy: float = 300_000,
) -> list[int]:
    """Pre-filter active tickers by recent price/volume to avoid loading unnecessary OHLCV.

    Uses DISTINCT ON to grab each ticker's most recent close/volume within the
    last 10 calendar days, then filters by min_price and a loose volume proxy.

    Returns a list of ticker IDs that pass the pre-filter.
    """
    stmt = text("""
        WITH latest AS (
            SELECT DISTINCT ON (ticker_id)
                ticker_id, close, volume
            FROM daily_market_data
            WHERE date >= :recent_cutoff
            ORDER BY ticker_id, date DESC
        )
        SELECT t.id
        FROM tickers t
        JOIN latest l ON l.ticker_id = t.id
        WHERE t.is_active = TRUE
          AND l.close > :min_price
          AND l.volume > :min_volume_proxy
    """)
    recent_cutoff = screen_date - timedelta(days=10)
    rows = db.execute(stmt, {
        "recent_cutoff": recent_cutoff,
        "min_price": min_price,
        "min_volume_proxy": min_volume_proxy,
    }).scalars().all()

    logger.info(
        "Pre-filter: %d tickers pass price>%.0f / vol>%.0f check for %s",
        len(rows), min_price, min_volume_proxy, screen_date,
    )
    return list(rows)
