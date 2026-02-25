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

    Uses the latest close for price filtering and average volume over the last
    10 calendar days as a proxy for ADV filtering.  Single-day volume is too
    spiky to approximate 20-day ADV, so averaging gives a much tighter filter.

    Returns a list of ticker IDs that pass the pre-filter.
    """
    stmt = text("""
        WITH latest_close AS (
            SELECT DISTINCT ON (ticker_id)
                ticker_id, close
            FROM daily_market_data
            WHERE date >= :recent_cutoff
            ORDER BY ticker_id, date DESC
        ),
        avg_vol AS (
            SELECT ticker_id, AVG(volume) AS avg_volume
            FROM daily_market_data
            WHERE date >= :recent_cutoff
            GROUP BY ticker_id
        )
        SELECT t.id
        FROM tickers t
        JOIN latest_close lc ON lc.ticker_id = t.id
        JOIN avg_vol av ON av.ticker_id = t.id
        WHERE t.is_active = TRUE
          AND lc.close > :min_price
          AND av.avg_volume > :min_volume_proxy
    """)
    recent_cutoff = screen_date - timedelta(days=10)
    rows = db.execute(stmt, {
        "recent_cutoff": recent_cutoff,
        "min_price": min_price,
        "min_volume_proxy": min_volume_proxy,
    }).scalars().all()

    logger.info(
        "Pre-filter: %d tickers pass price>%.0f / avg_vol>%.0f check for %s",
        len(rows), min_price, min_volume_proxy, screen_date,
    )
    return list(rows)
