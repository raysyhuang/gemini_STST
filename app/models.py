from sqlalchemy import (
    Column, Integer, String, Float, Date, BigInteger,
    Boolean, ForeignKey, UniqueConstraint, Index,
)
from sqlalchemy.orm import relationship
from app.database import Base


class Ticker(Base):
    __tablename__ = "tickers"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(10), unique=True, nullable=False, index=True)
    exchange = Column(String(10), nullable=False)
    company_name = Column(String(255))
    is_active = Column(Boolean, default=True)

    market_data = relationship("DailyMarketData", back_populates="ticker")
    signals = relationship("ScreenerSignal", back_populates="ticker")
    reversion_signals = relationship("ReversionSignal", back_populates="ticker")


class DailyMarketData(Base):
    __tablename__ = "daily_market_data"

    id = Column(Integer, primary_key=True)
    ticker_id = Column(Integer, ForeignKey("tickers.id"), nullable=False)
    date = Column(Date, nullable=False)

    # Raw OHLCV from Polygon
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(BigInteger, nullable=False)

    # Pre-calculated indicators (populated in Phase 2)
    atr_14 = Column(Float)
    atr_pct = Column(Float)
    rvol = Column(Float)
    sma_20 = Column(Float)

    ticker = relationship("Ticker", back_populates="market_data")

    __table_args__ = (
        UniqueConstraint("ticker_id", "date", name="uq_ticker_date"),
        Index("idx_date_ticker", "date", "ticker_id"),
    )


class ScreenerSignal(Base):
    __tablename__ = "screener_signals"

    id = Column(Integer, primary_key=True)
    ticker_id = Column(Integer, ForeignKey("tickers.id"), nullable=False)
    date = Column(Date, nullable=False, index=True)

    trigger_price = Column(Float, nullable=False)
    rvol_at_trigger = Column(Float, nullable=False)
    atr_pct_at_trigger = Column(Float, nullable=False)

    # Options flow overlay (Phase 5 Sprint 2)
    options_sentiment = Column(String(10))   # Bullish / Bearish / Neutral
    put_call_ratio = Column(Float)

    ticker = relationship("Ticker", back_populates="signals")

    __table_args__ = (
        UniqueConstraint("ticker_id", "date", name="uq_signal_ticker_date"),
    )


class ReversionSignal(Base):
    __tablename__ = "reversion_signals"

    id = Column(Integer, primary_key=True)
    ticker_id = Column(Integer, ForeignKey("tickers.id"), nullable=False)
    date = Column(Date, nullable=False, index=True)

    trigger_price = Column(Float, nullable=False)
    rsi2_at_trigger = Column(Float, nullable=False)
    drawdown_3d_pct = Column(Float, nullable=False)
    sma_distance_pct = Column(Float, nullable=False)

    # Options flow overlay (Phase 5 Sprint 2)
    options_sentiment = Column(String(10))   # Bullish / Bearish / Neutral
    put_call_ratio = Column(Float)

    ticker = relationship("Ticker", back_populates="reversion_signals")

    __table_args__ = (
        UniqueConstraint("ticker_id", "date", name="uq_reversion_signal_ticker_date"),
    )
