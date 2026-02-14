# Database Schema & Models
# Provide this to the AI when generating the SQLAlchemy models (e.g., in models.py)

from sqlalchemy import Column, Integer, String, Float, Date, BigInteger, Boolean, ForeignKey, UniqueConstraint, Index
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class Ticker(Base):
    __tablename__ = 'tickers'

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(10), unique=True, nullable=False, index=True)
    exchange = Column(String(10), nullable=False)
    company_name = Column(String(255))
    is_active = Column(Boolean, default=True) # Useful for filtering out delisted stocks over a 2-year backtest

    # Relationships
    market_data = relationship("DailyMarketData", back_populates="ticker")
    signals = relationship("ScreenerSignal", back_populates="ticker")

class DailyMarketData(Base):
    __tablename__ = 'daily_market_data'

    id = Column(Integer, primary_key=True)
    ticker_id = Column(Integer, ForeignKey('tickers.id'), nullable=False)
    date = Column(Date, nullable=False)
    
    # Raw Polygon Data
    # Note: Using Float instead of Numeric because pandas/numpy/vectorbt process Floats significantly faster in memory.
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(BigInteger, nullable=False) # Must be BigInteger, standard Integer will overflow on high volume days
    
    # Pre-calculated Quantitative Indicators
    atr_14 = Column(Float)      # 14-day Average True Range (Absolute)
    atr_pct = Column(Float)     # ATR as a percentage of price (Target > 8%)
    rvol = Column(Float)        # Relative Volume (Target > 2.0)
    sma_20 = Column(Float)      # 20-day Simple Moving Average (For Market Regime checks)

    # Relationship
    ticker = relationship("Ticker", back_populates="market_data")

    # Critical: Enforce unique dates per ticker, and index the date for fast time-series queries
    __table_args__ = (
        UniqueConstraint('ticker_id', 'date', name='uq_ticker_date'),
        Index('idx_date_ticker', 'date', 'ticker_id'),
    )

class ScreenerSignal(Base):
    __tablename__ = 'screener_signals'

    id = Column(Integer, primary_key=True)
    ticker_id = Column(Integer, ForeignKey('tickers.id'), nullable=False)
    date = Column(Date, nullable=False, index=True)
    
    # Snapshot of the metrics at the exact moment the stock triggered the screener
    trigger_price = Column(Float, nullable=False)
    rvol_at_trigger = Column(Float, nullable=False)
    atr_pct_at_trigger = Column(Float, nullable=False)
    
    # Relationship
    ticker = relationship("Ticker", back_populates="signals")

    __table_args__ = (
        UniqueConstraint('ticker_id', 'date', name='uq_signal_ticker_date'),
    )