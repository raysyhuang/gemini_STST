"""Pydantic response schemas for the FastAPI endpoints."""

from datetime import date
from typing import Any

from pydantic import BaseModel


class NewsArticle(BaseModel):
    headline: str
    source: str
    url: str
    published: str


class SignalResponse(BaseModel):
    ticker: str
    company_name: str
    date: date
    trigger_price: float
    rvol_at_trigger: float
    atr_pct_at_trigger: float
    news: list[NewsArticle] = []

    model_config = {"from_attributes": True}


class MarketRegimeResponse(BaseModel):
    spy_above_sma20: bool | None
    qqq_above_sma20: bool | None
    regime: str


class ScreenerResponse(BaseModel):
    date: date
    regime: MarketRegimeResponse
    signals: list[SignalResponse]


class BacktestResultResponse(BaseModel):
    ticker: str
    win_rate: float
    profit_factor: float
    total_return_pct: float
    max_drawdown_pct: float
    total_trades: int
    equity_curve: list[dict[str, Any]]
