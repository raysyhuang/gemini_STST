"""Pydantic response schemas for the FastAPI endpoints."""

from datetime import date
from typing import Any, Optional

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
    options_sentiment: str | None = None
    put_call_ratio: float | None = None
    rsi_14: float | None = None
    pct_from_52w_high: float | None = None
    quality_score: float | None = None
    confluence: bool = False
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


class ReversionSignalResponse(BaseModel):
    ticker: str
    company_name: str
    date: date
    trigger_price: float
    rsi2: float
    drawdown_3d_pct: float
    sma_distance_pct: float
    atr_pct_at_trigger: float | None = None
    options_sentiment: str | None = None
    put_call_ratio: float | None = None
    quality_score: float | None = None
    confluence: bool = False

    model_config = {"from_attributes": True}


class ReversionScreenerResponse(BaseModel):
    date: date
    signals: list[ReversionSignalResponse]


class BacktestResultResponse(BaseModel):
    ticker: str
    win_rate: float
    profit_factor: float
    total_return_pct: float
    max_drawdown_pct: float
    total_trades: int
    avg_position_size_pct: float = 10.0
    equity_curve: list[dict[str, Any]]


# -- Paper Trading --

class PaperTradeResponse(BaseModel):
    id: int
    ticker: str
    strategy: str
    signal_date: date
    entry_date: Optional[date] = None
    entry_price: Optional[float] = None
    shares: Optional[float] = None
    position_size: float
    quality_score: Optional[float] = None
    stop_level: Optional[float] = None
    planned_exit_date: Optional[date] = None
    actual_exit_date: Optional[date] = None
    exit_price: Optional[float] = None
    exit_reason: Optional[str] = None
    pnl_dollars: Optional[float] = None
    pnl_pct: Optional[float] = None
    status: str
    hold_days: Optional[int] = None

    model_config = {"from_attributes": True}


class StrategyBreakdown(BaseModel):
    total_trades: int = 0
    win_rate: float = 0.0
    avg_return_pct: float = 0.0
    total_pnl: float = 0.0


class PaperMetricsResponse(BaseModel):
    total_trades: int = 0
    open_trades: int = 0
    closed_trades: int = 0
    win_rate: float = 0.0
    profit_factor: float = 0.0
    avg_return_pct: float = 0.0
    total_pnl: float = 0.0
    avg_hold_days: float = 0.0
    best_trade_pct: float = 0.0
    worst_trade_pct: float = 0.0
    momentum: StrategyBreakdown = StrategyBreakdown()
    reversion: StrategyBreakdown = StrategyBreakdown()


class PaperTradesListResponse(BaseModel):
    total: int
    trades: list[PaperTradeResponse]


class BackfillResponse(BaseModel):
    total_created: int
    total_filled: int
    total_closed: int
    date_range: str
    trading_days_processed: int


class EquityCurveResponse(BaseModel):
    equity_curve: list[dict[str, Any]]


# -- Recalibration --

class FactorQuartileDetail(BaseModel):
    n: int
    raw_wr: Optional[float] = None
    smoothed_wr: float


class FactorAnalysisResponse(BaseModel):
    factor: str
    quartiles: dict[str, FactorQuartileDetail]
    predictive_power: float


class WeightRecommendationResponse(BaseModel):
    id: int
    strategy: str
    status: str
    n_trades_analyzed: int
    overall_win_rate: float
    current_weights: dict[str, float]
    recommended_weights: dict[str, float]
    factor_analysis: Optional[list[dict[str, Any]]] = None
    created_at: Optional[str] = None
    approved_at: Optional[str] = None
    notes: Optional[str] = None

    model_config = {"from_attributes": True}


class WeightRecommendationListResponse(BaseModel):
    recommendations: list[WeightRecommendationResponse]


class ActiveWeightResponse(BaseModel):
    strategy: str
    weights: dict[str, float]
    version: int
    activated_at: Optional[str] = None
    recommendation_id: Optional[int] = None
    quality_floor: Optional[float] = None
    regime_multipliers: Optional[dict[str, float]] = None
    source: str = "learned"  # "learned" or "hardcoded"

    model_config = {"from_attributes": True}


class ActiveWeightsListResponse(BaseModel):
    weights: list[ActiveWeightResponse]


class AnalyzeResponse(BaseModel):
    recommendations: list[WeightRecommendationResponse]
    errors: list[str] = []
