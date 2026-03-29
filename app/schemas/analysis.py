from typing import Literal, Optional

from pydantic import BaseModel, Field


class AnalysisTagStatRead(BaseModel):
    tag: str
    count: int = Field(ge=0)


class AnalysisMarketStatRead(BaseModel):
    market: str
    closed_trade_count: int = Field(ge=0)
    win_trade_count: int = Field(ge=0)
    loss_trade_count: int = Field(ge=0)
    breakeven_trade_count: int = Field(ge=0)
    win_rate_pct: Optional[float] = None


class AnalysisStatsRead(BaseModel):
    closed_trade_count: int = Field(ge=0)
    open_trade_count: int = Field(ge=0)
    win_trade_count: int = Field(ge=0)
    loss_trade_count: int = Field(ge=0)
    breakeven_trade_count: int = Field(ge=0)
    win_rate_pct: Optional[float] = None
    avg_roi_pct: Optional[float] = None
    avg_holding_days: Optional[float] = None
    avg_rating: Optional[float] = None
    review_completion_rate_pct: Optional[float] = None
    top_tags: list[AnalysisTagStatRead]
    market_breakdown: list[AnalysisMarketStatRead]


class AnalysisDataSufficiencyRead(BaseModel):
    enough_data: bool
    minimum_closed_trade_count: int = Field(ge=1)
    closed_trade_count: int = Field(ge=0)
    llm_status: Literal["generated", "insufficient_data", "unconfigured", "fallback", "error", "mock"]
    message: str


class AnalysisSummaryRead(BaseModel):
    summary: Optional[str] = None
    win_patterns: list[str]
    loss_patterns: list[str]
    actions: list[str]
    stats: AnalysisStatsRead
    data_sufficiency: AnalysisDataSufficiencyRead
    generated_at: str
