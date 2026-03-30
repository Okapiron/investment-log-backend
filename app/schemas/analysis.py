from typing import Literal, Optional

from pydantic import BaseModel, Field


class AnalysisDiagnosisCardRead(BaseModel):
    key: Literal["pnl_structure", "holding_execution", "recent_change"]
    title: str
    hypothesis: str
    summary: str
    evidence: list[str]
    tone: Literal["positive", "warning", "neutral"] = "neutral"


class AnalysisTagStatRead(BaseModel):
    tag: str
    count: int = Field(ge=0)


class AnalysisHoldingBucketRead(BaseModel):
    label: str
    closed_trade_count: int = Field(ge=0)
    win_rate_pct: Optional[float] = None
    avg_net_profit_amount: Optional[float] = None
    avg_win_profit_amount: Optional[float] = None
    avg_loss_amount: Optional[float] = None


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
    primary_market: Optional[str] = None
    primary_profit_currency: Optional[str] = None
    primary_closed_trade_count: int = Field(default=0, ge=0)
    avg_win_profit_amount: Optional[float] = None
    avg_loss_amount: Optional[float] = None
    profit_loss_ratio: Optional[float] = None
    avg_win_holding_days: Optional[float] = None
    avg_loss_holding_days: Optional[float] = None
    recent_closed_trade_count: int = Field(default=0, ge=0)
    recent_win_rate_pct: Optional[float] = None
    recent_avg_win_profit_amount: Optional[float] = None
    recent_avg_loss_amount: Optional[float] = None
    recent_avg_holding_days: Optional[float] = None
    recent_avg_roi_pct: Optional[float] = None
    longest_win_streak: int = Field(default=0, ge=0)
    longest_loss_streak: int = Field(default=0, ge=0)
    top_tags: list[AnalysisTagStatRead]
    market_breakdown: list[AnalysisMarketStatRead]
    holding_buckets: list[AnalysisHoldingBucketRead]


class AnalysisReviewGapRead(BaseModel):
    label: str
    missing_count: int = Field(ge=0)


class AnalysisTopImprovementRead(BaseModel):
    key: Literal["pnl_structure", "holding_execution", "recent_change", "position_sizing"]
    title: str
    message: str
    rationale: list[str]


class AnalysisDataSufficiencyRead(BaseModel):
    enough_data: bool
    minimum_closed_trade_count: int = Field(ge=1)
    closed_trade_count: int = Field(ge=0)
    llm_status: Literal["generated", "insufficient_data", "unconfigured", "fallback", "error", "mock", "rule_based"]
    message: str


class AnalysisSummaryRead(BaseModel):
    headline_summary: Optional[str] = None
    top_improvement: Optional[AnalysisTopImprovementRead] = None
    summary: Optional[str] = None
    diagnoses: list[AnalysisDiagnosisCardRead]
    win_patterns: list[str]
    loss_patterns: list[str]
    actions: list[str]
    stats: AnalysisStatsRead
    review_gaps: list[AnalysisReviewGapRead]
    data_sufficiency: AnalysisDataSufficiencyRead
    generated_at: str
