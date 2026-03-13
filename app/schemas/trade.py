from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, Field


class FillInput(BaseModel):
    side: str
    date: str = Field(min_length=10, max_length=10)
    price: Decimal = Field(ge=0)
    qty: int = Field(ge=1)
    fee: Optional[int] = Field(default=0, ge=0)


class FillRead(BaseModel):
    id: int
    trade_id: int
    side: str
    date: str
    price: float
    qty: int
    fee: int


class TradeCreate(BaseModel):
    market: str
    symbol: str = Field(min_length=1, max_length=64)
    name: Optional[str] = None
    notes_buy: Optional[str] = None
    notes_sell: Optional[str] = None
    notes_review: Optional[str] = None
    rating: Optional[int] = Field(default=None, ge=1, le=5)
    tags: Optional[str] = None
    chart_image_url: Optional[str] = None
    review_done: Optional[bool] = False
    reviewed_at: Optional[str] = None
    fills: list[FillInput]


class TradeUpdate(BaseModel):
    market: Optional[str] = None
    symbol: Optional[str] = Field(default=None, min_length=1, max_length=64)
    name: Optional[str] = None
    notes_buy: Optional[str] = None
    notes_sell: Optional[str] = None
    notes_review: Optional[str] = None
    rating: Optional[int] = Field(default=None, ge=1, le=5)
    tags: Optional[str] = None
    chart_image_url: Optional[str] = None
    review_done: Optional[bool] = None
    reviewed_at: Optional[str] = None
    buy_date: Optional[str] = Field(default=None, min_length=10, max_length=10)
    buy_price: Optional[Decimal] = Field(default=None, ge=0)
    buy_qty: Optional[int] = Field(default=None, ge=1)
    sell_date: Optional[str] = Field(default=None, min_length=10, max_length=10)
    sell_price: Optional[Decimal] = Field(default=None, ge=0)
    sell_qty: Optional[int] = Field(default=None, ge=1)
    fills: Optional[list[FillInput]] = None


class TradeRead(BaseModel):
    id: int
    market: str
    symbol: str
    name: Optional[str]
    notes_buy: Optional[str]
    notes_sell: Optional[str]
    notes_review: Optional[str]
    rating: Optional[int]
    tags: Optional[str]
    chart_image_url: Optional[str]
    review_done: bool
    reviewed_at: Optional[str]
    opened_at: str
    closed_at: Optional[str]
    created_at: str
    updated_at: str
    fills: list[FillRead]
    profit_jpy: Optional[float]
    profit_usd: Optional[float]
    profit_currency: str
    holding_days: Optional[int]
    is_open: bool


class TradeListStatsRead(BaseModel):
    total_profit_jpy: float
    total_profit_usd: float
    win_rate: Optional[float]
    avg_holding_days: Optional[float]
    avg_roi_pct: Optional[float]
    avg_rating: Optional[float]
    pending_review_count: int


class TradeListRead(BaseModel):
    items: list[TradeRead]
    total: int
    limit: int
    offset: int
    stats: TradeListStatsRead
