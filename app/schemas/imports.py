from typing import Literal, Optional

from pydantic import BaseModel, Field


class RakutenImportPreviewRequest(BaseModel):
    filename: Optional[str] = None
    content: str = Field(min_length=1)


class RakutenImportAuditRequest(BaseModel):
    tradehistory_filename: Optional[str] = None
    tradehistory_content: str = Field(min_length=1)
    realized_filename: Optional[str] = None
    realized_content: str = Field(min_length=1)


class ImportFillPreviewRead(BaseModel):
    date: str
    price: float
    qty: int = Field(ge=1)
    fee: int = Field(ge=0)


class ImportTradeCandidateRead(BaseModel):
    source_signature: str
    source_position_key: str
    source_lot_sequence: int = Field(ge=1)
    symbol: str
    name: Optional[str] = None
    market: Literal["JP"] = "JP"
    buy: ImportFillPreviewRead
    sell: Optional[ImportFillPreviewRead] = None
    source_lines: list[int] = Field(default_factory=list)
    already_imported: bool = False
    is_partial_exit: bool = False
    remaining_qty_after_sell: int = Field(default=0, ge=0)


class ImportIssueRead(BaseModel):
    line: Optional[int] = None
    code: str
    message: str


class RakutenImportPreviewResponse(BaseModel):
    broker: Literal["rakuten"]
    market_scope: Literal["JP"]
    filename: Optional[str] = None
    candidate_count: int = Field(ge=0)
    skipped_count: int = Field(ge=0)
    error_count: int = Field(ge=0)
    candidates: list[ImportTradeCandidateRead]
    skipped: list[ImportIssueRead]
    errors: list[ImportIssueRead]


class RakutenImportCommitRequest(BaseModel):
    filename: Optional[str] = None
    items: list[ImportTradeCandidateRead]


class RakutenImportCommitResponse(BaseModel):
    broker: Literal["rakuten"]
    created_count: int = Field(ge=0)
    updated_count: int = Field(ge=0)
    skipped_count: int = Field(ge=0)
    error_count: int = Field(ge=0)
    created_trade_ids: list[int]
    updated_trade_ids: list[int] = Field(default_factory=list)
    skipped: list[ImportIssueRead]
    errors: list[ImportIssueRead]


class RakutenAuditRowRead(BaseModel):
    symbol: str
    name: Optional[str] = None
    sell_date: str
    qty: int = Field(ge=1)
    sell_price: float
    buy_price_or_avg_cost: float
    tt_profit_jpy: Optional[float] = None
    rakuten_profit_jpy: Optional[float] = None
    message: Optional[str] = None


class RakutenImportAuditResponse(BaseModel):
    tt_total_jpy: float
    rakuten_total_jpy: float
    gap_jpy: float
    matched_count: int = Field(ge=0)
    missing_in_tt: list[RakutenAuditRowRead]
    pnl_mismatch: list[RakutenAuditRowRead]
    unmatched_tt: list[RakutenAuditRowRead]
    reimport_hint: str
