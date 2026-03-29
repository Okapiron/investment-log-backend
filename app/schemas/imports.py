from typing import Literal, Optional

from pydantic import BaseModel, Field


class RakutenImportPreviewRequest(BaseModel):
    filename: Optional[str] = None
    content: str = Field(min_length=1)


class ImportFillPreviewRead(BaseModel):
    date: str
    price: float
    qty: int = Field(ge=1)
    fee: int = Field(ge=0)


class ImportTradeCandidateRead(BaseModel):
    source_signature: str
    symbol: str
    name: Optional[str] = None
    market: Literal["JP"] = "JP"
    buy: ImportFillPreviewRead
    sell: ImportFillPreviewRead
    source_lines: list[int] = Field(default_factory=list)
    already_imported: bool = False


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
    skipped_count: int = Field(ge=0)
    error_count: int = Field(ge=0)
    created_trade_ids: list[int]
    skipped: list[ImportIssueRead]
    errors: list[ImportIssueRead]
