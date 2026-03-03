from typing import Optional

from pydantic import BaseModel, Field, field_validator


class MonthlyAssetRow(BaseModel):
    asset_id: int
    asset_name: str
    asset_type: str
    currency: str
    value_jpy: Optional[int] = None
    snapshot_id: Optional[int] = None


class MonthlyAccountRow(BaseModel):
    account_id: int
    account_name: str
    assets: list[MonthlyAssetRow]


class MonthlySummary(BaseModel):
    filled: int
    missing: int


class MonthlyResponse(BaseModel):
    month: str
    accounts: list[MonthlyAccountRow]
    summary: MonthlySummary


class CopyLatestRequest(BaseModel):
    to_month: str = Field(min_length=7, max_length=7)

    @field_validator("to_month")
    @classmethod
    def validate_to_month(cls, value: str) -> str:
        if len(value) != 7 or value[4] != "-":
            raise ValueError("to_month must be in YYYY-MM format")
        return value


class CopyLatestResponse(BaseModel):
    from_month: str
    to_month: str
    created: int
    skipped: int
