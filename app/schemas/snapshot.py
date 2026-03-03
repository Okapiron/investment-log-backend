from typing import Optional

from pydantic import BaseModel, Field, field_validator

from app.schemas.common import TimestampSchema


class SnapshotCreate(BaseModel):
    month: str = Field(min_length=7, max_length=7)
    asset_id: int
    value_jpy: int = Field(ge=0)
    memo: Optional[str] = None

    @field_validator("month")
    @classmethod
    def validate_month(cls, value: str) -> str:
        if len(value) != 7 or value[4] != "-":
            raise ValueError("month must be in YYYY-MM format")
        return value


class SnapshotUpdate(BaseModel):
    month: Optional[str] = Field(default=None, min_length=7, max_length=7)
    asset_id: Optional[int] = None
    value_jpy: Optional[int] = Field(default=None, ge=0)
    memo: Optional[str] = None

    @field_validator("month")
    @classmethod
    def validate_month(cls, value: Optional[str]) -> Optional[str]:
        if value is not None and (len(value) != 7 or value[4] != "-"):
            raise ValueError("month must be in YYYY-MM format")
        return value


class SnapshotRead(TimestampSchema):
    id: int
    month: str
    account_id: int
    asset_id: int
    value_jpy: int
    memo: Optional[str]
