from typing import Optional

from pydantic import BaseModel, Field

from app.core.constants import ASSET_TYPES
from app.schemas.common import TimestampSchema


class AssetCreate(BaseModel):
    account_id: int
    name: str = Field(min_length=1, max_length=255)
    asset_type: str
    currency: str = Field(default="JPY", min_length=3, max_length=10)
    ticker: Optional[str] = None
    note: Optional[str] = None
    display_order: int = 0
    is_active: bool = True


class AssetUpdate(BaseModel):
    account_id: Optional[int] = None
    name: Optional[str] = Field(default=None, min_length=1, max_length=255)
    asset_type: Optional[str] = None
    currency: Optional[str] = Field(default=None, min_length=3, max_length=10)
    ticker: Optional[str] = None
    note: Optional[str] = None
    display_order: Optional[int] = None
    is_active: Optional[bool] = None


class AssetRead(TimestampSchema):
    id: int
    account_id: int
    name: str
    asset_type: str
    currency: str
    ticker: Optional[str]
    note: Optional[str]
    display_order: int
    is_active: bool


def validate_asset_type(asset_type: str):
    if asset_type not in ASSET_TYPES:
        raise ValueError(f"asset_type must be one of: {', '.join(ASSET_TYPES)}")
