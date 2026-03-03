from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class AssetTypeShare(BaseModel):
    asset_type: str
    value_jpy: int
    ratio: float


class DashboardLatestResponse(BaseModel):
    month: Optional[str]
    total_jpy: int
    by_asset_type: list[AssetTypeShare]


class MonthlyPoint(BaseModel):
    month: str
    total_jpy: int
    by_asset_type: dict[str, int]


class DashboardMonthlyResponse(BaseModel):
    from_: Optional[str] = Field(default=None, alias="from")
    to: Optional[str] = None
    points: list[MonthlyPoint]

    model_config = ConfigDict(populate_by_name=True)
