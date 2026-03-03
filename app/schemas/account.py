from typing import Optional
from pydantic import BaseModel, Field

from app.schemas.common import TimestampSchema


class AccountCreate(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    institution: Optional[str] = None
    note: Optional[str] = None
    display_order: int = 0
    is_active: bool = True


class AccountUpdate(BaseModel):
    name: Optional[str] = Field(default=None, min_length=1, max_length=255)
    institution: Optional[str] = None
    note: Optional[str] = None
    display_order: Optional[int] = None
    is_active: Optional[bool] = None


class AccountRead(TimestampSchema):
    id: int
    name: str
    institution: Optional[str]
    note: Optional[str]
    display_order: int
    is_active: bool
