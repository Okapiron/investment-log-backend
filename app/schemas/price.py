from pydantic import BaseModel


class PriceBarRead(BaseModel):
    time: str
    open: float
    high: float
    low: float
    close: float
    volume: float


class PriceResponse(BaseModel):
    market: str
    symbol: str
    interval: str
    bars: list[PriceBarRead]
