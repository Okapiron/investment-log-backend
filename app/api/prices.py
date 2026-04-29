from fastapi import APIRouter, HTTPException, Query

from app.core.config import settings
from app.core.price_provider import SUPPORTED_INTERVALS, get_price_provider
from app.schemas.price import PriceResponse

router = APIRouter(prefix="/prices", tags=["prices"])


@router.get("", response_model=PriceResponse)
def get_prices(
    market: str = Query(..., pattern="^(JP|US)$"),
    symbol: str = Query(..., min_length=1),
    interval: str = Query(default="1d"),
):
    if not settings.price_api_enabled:
        raise HTTPException(status_code=503, detail="price api is disabled")
    if interval not in SUPPORTED_INTERVALS:
        raise HTTPException(status_code=422, detail="interval currently supports only 1d, 1w, 1m")

    provider = get_price_provider()
    bars = provider.get_bars(market=market, symbol=symbol, interval=interval)
    return PriceResponse(market=market, symbol=symbol, interval=interval, bars=bars)
