import csv
import io
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from fastapi import APIRouter, HTTPException, Query

from app.schemas.price import PriceBarRead, PriceResponse

router = APIRouter(prefix="/prices", tags=["prices"])
INTERVAL_TO_STOOQ = {
    "1d": "d",
    "1w": "w",
    "1m": "m",
}


def _to_stooq_symbol(market: str, symbol: str) -> str:
    s = (symbol or "").strip().lower()
    if not s:
        raise HTTPException(status_code=422, detail="symbol is required")
    if market == "JP":
        # MVP: 東証銘柄は 7203.jp 形式
        return f"{s}.jp"
    if market == "US":
        # MVP: NASDAQ/US は aapl.us 形式
        return f"{s}.us"
    raise HTTPException(status_code=422, detail="market must be JP or US")


def _parse_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    v = str(value).strip()
    if not v or v == "N/D":
        return None
    try:
        return float(v)
    except ValueError:
        return None


@router.get("", response_model=PriceResponse)
def get_prices(
    market: str = Query(..., pattern="^(JP|US)$"),
    symbol: str = Query(..., min_length=1),
    interval: str = Query(default="1d"),
):
    stooq_interval = INTERVAL_TO_STOOQ.get(interval)
    if stooq_interval is None:
        raise HTTPException(status_code=422, detail="interval currently supports only 1d, 1w, 1m")

    stooq_symbol = _to_stooq_symbol(market, symbol)
    url = f"https://stooq.com/q/d/l/?s={stooq_symbol}&i={stooq_interval}"

    try:
        with urlopen(url, timeout=10) as res:
            body = res.read().decode("utf-8", errors="ignore")
    except HTTPError as e:
        raise HTTPException(status_code=502, detail=f"price source http error: {e.code}")
    except URLError:
        raise HTTPException(status_code=502, detail="price source unavailable")
    except Exception:
        raise HTTPException(status_code=502, detail="failed to fetch price data")

    reader = csv.DictReader(io.StringIO(body))
    bars = []
    for row in reader:
        dt = (row.get("Date") or "").strip()
        if not dt:
            continue
        o = _parse_float(row.get("Open"))
        h = _parse_float(row.get("High"))
        l = _parse_float(row.get("Low"))
        c = _parse_float(row.get("Close"))
        v = _parse_float(row.get("Volume"))
        if None in (o, h, l, c):
            continue
        bars.append(
            PriceBarRead(
                time=dt,
                open=o,
                high=h,
                low=l,
                close=c,
                volume=v or 0,
            )
        )

    if not bars:
        raise HTTPException(status_code=404, detail="no price bars found")

    bars.sort(key=lambda x: x.time)
    return PriceResponse(market=market, symbol=symbol, interval=interval, bars=bars)
