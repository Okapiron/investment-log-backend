from datetime import date
import json
import time
from dataclasses import dataclass
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

from fastapi import HTTPException

from app.core.config import settings
from app.schemas.price import PriceBarRead

SUPPORTED_INTERVALS = {"1d", "1w", "1m"}


def _parse_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    v = str(value).strip()
    if not v:
        return None
    try:
        return float(v)
    except ValueError:
        return None


def _aggregate_bars(bars: list[PriceBarRead], interval: str) -> list[PriceBarRead]:
    if interval == "1d":
        return bars

    buckets: dict[str, list[PriceBarRead]] = {}
    for bar in bars:
        if interval == "1w":
            iso = date.fromisoformat(bar.time).isocalendar()
            iso_year = int(iso[0])
            iso_week = int(iso[1])
            bucket_key = f"{iso_year:04d}-W{iso_week:02d}"
        elif interval == "1m":
            bucket_key = bar.time[:7]
        else:
            raise HTTPException(status_code=422, detail="interval currently supports only 1d, 1w, 1m")
        buckets.setdefault(bucket_key, []).append(bar)

    aggregated: list[PriceBarRead] = []
    for rows in buckets.values():
        ordered = sorted(rows, key=lambda item: item.time)
        aggregated.append(
            PriceBarRead(
                time=ordered[-1].time,
                open=ordered[0].open,
                high=max(item.high for item in ordered),
                low=min(item.low for item in ordered),
                close=ordered[-1].close,
                volume=sum(float(item.volume or 0) for item in ordered),
            )
        )

    aggregated.sort(key=lambda item: item.time)
    return aggregated


@dataclass
class _CacheEntry:
    expires_at: float
    bars: list[PriceBarRead]


_CACHE: dict[str, _CacheEntry] = {}


class AlphaVantagePriceProvider:
    name = "alpha_vantage"

    def _to_vendor_symbol(self, market: str, symbol: str) -> str:
        normalized_market = str(market or "").strip().upper()
        normalized_symbol = str(symbol or "").strip().upper()
        if normalized_market != "JP":
            raise HTTPException(status_code=404, detail="price data is currently available only for JP market")
        if not normalized_symbol:
            raise HTTPException(status_code=422, detail="symbol is required")
        suffix = str(settings.alpha_vantage_jp_suffix or "TYO").strip().upper()
        return f"{normalized_symbol}.{suffix}"

    def _fetch_daily_bars(self, market: str, symbol: str) -> list[PriceBarRead]:
        vendor_symbol = self._to_vendor_symbol(market, symbol)
        api_key = str(settings.alpha_vantage_api_key or "").strip()
        if not api_key:
            raise HTTPException(status_code=503, detail="price source is not configured")

        query = urlencode(
            {
                "function": "TIME_SERIES_DAILY",
                "symbol": vendor_symbol,
                "apikey": api_key,
                "outputsize": "compact",
            }
        )
        url = f"https://www.alphavantage.co/query?{query}"

        try:
            with urlopen(url, timeout=10) as res:
                payload = json.loads(res.read().decode("utf-8", errors="ignore"))
        except HTTPError as exc:
            raise HTTPException(status_code=502, detail=f"price source http error: {exc.code}")
        except URLError:
            raise HTTPException(status_code=502, detail="price source unavailable")
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(status_code=502, detail="failed to fetch price data")

        if payload.get("Note"):
            raise HTTPException(status_code=502, detail="price source rate limited")
        if payload.get("Information") and "rate limit" in str(payload.get("Information")).lower():
            raise HTTPException(status_code=502, detail="price source rate limited")
        if payload.get("Error Message"):
            raise HTTPException(status_code=404, detail="no price bars found")

        series = payload.get("Time Series (Daily)")
        if not isinstance(series, dict) or not series:
            raise HTTPException(status_code=404, detail="no price bars found")

        bars: list[PriceBarRead] = []
        for dt, raw in series.items():
            open_price = _parse_float(raw.get("1. open"))
            high_price = _parse_float(raw.get("2. high"))
            low_price = _parse_float(raw.get("3. low"))
            close_price = _parse_float(raw.get("4. close"))
            volume = _parse_float(raw.get("5. volume")) or 0
            if None in (open_price, high_price, low_price, close_price):
                continue
            bars.append(
                PriceBarRead(
                    time=dt,
                    open=open_price,
                    high=high_price,
                    low=low_price,
                    close=close_price,
                    volume=volume,
                )
            )

        if not bars:
            raise HTTPException(status_code=404, detail="no price bars found")

        bars.sort(key=lambda item: item.time)
        return bars

    def get_bars(self, market: str, symbol: str, interval: str) -> list[PriceBarRead]:
        if interval not in SUPPORTED_INTERVALS:
            raise HTTPException(status_code=422, detail="interval currently supports only 1d, 1w, 1m")

        normalized_market = str(market or "").strip().upper()
        normalized_symbol = str(symbol or "").strip().upper()
        cache_key = f"{self.name}:{normalized_market}:{normalized_symbol}"
        cached = _CACHE.get(cache_key)
        now_ts = time.time()

        if cached and cached.expires_at > now_ts:
            daily_bars = cached.bars
        else:
            try:
                daily_bars = self._fetch_daily_bars(normalized_market, normalized_symbol)
                ttl_seconds = max(300, int(settings.price_cache_ttl_seconds))
                _CACHE[cache_key] = _CacheEntry(expires_at=now_ts + ttl_seconds, bars=daily_bars)
            except HTTPException:
                if cached and cached.bars:
                    daily_bars = cached.bars
                else:
                    raise

        return _aggregate_bars(daily_bars, interval)


def get_price_provider() -> AlphaVantagePriceProvider:
    provider = str(settings.price_provider or "").strip().lower()
    if provider in {"", "alpha_vantage"}:
        return AlphaVantagePriceProvider()
    raise HTTPException(status_code=503, detail="unsupported price provider")
