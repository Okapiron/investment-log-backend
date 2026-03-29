from datetime import date
import json
import time
from dataclasses import dataclass
from datetime import timedelta
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


class MarketstackPriceProvider:
    name = "marketstack"

    def _normalize_symbol(self, market: str, symbol: str) -> tuple[str, Optional[str]]:
        normalized_market = str(market or "").strip().upper()
        normalized_symbol = str(symbol or "").strip().upper()
        if not normalized_symbol:
            raise HTTPException(status_code=422, detail="symbol is required")
        if normalized_market == "JP":
            return normalized_symbol, str(settings.marketstack_jp_mic or "XTKS").strip().upper()
        if normalized_market == "US":
            return normalized_symbol.replace(".", "-"), None
        raise HTTPException(status_code=422, detail="market must be JP or US")

    def _fetch_daily_bars(self, market: str, symbol: str) -> list[PriceBarRead]:
        access_key = str(settings.marketstack_access_key or "").strip()
        if not access_key:
            raise HTTPException(status_code=503, detail="price source is not configured")

        vendor_symbol, exchange_mic = self._normalize_symbol(market, symbol)
        today = date.today()
        date_from = today - timedelta(days=max(60, int(settings.price_history_days)))
        query = urlencode(
            {
                "access_key": access_key,
                "symbols": vendor_symbol,
                "date_from": date_from.isoformat(),
                "date_to": today.isoformat(),
                "limit": 1000,
                "sort": "ASC",
            }
        )
        if exchange_mic:
            query = f"{query}&exchange={exchange_mic}"
        base_url = str(settings.marketstack_base_url or "https://api.marketstack.com/v2").rstrip("/")
        url = f"{base_url}/eod?{query}"

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

        error = payload.get("error")
        if isinstance(error, dict):
            code = str(error.get("code") or "")
            message = str(error.get("message") or "").lower()
            if code == "rate_limit_reached" or "rate limit" in message:
                raise HTTPException(status_code=502, detail="price source rate limited")
            raise HTTPException(status_code=502, detail="price source unavailable")
        if payload.get("pagination") is None and not payload.get("data"):
            raise HTTPException(status_code=502, detail="price source unavailable")
        if payload.get("Note"):
            raise HTTPException(status_code=502, detail="price source rate limited")
        rows = payload.get("data")
        if not isinstance(rows, list) or not rows:
            raise HTTPException(status_code=404, detail="no price bars found")

        bars: list[PriceBarRead] = []
        for raw in rows:
            dt_raw = str(raw.get("date") or "").strip()
            dt = dt_raw[:10]
            open_price = _parse_float(raw.get("open"))
            high_price = _parse_float(raw.get("high"))
            low_price = _parse_float(raw.get("low"))
            close_price = _parse_float(raw.get("close"))
            volume = _parse_float(raw.get("volume")) or 0
            row_symbol = str(raw.get("symbol") or "").strip().upper()
            row_exchange = str(raw.get("exchange") or "").strip().upper()
            if row_symbol and row_symbol != vendor_symbol:
                continue
            if exchange_mic and row_exchange and row_exchange != exchange_mic:
                continue
            if not dt:
                continue
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


def get_price_provider() -> MarketstackPriceProvider:
    provider = str(settings.price_provider or "").strip().lower()
    if provider in {"", "marketstack"}:
        return MarketstackPriceProvider()
    raise HTTPException(status_code=503, detail="unsupported price provider")
