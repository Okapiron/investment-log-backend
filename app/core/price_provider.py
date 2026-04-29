from datetime import date, datetime, time as dt_time, timedelta, timezone
import json
import time
from dataclasses import dataclass
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from fastapi import HTTPException

from app.core.config import settings
from app.schemas.price import PriceBarRead

SUPPORTED_INTERVALS = {"1d", "1w", "1m"}


def _parse_float(value) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "none":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _aggregate_bars(bars: list[PriceBarRead], interval: str) -> list[PriceBarRead]:
    if interval == "1d":
        return bars

    buckets: dict[str, list[PriceBarRead]] = {}
    for bar in bars:
        if interval == "1w":
            iso = date.fromisoformat(bar.time).isocalendar()
            bucket_key = f"{int(iso[0]):04d}-W{int(iso[1]):02d}"
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


class _BasePriceProvider:
    name = "base"

    def _cache_key(self, market: str, symbol: str) -> str:
        return f"{self.name}:{str(market or '').strip().upper()}:{str(symbol or '').strip().upper()}"

    def _read_cached_daily_bars(self, market: str, symbol: str) -> Optional[list[PriceBarRead]]:
        cached = _CACHE.get(self._cache_key(market, symbol))
        if not cached:
            return None
        if cached.expires_at > time.time():
            return cached.bars
        return None

    def _read_stale_daily_bars(self, market: str, symbol: str) -> Optional[list[PriceBarRead]]:
        cached = _CACHE.get(self._cache_key(market, symbol))
        return cached.bars if cached and cached.bars else None

    def _write_cache(self, market: str, symbol: str, bars: list[PriceBarRead]) -> None:
        ttl_seconds = max(300, int(settings.price_cache_ttl_seconds))
        _CACHE[self._cache_key(market, symbol)] = _CacheEntry(
            expires_at=time.time() + ttl_seconds,
            bars=bars,
        )

    def _get_or_fetch_daily_bars(self, market: str, symbol: str) -> list[PriceBarRead]:
        cached = self._read_cached_daily_bars(market, symbol)
        if cached:
            return cached
        try:
            bars = self._fetch_daily_bars(market, symbol)
            self._write_cache(market, symbol, bars)
            return bars
        except HTTPException:
            stale = self._read_stale_daily_bars(market, symbol)
            if stale:
                return stale
            raise

    def get_bars(self, market: str, symbol: str, interval: str) -> list[PriceBarRead]:
        if interval not in SUPPORTED_INTERVALS:
            raise HTTPException(status_code=422, detail="interval currently supports only 1d, 1w, 1m")
        daily_bars = self._get_or_fetch_daily_bars(market, symbol)
        return _aggregate_bars(daily_bars, interval)


class YahooUnofficialPriceProvider(_BasePriceProvider):
    name = "yahoo_unofficial"

    def _normalize_symbol(self, market: str, symbol: str) -> str:
        normalized_market = str(market or "").strip().upper()
        normalized_symbol = str(symbol or "").strip().upper()
        if not normalized_symbol:
            raise HTTPException(status_code=422, detail="symbol is required")
        if normalized_market == "JP":
            return normalized_symbol if normalized_symbol.endswith(".T") else f"{normalized_symbol}.T"
        if normalized_market == "US":
            return normalized_symbol.replace(".", "-")
        raise HTTPException(status_code=422, detail="market must be JP or US")

    def _fetch_daily_bars(self, market: str, symbol: str) -> list[PriceBarRead]:
        vendor_symbol = self._normalize_symbol(market, symbol)
        today = date.today()
        period_from = today - timedelta(days=max(90, int(settings.price_history_days)))
        period1 = int(datetime.combine(period_from, dt_time.min, tzinfo=timezone.utc).timestamp())
        period2 = int(datetime.combine(today + timedelta(days=1), dt_time.min, tzinfo=timezone.utc).timestamp())
        params = urlencode(
            {
                "interval": "1d",
                "period1": period1,
                "period2": period2,
                "includeAdjustedClose": "true",
                "events": "div,splits",
            }
        )
        base_url = str(settings.yahoo_chart_base_url or "https://query1.finance.yahoo.com/v8/finance/chart").rstrip("/")
        url = f"{base_url}/{vendor_symbol}?{params}"
        req = Request(
            url=url,
            method="GET",
            headers={
                "User-Agent": str(settings.yahoo_user_agent or "").strip(),
                "Accept": "application/json",
            },
        )

        try:
            with urlopen(req, timeout=10) as res:
                payload = json.loads(res.read().decode("utf-8", errors="ignore"))
        except HTTPError as exc:
            if exc.code == 429:
                raise HTTPException(status_code=502, detail="price source rate limited")
            raise HTTPException(status_code=502, detail=f"price source http error: {exc.code}")
        except URLError:
            raise HTTPException(status_code=502, detail="price source unavailable")
        except Exception:
            raise HTTPException(status_code=502, detail="failed to fetch price data")

        chart = payload.get("chart") if isinstance(payload, dict) else None
        if not isinstance(chart, dict):
            raise HTTPException(status_code=502, detail="price source unavailable")

        error = chart.get("error")
        if error:
            message = str(error.get("description") or error.get("code") or "price source unavailable")
            raise HTTPException(status_code=502, detail=message)

        results = chart.get("result")
        if not isinstance(results, list) or not results:
            raise HTTPException(status_code=404, detail="no price bars found")

        first = results[0] or {}
        timestamps = first.get("timestamp")
        indicators = first.get("indicators") or {}
        quotes = indicators.get("quote") if isinstance(indicators, dict) else None
        quote = quotes[0] if isinstance(quotes, list) and quotes else None
        if not isinstance(timestamps, list) or not isinstance(quote, dict):
            raise HTTPException(status_code=404, detail="no price bars found")

        opens = quote.get("open") or []
        highs = quote.get("high") or []
        lows = quote.get("low") or []
        closes = quote.get("close") or []
        volumes = quote.get("volume") or []

        bars: list[PriceBarRead] = []
        for idx, ts in enumerate(timestamps):
            if not isinstance(ts, (int, float)):
                continue
            open_price = _parse_float(opens[idx] if idx < len(opens) else None)
            high_price = _parse_float(highs[idx] if idx < len(highs) else None)
            low_price = _parse_float(lows[idx] if idx < len(lows) else None)
            close_price = _parse_float(closes[idx] if idx < len(closes) else None)
            volume = _parse_float(volumes[idx] if idx < len(volumes) else None) or 0
            if None in (open_price, high_price, low_price, close_price):
                continue
            bars.append(
                PriceBarRead(
                    time=time.strftime("%Y-%m-%d", time.gmtime(float(ts))),
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


class MarketstackPriceProvider(_BasePriceProvider):
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
            if not dt or None in (open_price, high_price, low_price, close_price):
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


def get_price_provider():
    provider = str(settings.price_provider or "").strip().lower()
    if provider in {"", "yahoo", "yahoo_unofficial"}:
        if not settings.allow_unofficial_price_source:
            raise HTTPException(status_code=503, detail="unofficial price source is disabled")
        return YahooUnofficialPriceProvider()
    if provider == "marketstack":
        return MarketstackPriceProvider()
    raise HTTPException(status_code=503, detail="unsupported price provider")
