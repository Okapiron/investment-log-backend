from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, Tuple

from fastapi import HTTPException
from sqlalchemy import Select, or_, select
from sqlalchemy.orm import Session, selectinload

from app.db.models import Fill, Trade
from app.schemas.trade import FillInput, TradeCreate, TradeUpdate


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise HTTPException(status_code=422, detail="date must be in YYYY-MM-DD format")


def _validate_market(market: str) -> None:
    if market not in ("JP", "US"):
        raise HTTPException(status_code=422, detail="market must be JP or US")


def _validate_position_side(position_side: str) -> str:
    normalized = str(position_side or "").strip().lower()
    if normalized not in {"long", "short"}:
        raise HTTPException(status_code=422, detail="position_side must be long or short")
    return normalized


def _validate_data_quality(data_quality: str) -> str:
    normalized = str(data_quality or "").strip().lower()
    if normalized not in {"full", "realized_only"}:
        raise HTTPException(status_code=422, detail="data_quality must be full or realized_only")
    return normalized


def _open_fill_side(position_side: str) -> str:
    return "buy" if position_side == "long" else "sell"


def _close_fill_side(position_side: str) -> str:
    return "sell" if position_side == "long" else "buy"


def _price_decimal_places(value: Decimal) -> int:
    exponent = value.as_tuple().exponent
    if exponent >= 0:
        return 0
    return -exponent


def _normalize_price_for_market(market: str, price: Decimal, side: str) -> Decimal:
    if price <= 0:
        raise HTTPException(status_code=422, detail=f"{side}.price must be greater than 0")

    if market == "JP":
        if _price_decimal_places(price) > 2:
            raise HTTPException(status_code=422, detail=f"{side}.price must allow up to 2 decimal places for JP market")
        return price.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    if market == "US":
        if _price_decimal_places(price) > 2:
            raise HTTPException(status_code=422, detail=f"{side}.price must allow up to 2 decimal places for US market")
        return price.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    raise HTTPException(status_code=422, detail="market must be JP or US")


def _normalize_fill_prices_for_market(market: str, open_fill: FillInput, close_fill: Optional[FillInput]) -> None:
    open_fill.price = _normalize_price_for_market(market, open_fill.price, open_fill.side)
    if close_fill is not None:
        close_fill.price = _normalize_price_for_market(market, close_fill.price, close_fill.side)


def _extract_open_close_optional(fills: list[FillInput], position_side: str) -> Tuple[FillInput, Optional[FillInput]]:
    if len(fills) < 1 or len(fills) > 2:
        raise HTTPException(status_code=422, detail="fills must contain open or open+close")

    by_side = {}
    for fill in fills:
        if fill.side in by_side:
            raise HTTPException(status_code=422, detail="fills contains duplicate side")
        by_side[fill.side] = fill

    open_side = _open_fill_side(position_side)
    close_side = _close_fill_side(position_side)
    open_fill = by_side.get(open_side)
    close_fill = by_side.get(close_side)

    if open_fill is None:
        raise HTTPException(status_code=422, detail=f"fills must include {open_side}")
    if close_fill is not None:
        if open_fill.qty != close_fill.qty:
            raise HTTPException(status_code=422, detail=f"{open_side}.qty and {close_side}.qty must match")

        open_date = _parse_iso_date(open_fill.date)
        close_date = _parse_iso_date(close_fill.date)
        if close_date < open_date:
            raise HTTPException(status_code=422, detail=f"{close_side}.date must be greater than or equal to {open_side}.date")

    return open_fill, close_fill


def _fill_fee_total(fill) -> Decimal:
    explicit_total = getattr(fill, "fee_total_jpy", None)
    if explicit_total is not None:
        return Decimal(str(explicit_total))
    return Decimal(str(getattr(fill, "fee", 0) or 0))


def _fill_fee_component(fill, key: str) -> Decimal:
    value = getattr(fill, key, None)
    if value is None:
        return Decimal("0")
    return Decimal(str(value))


def compute_trade_financials(open_fill: Fill, close_fill: Fill, position_side: str) -> dict[str, float]:
    open_price = Decimal(str(open_fill.price))
    close_price = Decimal(str(close_fill.price))
    qty = Decimal(str(open_fill.qty))
    open_cost = _fill_fee_total(open_fill)
    close_cost = _fill_fee_total(close_fill)

    if position_side == "short":
        gross = (open_price - close_price) * qty
    else:
        gross = (close_price - open_price) * qty

    net = gross - open_cost - close_cost
    holding_days = (_parse_iso_date(close_fill.date) - _parse_iso_date(open_fill.date)).days
    return {
        "gross_profit_jpy": float(gross.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)),
        "net_profit_jpy": float(net.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)),
        "holding_days": holding_days,
        "open_leg_cost_jpy": float(open_cost),
        "close_leg_cost_jpy": float(close_cost),
        "total_commission_jpy": float(
            _fill_fee_component(open_fill, "fee_commission_jpy") + _fill_fee_component(close_fill, "fee_commission_jpy")
        ),
        "total_tax_jpy": float(_fill_fee_component(open_fill, "fee_tax_jpy") + _fill_fee_component(close_fill, "fee_tax_jpy")),
        "total_other_cost_jpy": float(
            _fill_fee_component(open_fill, "fee_other_jpy") + _fill_fee_component(close_fill, "fee_other_jpy")
        ),
    }


def compute_profit_holding(open_fill: Fill, close_fill: Fill, position_side: str = "long") -> Tuple[float, int]:
    result = compute_trade_financials(open_fill, close_fill, position_side)
    return float(result["net_profit_jpy"]), int(result["holding_days"])


def _has_non_empty_text(value: Optional[str]) -> bool:
    return bool(str(value or "").strip())


def _has_any_tag(csv_text: Optional[str]) -> bool:
    return len([x.strip() for x in str(csv_text or "").split(",") if x.strip()]) > 0


def review_completion_missing_items(trade: Trade) -> list[str]:
    missing = []
    is_closed = bool(str(trade.closed_at or "").strip())

    if not is_closed:
        missing.append("売却データ")
    if not _has_any_tag(trade.tags):
        missing.append("タグ")
    if trade.rating is None or int(trade.rating) <= 0:
        missing.append("評価")
    if not _has_non_empty_text(trade.notes_buy):
        missing.append("購入理由")
    if not _has_non_empty_text(trade.notes_sell):
        missing.append("売却理由")
    if not _has_non_empty_text(trade.notes_review):
        missing.append("考察")
    return missing


def create_trade_with_fills(db: Session, payload: TradeCreate, user_id: Optional[str] = None) -> Trade:
    _validate_market(payload.market)
    position_side = _validate_position_side(payload.position_side or "long")
    open_input, close_input = _extract_open_close_optional(payload.fills, position_side)
    _normalize_fill_prices_for_market(payload.market, open_input, close_input)

    trade = Trade(
        user_id=(str(user_id).strip() or None) if user_id is not None else None,
        market=payload.market,
        position_side=position_side,
        data_quality=_validate_data_quality(payload.data_quality or "full"),
        broker_profit_jpy=payload.broker_profit_jpy,
        symbol=payload.symbol,
        name=payload.name,
        notes_buy=payload.notes_buy,
        notes_sell=payload.notes_sell,
        notes_review=payload.notes_review,
        rating=payload.rating,
        tags=payload.tags,
        chart_image_url=payload.chart_image_url,
        # New trades are always pending review by design.
        review_done=False,
        reviewed_at=None,
        opened_at=open_input.date,
        closed_at=close_input.date if close_input is not None else "",
        created_at=_utc_now_iso(),
        updated_at=_utc_now_iso(),
    )
    db.add(trade)
    db.flush()

    db.add(
        Fill(
            trade_id=trade.id,
            side=open_input.side,
            date=open_input.date,
            price=open_input.price,
            qty=open_input.qty,
            fee=open_input.fee or open_input.fee_total_jpy or 0,
            fee_commission_jpy=open_input.fee_commission_jpy,
            fee_tax_jpy=open_input.fee_tax_jpy,
            fee_other_jpy=open_input.fee_other_jpy,
            fee_total_jpy=open_input.fee_total_jpy if open_input.fee_total_jpy is not None else open_input.fee or 0,
        )
    )
    if close_input is not None:
        db.add(
            Fill(
                trade_id=trade.id,
                side=close_input.side,
                date=close_input.date,
                price=close_input.price,
                qty=close_input.qty,
                fee=close_input.fee or close_input.fee_total_jpy or 0,
                fee_commission_jpy=close_input.fee_commission_jpy,
                fee_tax_jpy=close_input.fee_tax_jpy,
                fee_other_jpy=close_input.fee_other_jpy,
                fee_total_jpy=close_input.fee_total_jpy if close_input.fee_total_jpy is not None else close_input.fee or 0,
            )
        )
    return trade


def update_trade_with_fills(db: Session, trade: Trade, payload: TradeUpdate) -> Trade:
    data = payload.model_dump(exclude_unset=True)

    if "market" in data and data["market"] is not None:
        _validate_market(data["market"])
    if "position_side" in data and data["position_side"] is not None:
        data["position_side"] = _validate_position_side(data["position_side"])
    if "data_quality" in data and data["data_quality"] is not None:
        data["data_quality"] = _validate_data_quality(data["data_quality"])

    fills_payload = data.pop("fills", None)
    buy_date = data.pop("buy_date", None)
    buy_price = data.pop("buy_price", None)
    buy_qty = data.pop("buy_qty", None)
    sell_date = data.pop("sell_date", None)
    sell_price = data.pop("sell_price", None)
    sell_qty = data.pop("sell_qty", None)

    has_trade_fill_fields = any(v is not None for v in [buy_date, buy_price, buy_qty, sell_date, sell_price, sell_qty])

    for key, value in data.items():
        setattr(trade, key, value)

    effective_market = data.get("market") or trade.market
    effective_position_side = data.get("position_side") or trade.position_side or "long"

    if fills_payload is not None or has_trade_fill_fields:
        normalized = None
        if fills_payload is not None:
            normalized = [FillInput(**item) if isinstance(item, dict) else item for item in fills_payload]
        else:
            if buy_date is None or buy_price is None or buy_qty is None:
                raise HTTPException(status_code=422, detail="buy_date, buy_price and buy_qty are required")

            has_any_sell = sell_date is not None or sell_price is not None or sell_qty is not None
            has_all_sell = sell_date is not None and sell_price is not None and sell_qty is not None
            if has_any_sell and not has_all_sell:
                raise HTTPException(status_code=422, detail="sell_date, sell_price and sell_qty must be all set together")
            normalized = [FillInput(side="buy", date=buy_date, price=buy_price, qty=buy_qty, fee=0)]
            if has_all_sell:
                normalized.append(FillInput(side="sell", date=sell_date, price=sell_price, qty=sell_qty, fee=0))

        open_input, close_input = _extract_open_close_optional(normalized, effective_position_side)
        _normalize_fill_prices_for_market(effective_market, open_input, close_input)

        existing = {fill.side: fill for fill in trade.fills}
        open_fill = existing.get(_open_fill_side(effective_position_side))
        close_fill = existing.get(_close_fill_side(effective_position_side))

        for fill in list(trade.fills):
            if fill.side not in {_open_fill_side(effective_position_side), _close_fill_side(effective_position_side)}:
                db.delete(fill)

        if open_fill is None:
            open_fill = Fill(trade_id=trade.id, side=open_input.side, date=open_input.date, price=0, qty=1, fee=0)
            db.add(open_fill)
        open_fill.side = open_input.side
        open_fill.date = open_input.date
        open_fill.price = open_input.price
        open_fill.qty = open_input.qty
        open_fill.fee = open_input.fee or open_input.fee_total_jpy or 0
        open_fill.fee_commission_jpy = open_input.fee_commission_jpy
        open_fill.fee_tax_jpy = open_input.fee_tax_jpy
        open_fill.fee_other_jpy = open_input.fee_other_jpy
        open_fill.fee_total_jpy = open_input.fee_total_jpy if open_input.fee_total_jpy is not None else open_input.fee or 0

        if close_input is not None:
            if close_fill is None:
                close_fill = Fill(trade_id=trade.id, side=close_input.side, date=close_input.date, price=0, qty=1, fee=0)
                db.add(close_fill)

            close_fill.side = close_input.side
            close_fill.date = close_input.date
            close_fill.price = close_input.price
            close_fill.qty = close_input.qty
            close_fill.fee = close_input.fee or close_input.fee_total_jpy or 0
            close_fill.fee_commission_jpy = close_input.fee_commission_jpy
            close_fill.fee_tax_jpy = close_input.fee_tax_jpy
            close_fill.fee_other_jpy = close_input.fee_other_jpy
            close_fill.fee_total_jpy = close_input.fee_total_jpy if close_input.fee_total_jpy is not None else close_input.fee or 0
        elif close_fill is not None:
            db.delete(close_fill)

        trade.position_side = effective_position_side
        trade.opened_at = open_input.date
        trade.closed_at = close_input.date if close_input is not None else ""
    elif "market" in data:
        for fill in trade.fills:
            _normalize_price_for_market(effective_market, Decimal(str(fill.price)), fill.side)

    # Keep open positions unrated to avoid status/filter noise.
    if not str(trade.closed_at or "").strip():
        trade.rating = None

    # If review requirements are no longer satisfied, keep trade as pending review.
    if review_completion_missing_items(trade):
        trade.review_done = False
        trade.reviewed_at = None

    trade.updated_at = _utc_now_iso()
    return trade


def apply_trade_filters(
    stmt: Select,
    market: Optional[str],
    symbol: Optional[str],
    tag: Optional[str],
    rating: Optional[int],
    from_: Optional[str],
    to: Optional[str],
    memo: Optional[str],
) -> Select:
    if market:
        stmt = stmt.where(Trade.market == market)
    if symbol:
        stmt = stmt.where(Trade.symbol.like(f"%{symbol}%"))
    if tag:
        stmt = stmt.where(Trade.tags.like(f"%{tag}%"))
    if rating is not None:
        stmt = stmt.where(Trade.rating == rating)
    if from_:
        _parse_iso_date(from_)
        stmt = stmt.where(Trade.closed_at != "", Trade.closed_at >= from_)
    if to:
        _parse_iso_date(to)
        stmt = stmt.where(Trade.closed_at != "", Trade.closed_at <= to)
    if memo:
        stmt = stmt.where(
            or_(
                Trade.notes_buy.like(f"%{memo}%"),
                Trade.notes_sell.like(f"%{memo}%"),
                Trade.notes_review.like(f"%{memo}%"),
            )
        )
    return stmt


def fetch_trade(db: Session, trade_id: int, user_id: Optional[str] = None) -> Optional[Trade]:
    stmt = select(Trade).options(selectinload(Trade.fills)).where(Trade.id == trade_id)
    if user_id is not None:
        stmt = stmt.where(Trade.user_id == user_id)
    return db.scalar(stmt)
