from datetime import date, datetime, timezone
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


def _extract_buy_sell_optional(fills: list[FillInput]) -> Tuple[FillInput, Optional[FillInput]]:
    if len(fills) < 1 or len(fills) > 2:
        raise HTTPException(status_code=422, detail="fills must contain buy or buy+sell")

    by_side = {}
    for fill in fills:
        if fill.side in by_side:
            raise HTTPException(status_code=422, detail="fills contains duplicate side")
        by_side[fill.side] = fill

    buy = by_side.get("buy")
    sell = by_side.get("sell")

    if buy is None:
        raise HTTPException(status_code=422, detail="fills must include buy")

    if sell is not None:
        if buy.qty != sell.qty:
            raise HTTPException(status_code=422, detail="buy.qty and sell.qty must match")

        buy_date = _parse_iso_date(buy.date)
        sell_date = _parse_iso_date(sell.date)
        if sell_date < buy_date:
            raise HTTPException(status_code=422, detail="sell.date must be greater than or equal to buy.date")

    return buy, sell


def compute_profit_holding(buy: Fill, sell: Fill) -> Tuple[int, int]:
    buy_fee = buy.fee or 0
    sell_fee = sell.fee or 0
    qty = buy.qty
    profit = (sell.price - buy.price) * qty - (buy_fee + sell_fee)
    holding_days = (_parse_iso_date(sell.date) - _parse_iso_date(buy.date)).days
    return profit, holding_days


def create_trade_with_fills(db: Session, payload: TradeCreate) -> Trade:
    _validate_market(payload.market)
    buy_input, sell_input = _extract_buy_sell_optional(payload.fills)

    trade = Trade(
        market=payload.market,
        symbol=payload.symbol,
        name=payload.name,
        notes_buy=payload.notes_buy,
        notes_sell=payload.notes_sell,
        notes_review=payload.notes_review,
        rating=payload.rating,
        tags=payload.tags,
        chart_image_url=payload.chart_image_url,
        review_done=bool(payload.review_done),
        reviewed_at=payload.reviewed_at,
        opened_at=buy_input.date,
        closed_at=sell_input.date if sell_input is not None else "",
        created_at=_utc_now_iso(),
        updated_at=_utc_now_iso(),
    )
    db.add(trade)
    db.flush()

    db.add(
        Fill(
            trade_id=trade.id,
            side=buy_input.side,
            date=buy_input.date,
            price=buy_input.price,
            qty=buy_input.qty,
            fee=buy_input.fee or 0,
        )
    )
    if sell_input is not None:
        db.add(
            Fill(
                trade_id=trade.id,
                side=sell_input.side,
                date=sell_input.date,
                price=sell_input.price,
                qty=sell_input.qty,
                fee=sell_input.fee or 0,
            )
        )
    return trade


def update_trade_with_fills(db: Session, trade: Trade, payload: TradeUpdate) -> Trade:
    data = payload.model_dump(exclude_unset=True)

    if "market" in data and data["market"] is not None:
        _validate_market(data["market"])

    fills_payload = data.pop("fills", None)

    for key, value in data.items():
        setattr(trade, key, value)

    if fills_payload is not None:
        normalized = [FillInput(**item) if isinstance(item, dict) else item for item in fills_payload]
        buy_input, sell_input = _extract_buy_sell_optional(normalized)

        existing = {fill.side: fill for fill in trade.fills}
        buy_fill = existing.get("buy")
        sell_fill = existing.get("sell")

        if buy_fill is None:
            buy_fill = Fill(trade_id=trade.id, side="buy", date=buy_input.date, price=0, qty=1, fee=0)
            db.add(buy_fill)
        buy_fill.date = buy_input.date
        buy_fill.price = buy_input.price
        buy_fill.qty = buy_input.qty
        buy_fill.fee = buy_input.fee or 0

        if sell_input is not None:
            if sell_fill is None:
                sell_fill = Fill(trade_id=trade.id, side="sell", date=sell_input.date, price=0, qty=1, fee=0)
                db.add(sell_fill)

            sell_fill.date = sell_input.date
            sell_fill.price = sell_input.price
            sell_fill.qty = sell_input.qty
            sell_fill.fee = sell_input.fee or 0
        elif sell_fill is not None:
            db.delete(sell_fill)

        trade.opened_at = buy_input.date
        trade.closed_at = sell_input.date if sell_input is not None else ""

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


def fetch_trade(db: Session, trade_id: int) -> Optional[Trade]:
    return db.scalar(select(Trade).options(selectinload(Trade.fills)).where(Trade.id == trade_id))
