from functools import cmp_to_key
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload

from app.api.deps import get_session
from app.core.errors import raise_409_from_integrity
from app.crud.trades import (
    compute_profit_holding,
    create_trade_with_fills,
    fetch_trade,
    update_trade_with_fills,
)
from app.db.models import Fill, Trade
from app.schemas.trade import FillRead, TradeCreate, TradeListRead, TradeListStatsRead, TradeRead, TradeUpdate

router = APIRouter(prefix="/trades", tags=["trades"])


def _to_trade_read(trade: Trade) -> TradeRead:
    fills = sorted(trade.fills, key=lambda x: x.side)
    fill_map = {fill.side: fill for fill in trade.fills}
    buy = fill_map.get("buy")
    sell = fill_map.get("sell")
    if buy is None:
        raise HTTPException(status_code=409, detail="trade fills are inconsistent")

    is_open = sell is None
    profit_jpy = None
    profit_usd = None
    profit_currency = "JPY" if trade.market == "JP" else "USD"
    holding_days = None
    if sell is not None:
        profit_value, holding_days = compute_profit_holding(buy, sell)
        if trade.market == "JP":
            profit_jpy = profit_value
        else:
            profit_usd = profit_value

    return TradeRead(
        id=trade.id,
        market=trade.market,
        symbol=trade.symbol,
        name=trade.name,
        notes_buy=trade.notes_buy,
        notes_sell=trade.notes_sell,
        notes_review=trade.notes_review,
        rating=trade.rating,
        tags=trade.tags,
        chart_image_url=trade.chart_image_url,
        opened_at=trade.opened_at,
        closed_at=trade.closed_at or None,
        created_at=trade.created_at,
        updated_at=trade.updated_at,
        fills=[
            FillRead(
                id=fill.id,
                trade_id=fill.trade_id,
                side=fill.side,
                date=fill.date,
                price=fill.price,
                qty=fill.qty,
                fee=fill.fee or 0,
            )
            for fill in fills
        ],
        profit_jpy=profit_jpy,
        profit_usd=profit_usd,
        profit_currency=profit_currency,
        holding_days=holding_days,
        is_open=is_open,
    )


def _parse_csv(raw: Optional[str]) -> list[str]:
    if not raw:
        return []
    values = []
    seen = set()
    for part in str(raw).split(","):
        v = part.strip()
        if not v:
            continue
        if v in seen:
            continue
        seen.add(v)
        values.append(v)
    return values


def _normalize_sort(value: str) -> str:
    legacy_map = {
        "newest": "sell_date",
        "oldest": "sell_date",
        "profit_desc": "profit",
        "profit_asc": "profit",
        "roi_desc": "roi",
        "roi_asc": "roi",
        "holding_desc": "holding",
        "holding_asc": "holding",
        "rating_desc": "rating",
        "rating_asc": "rating",
    }
    v = (value or "").strip()
    if v in legacy_map:
        return legacy_map[v]
    allowed = {"buy_date", "sell_date", "name", "profit", "roi", "holding", "rating"}
    return v if v in allowed else "sell_date"


def _normalize_sort_dir(sort: str, sort_dir: str) -> str:
    v = (sort_dir or "").strip()
    if v in {"asc", "desc"}:
        return v
    if sort == "newest":
        return "desc"
    if sort == "oldest":
        return "asc"
    if sort.endswith("_asc"):
        return "asc"
    if sort.endswith("_desc"):
        return "desc"
    return "desc"


def _is_open_trade(item: TradeRead) -> bool:
    if item.is_open:
        return True
    return item.closed_at is None


def _profit_value(item: TradeRead) -> Optional[float]:
    if item.profit_currency == "USD":
        return None if item.profit_usd is None else float(item.profit_usd)
    return None if item.profit_jpy is None else float(item.profit_jpy)


def _roi_value(item: TradeRead) -> Optional[float]:
    if _is_open_trade(item):
        return None
    buy = None
    for fill in item.fills:
        if fill.side == "buy":
            buy = fill
            break
    if buy is None:
        return None
    principal = float(buy.price) * float(buy.qty) + float(buy.fee or 0)
    if principal <= 0:
        return None
    profit = _profit_value(item)
    if profit is None:
        return None
    return (profit / principal) * 100.0


def _compare_nullable(a, b, asc: bool) -> int:
    a_missing = a is None or a == ""
    b_missing = b is None or b == ""
    if a_missing and b_missing:
        return 0
    if a_missing:
        return 1
    if b_missing:
        return -1
    if a < b:
        return -1 if asc else 1
    if a > b:
        return 1 if asc else -1
    return 0


@router.get("", response_model=TradeListRead)
def list_trades(
    limit: int = Query(default=20, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    q: Optional[str] = None,
    market: Optional[str] = None,
    rating: Optional[str] = None,
    tag: Optional[str] = None,
    pos: str = "all",
    win_only: Optional[str] = None,
    loss_only: Optional[str] = None,
    win_from: Optional[str] = None,
    win_to: Optional[str] = None,
    sort: str = "sell_date",
    sort_dir: str = "desc",
    # legacy compatibility
    symbol: Optional[str] = None,
    memo: Optional[str] = None,
    from_: Optional[str] = Query(default=None, alias="from"),
    to: Optional[str] = None,
    db: Session = Depends(get_session),
):
    stmt = select(Trade).options(selectinload(Trade.fills))
    # minimal DB prefilter for obvious dimensions
    market_values = [v for v in _parse_csv(market) if v in {"JP", "US"}]
    if market_values:
        stmt = stmt.where(Trade.market.in_(market_values))

    rating_values = []
    for v in _parse_csv(rating):
        try:
            n = int(v)
            if 1 <= n <= 5:
                rating_values.append(n)
        except ValueError:
            continue
    if rating_values:
        stmt = stmt.where(Trade.rating.in_(rating_values))

    if symbol and not q:
        q = symbol
    if from_ and not win_from:
        win_from = from_
    if to and not win_to:
        win_to = to

    trades = [_to_trade_read(trade) for trade in list(db.scalars(stmt).all())]

    q_lower = (q or "").strip().lower()
    memo_lower = (memo or "").strip().lower()
    tag_values_raw = _parse_csv(tag)
    tag_values = [v.lower() for v in tag_values_raw]
    has_unset_tag = "未設定" in tag_values_raw
    pos_value = pos if pos in {"all", "open", "closed"} else "all"
    win_only_enabled = win_only == "1"
    loss_only_enabled = loss_only == "1"

    filtered = []
    for item in trades:
        if pos_value == "open" and not _is_open_trade(item):
            continue
        if pos_value == "closed" and _is_open_trade(item):
            continue

        tags = [v.strip() for v in (item.tags or "").split(",") if v.strip()]
        tags_lower = [v.lower() for v in tags]
        if tag_values:
            tag_hit = False
            if has_unset_tag and len(tags_lower) == 0:
                tag_hit = True
            if not tag_hit:
                for tv in tag_values:
                    if tv == "未設定":
                        continue
                    if tv in tags_lower:
                        tag_hit = True
                        break
            if not tag_hit:
                continue

        if q_lower:
            haystack = " ".join(
                [
                    item.symbol or "",
                    item.name or "",
                    item.tags or "",
                    item.notes_buy or "",
                    item.notes_sell or "",
                    item.notes_review or "",
                ]
            ).lower()
            if q_lower not in haystack:
                continue

        if memo_lower:
            memo_haystack = " ".join([item.notes_buy or "", item.notes_sell or "", item.notes_review or ""]).lower()
            if memo_lower not in memo_haystack:
                continue

        if win_only_enabled or loss_only_enabled or win_from or win_to:
            closed = item.closed_at
            if not closed:
                continue
            profit = _profit_value(item)
            if profit is None:
                continue
            if win_only_enabled and profit <= 0:
                continue
            if loss_only_enabled and profit >= 0:
                continue
            if win_from:
                if closed < win_from:
                    continue
            if win_to:
                if closed > win_to:
                    continue

        filtered.append(item)

    normalized_sort = _normalize_sort(sort)
    normalized_sort_dir = _normalize_sort_dir(sort, sort_dir)
    is_asc = normalized_sort_dir == "asc"

    indexed = list(enumerate(filtered))

    def sort_value(item: TradeRead):
        if normalized_sort == "buy_date":
            return item.opened_at or item.created_at
        if normalized_sort == "sell_date":
            return item.closed_at or item.created_at
        if normalized_sort == "name":
            name = (item.name or "").strip()
            return name if name else None
        if normalized_sort == "profit":
            return _profit_value(item)
        if normalized_sort == "roi":
            return _roi_value(item)
        if normalized_sort == "holding":
            return item.holding_days
        if normalized_sort == "rating":
            return item.rating if item.rating and item.rating > 0 else None
        return item.closed_at or item.created_at

    def compare(a, b):
        ai, at = a
        bi, bt = b
        cmp = _compare_nullable(sort_value(at), sort_value(bt), is_asc)
        if cmp != 0:
            return cmp
        return ai - bi

    indexed.sort(key=cmp_to_key(compare))
    sorted_items = [t for _, t in indexed]

    total = len(sorted_items)
    page_items = sorted_items[offset : offset + limit]

    total_profit_jpy = 0.0
    total_profit_usd = 0.0
    win_count = 0
    closed_count = 0
    holding_sum = 0.0
    holding_count = 0
    roi_sum = 0.0
    roi_count = 0
    rating_sum = 0.0
    rating_count = 0

    for item in sorted_items:
        profit = _profit_value(item)
        if profit is not None:
            if item.profit_currency == "USD":
                total_profit_usd += profit
            else:
                total_profit_jpy += profit
            if profit > 0:
                win_count += 1
            closed_count += 1

        if item.holding_days is not None:
            holding_sum += float(item.holding_days)
            holding_count += 1

        roi = _roi_value(item)
        if roi is not None:
            roi_sum += roi
            roi_count += 1

        if item.rating is not None and item.rating > 0:
            rating_sum += float(item.rating)
            rating_count += 1

    stats = TradeListStatsRead(
        total_profit_jpy=total_profit_jpy,
        total_profit_usd=total_profit_usd,
        win_rate=(float(win_count) / float(closed_count) * 100.0) if closed_count > 0 else None,
        avg_holding_days=(holding_sum / float(holding_count)) if holding_count > 0 else None,
        avg_roi_pct=(roi_sum / float(roi_count)) if roi_count > 0 else None,
        avg_rating=(rating_sum / float(rating_count)) if rating_count > 0 else None,
    )

    return TradeListRead(items=page_items, total=total, limit=limit, offset=offset, stats=stats)


@router.post("", response_model=TradeRead, status_code=201)
def create_trade(payload: TradeCreate, db: Session = Depends(get_session)):
    trade = create_trade_with_fills(db, payload)
    try:
        db.commit()
    except IntegrityError as e:
        db.rollback()
        raise_409_from_integrity(e)

    reloaded = fetch_trade(db, trade.id)
    if reloaded is None:
        raise HTTPException(status_code=404, detail="trade not found")
    return _to_trade_read(reloaded)


@router.get("/{trade_id}", response_model=TradeRead)
def get_trade(trade_id: int, db: Session = Depends(get_session)):
    trade = fetch_trade(db, trade_id)
    if trade is None:
        raise HTTPException(status_code=404, detail="trade not found")
    return _to_trade_read(trade)


@router.patch("/{trade_id}", response_model=TradeRead)
def update_trade(trade_id: int, payload: TradeUpdate, db: Session = Depends(get_session)):
    trade = fetch_trade(db, trade_id)
    if trade is None:
        raise HTTPException(status_code=404, detail="trade not found")

    update_trade_with_fills(db, trade, payload)

    try:
        db.commit()
    except IntegrityError as e:
        db.rollback()
        raise_409_from_integrity(e)

    reloaded = fetch_trade(db, trade_id)
    if reloaded is None:
        raise HTTPException(status_code=404, detail="trade not found")
    return _to_trade_read(reloaded)


@router.delete("/{trade_id}", status_code=204)
def delete_trade(trade_id: int, db: Session = Depends(get_session)):
    trade = db.get(Trade, trade_id)
    if trade is None:
        raise HTTPException(status_code=404, detail="trade not found")

    db.delete(trade)
    db.commit()
    return None
