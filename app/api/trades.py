from functools import cmp_to_key
import unicodedata
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload

from app.api.deps import get_session, require_invited_auth
from app.core.config import settings
from app.core.errors import raise_409_from_integrity
from app.crud.trades import (
    compute_profit_holding,
    compute_trade_financials,
    create_trade_with_fills,
    fetch_trade,
    review_completion_missing_items,
    update_trade_with_fills,
)
from app.db.models import Fill, Trade, TradeImportRecord
from app.schemas.trade import FillRead, TradeCreate, TradeListRead, TradeListStatsRead, TradeRead, TradeUpdate

router = APIRouter(prefix="/trades", tags=["trades"])


def _scoped_user_id(claims: dict) -> Optional[str]:
    if not settings.auth_enabled:
        return None
    sub = str((claims or {}).get("sub") or "").strip()
    if not sub:
        raise HTTPException(status_code=401, detail="invalid auth token: sub is missing")
    return sub


def _load_partial_exit_flags(db: Session, trade_ids: list[int]) -> dict[int, bool]:
    if not trade_ids:
        return {}
    rows = db.execute(
        select(TradeImportRecord.trade_id, TradeImportRecord.is_partial_exit).where(
            TradeImportRecord.trade_id.in_(trade_ids),
            TradeImportRecord.is_partial_exit.is_(True),
        )
    ).all()
    return {int(trade_id): bool(is_partial_exit) for trade_id, is_partial_exit in rows if trade_id is not None}


def _load_import_metadata(db: Session, trade_ids: list[int]) -> dict[int, dict[str, object]]:
    if not trade_ids:
        return {}
    rows = db.execute(
        select(TradeImportRecord.trade_id, TradeImportRecord.is_partial_exit, TradeImportRecord.broker).where(
            TradeImportRecord.trade_id.in_(trade_ids)
        )
    ).all()
    metadata: dict[int, dict[str, object]] = {}
    for trade_id, is_partial_exit, broker in rows:
        if trade_id is None:
            continue
        key = int(trade_id)
        current = metadata.setdefault(key, {"is_partial_exit": False, "broker": None})
        current["is_partial_exit"] = bool(current["is_partial_exit"]) or bool(is_partial_exit)
        if not current["broker"] and broker:
            current["broker"] = str(broker)
    return metadata


def _opening_fill(fill_map: dict[str, Fill], position_side: str) -> Optional[Fill]:
    return fill_map.get("buy" if position_side == "long" else "sell")


def _closing_fill(fill_map: dict[str, Fill], position_side: str) -> Optional[Fill]:
    return fill_map.get("sell" if position_side == "long" else "buy")


def _to_trade_read(trade: Trade, *, is_partial_exit: bool = False, import_source: Optional[str] = None) -> TradeRead:
    fills = sorted(trade.fills, key=lambda x: x.side)
    fill_map = {fill.side: fill for fill in trade.fills}
    position_side = str(getattr(trade, "position_side", "long") or "long")
    opening_fill = _opening_fill(fill_map, position_side)
    closing_fill = _closing_fill(fill_map, position_side)
    if opening_fill is None:
        raise HTTPException(status_code=409, detail="trade fills are inconsistent")

    is_open = closing_fill is None
    profit_jpy = None
    profit_usd = None
    gross_profit_jpy = None
    net_profit_jpy = None
    open_leg_cost_jpy = None
    close_leg_cost_jpy = None
    total_commission_jpy = None
    total_tax_jpy = None
    total_other_cost_jpy = None
    profit_currency = "JPY" if trade.market == "JP" else "USD"
    holding_days = None
    if closing_fill is not None:
        profit_value, holding_days = compute_profit_holding(opening_fill, closing_fill, position_side=position_side)
        totals = compute_trade_financials(opening_fill, closing_fill, position_side=position_side)
        if trade.market == "JP":
            profit_jpy = profit_value
            gross_profit_jpy = float(totals["gross_profit_jpy"])
            net_profit_jpy = float(totals["net_profit_jpy"])
            open_leg_cost_jpy = float(totals["open_leg_cost_jpy"])
            close_leg_cost_jpy = float(totals["close_leg_cost_jpy"])
            total_commission_jpy = float(totals["total_commission_jpy"])
            total_tax_jpy = float(totals["total_tax_jpy"])
            total_other_cost_jpy = float(totals["total_other_cost_jpy"])
        else:
            profit_usd = profit_value

    return TradeRead(
        id=trade.id,
        market=trade.market,
        position_side=position_side,
        symbol=trade.symbol,
        name=trade.name,
        notes_buy=trade.notes_buy,
        notes_sell=trade.notes_sell,
        notes_review=trade.notes_review,
        rating=trade.rating,
        tags=trade.tags,
        chart_image_url=trade.chart_image_url,
        review_done=bool(trade.review_done),
        reviewed_at=trade.reviewed_at,
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
                fee_commission_jpy=fill.fee_commission_jpy,
                fee_tax_jpy=fill.fee_tax_jpy,
                fee_other_jpy=fill.fee_other_jpy,
                fee_total_jpy=fill.fee_total_jpy if fill.fee_total_jpy is not None else fill.fee or 0,
            )
            for fill in fills
        ],
        profit_jpy=profit_jpy,
        profit_usd=profit_usd,
        profit_currency=profit_currency,
        gross_profit_jpy=gross_profit_jpy,
        net_profit_jpy=net_profit_jpy,
        open_leg_cost_jpy=open_leg_cost_jpy,
        close_leg_cost_jpy=close_leg_cost_jpy,
        total_commission_jpy=total_commission_jpy,
        total_tax_jpy=total_tax_jpy,
        total_other_cost_jpy=total_other_cost_jpy,
        import_source=import_source,
        holding_days=holding_days,
        is_open=is_open,
        is_partial_exit=bool(is_partial_exit),
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
    allowed = {"status", "buy_date", "sell_date", "name", "profit", "roi", "holding", "rating"}
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


def _is_pending_review(item: TradeRead) -> bool:
    return _trade_status(item) == "pending"


def _trade_status(item: TradeRead) -> str:
    if _is_open_trade(item):
        return "open"
    if bool(item.review_done):
        return "complete"
    return "pending"


def _profit_value(item: TradeRead) -> Optional[float]:
    if item.profit_currency == "USD":
        return None if item.profit_usd is None else float(item.profit_usd)
    return None if item.profit_jpy is None else float(item.profit_jpy)


def _roi_value(item: TradeRead) -> Optional[float]:
    if _is_open_trade(item):
        return None
    opening_side = "buy" if item.position_side == "long" else "sell"
    opening_fill = None
    for fill in item.fills:
        if fill.side == opening_side:
            opening_fill = fill
            break
    if opening_fill is None:
        return None
    principal = float(opening_fill.price) * float(opening_fill.qty) + float(opening_fill.fee_total_jpy or opening_fill.fee or 0)
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


def _katakana_to_hiragana(text: str) -> str:
    chars = []
    for ch in text:
        code = ord(ch)
        if 0x30A1 <= code <= 0x30F6:
            chars.append(chr(code - 0x60))
        else:
            chars.append(ch)
    return "".join(chars)


def _normalize_name_for_sort(text: str) -> str:
    # NFKC: 全角英数や記号の揺れを吸収し、JPはひらがな寄せで五十音順に近づける
    normalized = unicodedata.normalize("NFKC", str(text or "").strip()).casefold()
    return _katakana_to_hiragana(normalized)


def _name_sort_key(item: TradeRead):
    base_name = (item.name or "").strip()
    fallback_symbol = (item.symbol or "").strip()
    label = base_name or fallback_symbol
    if not label:
        return None

    market_rank = 0 if item.market == "JP" else 1 if item.market == "US" else 2
    return (market_rank, _normalize_name_for_sort(label), fallback_symbol.casefold())


@router.get("", response_model=TradeListRead)
def list_trades(
    limit: int = Query(default=20, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    q: Optional[str] = None,
    market: Optional[str] = None,
    rating: Optional[str] = None,
    tag: Optional[str] = None,
    status: str = "all",
    pos: str = "all",
    review: str = "all",
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
    claims: dict = Depends(require_invited_auth),
):
    stmt = select(Trade).options(selectinload(Trade.fills))
    scoped_user_id = _scoped_user_id(claims)
    if scoped_user_id is not None:
        stmt = stmt.where(Trade.user_id == scoped_user_id)

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

    trade_models = list(db.scalars(stmt).all())
    import_metadata = _load_import_metadata(db, [int(trade.id) for trade in trade_models])
    trades = [
        _to_trade_read(
            trade,
            is_partial_exit=bool(import_metadata.get(int(trade.id), {}).get("is_partial_exit", False)),
            import_source=import_metadata.get(int(trade.id), {}).get("broker"),
        )
        for trade in trade_models
    ]

    q_lower = (q or "").strip().lower()
    memo_lower = (memo or "").strip().lower()
    tag_values_raw = _parse_csv(tag)
    tag_values = [v.lower() for v in tag_values_raw]
    has_unset_tag = "未設定" in tag_values_raw
    pos_value = pos if pos in {"all", "open", "closed"} else "all"
    review_value = review if review in {"all", "pending", "done"} else "all"
    status_value = status if status in {"all", "open", "pending", "complete"} else "all"
    if status_value == "all":
        # legacy compatibility for old URLs/clients
        if pos_value == "open":
            status_value = "open"
        elif pos_value == "closed" and review_value == "pending":
            status_value = "pending"
        elif pos_value == "closed" and review_value == "done":
            status_value = "complete"
        elif review_value == "pending":
            status_value = "pending"
        elif review_value == "done":
            status_value = "complete"
        elif pos_value == "closed":
            status_value = "closed"
    win_only_enabled = win_only == "1"
    loss_only_enabled = loss_only == "1"

    filtered = []
    for item in trades:
        trade_status = _trade_status(item)
        if status_value == "open" and trade_status != "open":
            continue
        if status_value == "pending" and trade_status != "pending":
            continue
        if status_value == "complete" and trade_status != "complete":
            continue
        if status_value == "closed" and trade_status == "open":
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
        if normalized_sort == "status":
            status_rank = {"complete": 0, "pending": 1, "open": 2}.get(_trade_status(item), 3)
            return (status_rank, item.opened_at or item.created_at)
        if normalized_sort == "buy_date":
            return item.opened_at or item.created_at
        if normalized_sort == "sell_date":
            return item.closed_at or item.created_at
        if normalized_sort == "name":
            return _name_sort_key(item)
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
    pending_review_count = 0

    for item in sorted_items:
        if _trade_status(item) == "pending":
            pending_review_count += 1

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
        pending_review_count=pending_review_count,
    )

    return TradeListRead(items=page_items, total=total, limit=limit, offset=offset, stats=stats)


@router.post("", response_model=TradeRead, status_code=201)
def create_trade(
    payload: TradeCreate,
    db: Session = Depends(get_session),
    claims: dict = Depends(require_invited_auth),
):
    scoped_user_id = _scoped_user_id(claims)
    trade = create_trade_with_fills(db, payload, user_id=scoped_user_id)
    try:
        db.commit()
    except IntegrityError as e:
        db.rollback()
        raise_409_from_integrity(e)

    reloaded = fetch_trade(db, trade.id, user_id=scoped_user_id)
    if reloaded is None:
        raise HTTPException(status_code=404, detail="trade not found")
    import_metadata = _load_import_metadata(db, [int(reloaded.id)])
    return _to_trade_read(
        reloaded,
        is_partial_exit=bool(import_metadata.get(int(reloaded.id), {}).get("is_partial_exit", False)),
        import_source=import_metadata.get(int(reloaded.id), {}).get("broker"),
    )


@router.get("/{trade_id}", response_model=TradeRead)
def get_trade(
    trade_id: int,
    db: Session = Depends(get_session),
    claims: dict = Depends(require_invited_auth),
):
    scoped_user_id = _scoped_user_id(claims)
    trade = fetch_trade(db, trade_id, user_id=scoped_user_id)
    if trade is None:
        raise HTTPException(status_code=404, detail="trade not found")
    import_metadata = _load_import_metadata(db, [int(trade.id)])
    return _to_trade_read(
        trade,
        is_partial_exit=bool(import_metadata.get(int(trade.id), {}).get("is_partial_exit", False)),
        import_source=import_metadata.get(int(trade.id), {}).get("broker"),
    )


@router.patch("/{trade_id}", response_model=TradeRead)
def update_trade(
    trade_id: int,
    payload: TradeUpdate,
    db: Session = Depends(get_session),
    claims: dict = Depends(require_invited_auth),
):
    scoped_user_id = _scoped_user_id(claims)
    trade = fetch_trade(db, trade_id, user_id=scoped_user_id)
    if trade is None:
        raise HTTPException(status_code=404, detail="trade not found")

    patch_data = payload.model_dump(exclude_unset=True)
    review_keys = {"review_done", "reviewed_at"}
    has_review_update = any(k in patch_data for k in review_keys)
    has_regular_update = any(k not in review_keys for k in patch_data.keys())
    if has_review_update and has_regular_update:
        raise HTTPException(status_code=422, detail="review fields must be updated separately")
    if has_review_update and patch_data.get("review_done") is True:
        missing = review_completion_missing_items(trade)
        if missing:
            raise HTTPException(
                status_code=422,
                detail=f"レビュー完了に必要な項目が不足しています: {'、'.join(missing)}",
            )

    update_trade_with_fills(db, trade, payload)

    try:
        db.commit()
    except IntegrityError as e:
        db.rollback()
        raise_409_from_integrity(e)

    reloaded = fetch_trade(db, trade_id, user_id=scoped_user_id)
    if reloaded is None:
        raise HTTPException(status_code=404, detail="trade not found")
    import_metadata = _load_import_metadata(db, [int(reloaded.id)])
    return _to_trade_read(
        reloaded,
        is_partial_exit=bool(import_metadata.get(int(reloaded.id), {}).get("is_partial_exit", False)),
        import_source=import_metadata.get(int(reloaded.id), {}).get("broker"),
    )


@router.delete("/{trade_id}", status_code=204)
def delete_trade(
    trade_id: int,
    db: Session = Depends(get_session),
    claims: dict = Depends(require_invited_auth),
):
    scoped_user_id = _scoped_user_id(claims)
    trade = fetch_trade(db, trade_id, user_id=scoped_user_id)
    if trade is None:
        raise HTTPException(status_code=404, detail="trade not found")

    db.delete(trade)
    db.commit()
    return None
