from decimal import Decimal
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import and_, select
from sqlalchemy.orm import Session, aliased

from app.api.deps import get_session, require_invited_auth
from app.core.config import settings
from app.core.rakuten_csv import audit_rakuten_tradehistory_against_realized, parse_rakuten_domestic_csv
from app.core.sbi_csv import audit_sbi_tradehistory_against_realized, parse_sbi_domestic_csv, parse_sbi_realized_only_csv
from app.crud.trades import create_trade_with_fills, fetch_trade, update_trade_with_fills
from app.db.models import Fill, ImportSession, Trade, TradeImportRecord
from app.schemas.imports import (
    BrokerImportAuditRequest,
    ImportSessionRead,
    ImportIssueRead,
    RakutenImportAuditRequest,
    RakutenImportAuditResponse,
    RakutenImportCommitRequest,
    RakutenImportCommitResponse,
    RakutenImportPreviewRequest,
    RakutenImportPreviewResponse,
    SbiRealizedImportCandidateRead,
    SbiRealizedImportCommitRequest,
    SbiRealizedImportCommitResponse,
    SbiRealizedImportPreviewRequest,
    SbiRealizedImportPreviewResponse,
)
from app.schemas.trade import FillInput, TradeCreate, TradeUpdate

router = APIRouter(prefix="/imports", tags=["imports"])

SUPPORTED_BROKERS = {"rakuten", "sbi"}


def _enabled_brokers() -> set[str]:
    enabled = {"rakuten"}
    if settings.import_sbi_enabled:
        enabled.add("sbi")
    return enabled


def _ensure_broker_enabled(broker: str) -> None:
    if broker not in _enabled_brokers():
        raise HTTPException(status_code=404, detail="unsupported broker")


def _scoped_user_id(claims: dict) -> Optional[str]:
    if not settings.auth_enabled:
        return None
    sub = str((claims or {}).get("sub") or "").strip()
    return sub or None


def _candidate_open_fill(item):
    return item.buy if item.position_side == "long" else item.sell


def _candidate_close_fill(item):
    return item.sell if item.position_side == "long" else item.buy


def _candidate_qty(item) -> int:
    open_fill = _candidate_open_fill(item)
    close_fill = _candidate_close_fill(item)
    return int((open_fill.qty if open_fill is not None else close_fill.qty) if (open_fill is not None or close_fill is not None) else 0)


def _candidate_open_date(item) -> str:
    open_fill = _candidate_open_fill(item)
    return open_fill.date if open_fill is not None else ""


def _candidate_close_date(item) -> str:
    close_fill = _candidate_close_fill(item)
    return close_fill.date if close_fill is not None else ""


def _candidate_import_state(item) -> str:
    return "closed_round_trip" if _candidate_close_fill(item) is not None else "open_remaining"


def _candidate_fill_price(fill) -> Decimal:
    return Decimal(str(getattr(fill, "price", 0) or 0))


def _candidate_fill_fee_total(fill) -> int:
    fee_total = getattr(fill, "fee_total_jpy", None)
    if fee_total is not None:
        return int(fee_total)
    return int(getattr(fill, "fee", 0) or 0)


def _money_decimal(value) -> Decimal:
    return Decimal(str(value or 0)).quantize(Decimal("0.01"))


def _candidate_fallback_key(item):
    open_fill = _candidate_open_fill(item)
    if open_fill is None:
        return None
    close_fill = _candidate_close_fill(item)
    return (
        item.market,
        item.symbol,
        item.position_side,
        open_fill.date,
        int(open_fill.qty),
        str(_candidate_fill_price(open_fill)),
        int(_candidate_fill_fee_total(open_fill)),
        close_fill.date if close_fill is not None else "",
        int(close_fill.qty) if close_fill is not None else 0,
        str(_candidate_fill_price(close_fill)) if close_fill is not None else "",
        int(_candidate_fill_fee_total(close_fill)) if close_fill is not None else 0,
    )


def _fallback_collision_counts(items) -> dict[tuple, int]:
    counts: dict[tuple, int] = {}
    for item in items:
        key = _candidate_fallback_key(item)
        if key is None:
            continue
        counts[key] = counts.get(key, 0) + 1
    return counts


def _record_query(stmt, *, user_id: Optional[str], join_trade: bool = False):
    if user_id is None:
        return stmt
    if not join_trade:
        stmt = stmt.join(Trade, Trade.id == TradeImportRecord.trade_id)
    return stmt.where(Trade.user_id == user_id)


def _find_existing_import_record(
    db: Session,
    item,
    *,
    user_id: Optional[str],
    allow_fallback: bool = True,
) -> Optional[TradeImportRecord]:
    exact = _record_query(
        select(TradeImportRecord).where(TradeImportRecord.source_signature == item.source_signature),
        user_id=user_id,
    ).order_by(TradeImportRecord.id.asc())
    record = db.scalar(exact)
    if record is not None:
        return record

    lineage = _record_query(
        select(TradeImportRecord).where(
            TradeImportRecord.source_position_key == item.source_position_key,
            TradeImportRecord.source_lot_sequence == item.source_lot_sequence,
        ),
        user_id=user_id,
    ).order_by(TradeImportRecord.id.asc())
    record = db.scalar(lineage)
    if record is not None:
        return record

    if not allow_fallback:
        return None

    open_fill = _candidate_open_fill(item)
    close_fill = _candidate_close_fill(item)
    if open_fill is None:
        return None

    open_fill_alias = aliased(Fill)
    open_side = "buy" if item.position_side == "long" else "sell"
    fallback = (
        select(TradeImportRecord)
        .join(Trade, Trade.id == TradeImportRecord.trade_id)
        .join(
            open_fill_alias,
            and_(open_fill_alias.trade_id == Trade.id, open_fill_alias.side == open_side),
        )
        .where(
            Trade.market == item.market,
            Trade.symbol == item.symbol,
            Trade.position_side == item.position_side,
            Trade.opened_at == _candidate_open_date(item),
            Trade.closed_at == _candidate_close_date(item),
            open_fill_alias.date == open_fill.date,
            open_fill_alias.qty == open_fill.qty,
            open_fill_alias.price == _candidate_fill_price(open_fill),
            open_fill_alias.fee_total_jpy == _candidate_fill_fee_total(open_fill),
        )
    )

    if close_fill is not None:
        close_fill_alias = aliased(Fill)
        close_side = "sell" if item.position_side == "long" else "buy"
        fallback = fallback.join(
            close_fill_alias,
            and_(close_fill_alias.trade_id == Trade.id, close_fill_alias.side == close_side),
        ).where(
            close_fill_alias.date == close_fill.date,
            close_fill_alias.qty == close_fill.qty,
            close_fill_alias.price == _candidate_fill_price(close_fill),
            close_fill_alias.fee_total_jpy == _candidate_fill_fee_total(close_fill),
        )

    fallback = fallback.order_by(TradeImportRecord.id.asc()).limit(2)
    if user_id is not None:
        fallback = fallback.where(Trade.user_id == user_id)
    matches = list(db.scalars(fallback).all())
    if len(matches) != 1:
        return None
    return matches[0]


def _find_detailed_trade_for_realized_candidate(
    db: Session,
    item: SbiRealizedImportCandidateRead,
    *,
    user_id: Optional[str],
) -> Optional[Trade]:
    buy_fill = aliased(Fill)
    sell_fill = aliased(Fill)
    stmt = (
        select(Trade)
        .join(buy_fill, and_(buy_fill.trade_id == Trade.id, buy_fill.side == "buy"))
        .join(sell_fill, and_(sell_fill.trade_id == Trade.id, sell_fill.side == "sell"))
        .where(
            Trade.market == "JP",
            Trade.symbol == item.symbol,
            Trade.position_side == "long",
            Trade.data_quality == "full",
            Trade.closed_at == item.close_date,
            buy_fill.qty == item.qty,
            buy_fill.price == _money_decimal(item.avg_cost),
            sell_fill.qty == item.qty,
            sell_fill.price == _money_decimal(item.sell_price),
        )
        .order_by(Trade.id.asc())
        .limit(1)
    )
    if user_id is not None:
        stmt = stmt.where(Trade.user_id == user_id)
    return db.scalar(stmt)


def _find_realized_only_import_record_for_detailed_item(
    db: Session,
    item,
    *,
    user_id: Optional[str],
) -> Optional[TradeImportRecord]:
    if item.position_side != "long" or item.market != "JP":
        return None
    open_fill = _candidate_open_fill(item)
    close_fill = _candidate_close_fill(item)
    if open_fill is None or close_fill is None:
        return None

    buy_fill = aliased(Fill)
    sell_fill = aliased(Fill)
    stmt = (
        select(TradeImportRecord)
        .join(Trade, Trade.id == TradeImportRecord.trade_id)
        .join(buy_fill, and_(buy_fill.trade_id == Trade.id, buy_fill.side == "buy"))
        .join(sell_fill, and_(sell_fill.trade_id == Trade.id, sell_fill.side == "sell"))
        .where(
            TradeImportRecord.broker == "sbi",
            TradeImportRecord.import_state == "realized_only",
            Trade.market == "JP",
            Trade.symbol == item.symbol,
            Trade.position_side == "long",
            Trade.data_quality == "realized_only",
            Trade.closed_at == close_fill.date,
            buy_fill.qty == open_fill.qty,
            buy_fill.price == _candidate_fill_price(open_fill),
            sell_fill.qty == close_fill.qty,
            sell_fill.price == _candidate_fill_price(close_fill),
        )
        .order_by(TradeImportRecord.id.asc())
        .limit(1)
    )
    if user_id is not None:
        stmt = stmt.where(Trade.user_id == user_id)
    return db.scalar(stmt)


def _mark_existing_candidates(db: Session, preview: RakutenImportPreviewResponse, *, user_id: Optional[str]) -> RakutenImportPreviewResponse:
    fallback_counts = _fallback_collision_counts(preview.candidates)
    for item in preview.candidates:
        fallback_key = _candidate_fallback_key(item)
        allow_fallback = fallback_key is None or fallback_counts.get(fallback_key, 0) <= 1
        item.already_imported = _find_existing_import_record(db, item, user_id=user_id, allow_fallback=allow_fallback) is not None
    return preview


def _mark_sbi_realized_candidates(
    db: Session,
    preview: SbiRealizedImportPreviewResponse,
    *,
    user_id: Optional[str],
) -> SbiRealizedImportPreviewResponse:
    create_count = 0
    update_count = 0
    detailed_skip_count = 0
    for item in preview.candidates:
        existing_record = _record_query(
            select(TradeImportRecord).where(TradeImportRecord.source_signature == item.source_signature),
            user_id=user_id,
        ).order_by(TradeImportRecord.id.asc())
        item.already_imported = db.scalar(existing_record) is not None
        item.detailed_trade_exists = _find_detailed_trade_for_realized_candidate(db, item, user_id=user_id) is not None
        if item.detailed_trade_exists:
            detailed_skip_count += 1
        elif item.already_imported:
            update_count += 1
        else:
            create_count += 1
    preview.create_count = create_count
    preview.update_count = update_count
    preview.detailed_skip_count = detailed_skip_count
    return preview


def _build_trade_create(item) -> TradeCreate:
    fills = []
    if item.buy is not None:
        fills.append(
            FillInput(
                side="buy",
                date=item.buy.date,
                price=Decimal(str(item.buy.price)),
                qty=item.buy.qty,
                fee=item.buy.fee,
                fee_commission_jpy=item.buy.fee_commission_jpy,
                fee_tax_jpy=item.buy.fee_tax_jpy,
                fee_other_jpy=item.buy.fee_other_jpy,
                fee_total_jpy=item.buy.fee_total_jpy,
            )
        )
    if item.sell is not None:
        fills.append(
            FillInput(
                side="sell",
                date=item.sell.date,
                price=Decimal(str(item.sell.price)),
                qty=item.sell.qty,
                fee=item.sell.fee,
                fee_commission_jpy=item.sell.fee_commission_jpy,
                fee_tax_jpy=item.sell.fee_tax_jpy,
                fee_other_jpy=item.sell.fee_other_jpy,
                fee_total_jpy=item.sell.fee_total_jpy,
            )
        )
    return TradeCreate(
        market="JP",
        position_side=item.position_side,
        data_quality="full",
        broker_profit_jpy=None,
        symbol=item.symbol,
        name=item.name,
        review_done=False,
        fills=fills,
    )


def _build_trade_update(item) -> TradeUpdate:
    fills = []
    if item.buy is not None:
        fills.append(
            FillInput(
                side="buy",
                date=item.buy.date,
                price=Decimal(str(item.buy.price)),
                qty=item.buy.qty,
                fee=item.buy.fee,
                fee_commission_jpy=item.buy.fee_commission_jpy,
                fee_tax_jpy=item.buy.fee_tax_jpy,
                fee_other_jpy=item.buy.fee_other_jpy,
                fee_total_jpy=item.buy.fee_total_jpy,
            )
        )
    if item.sell is not None:
        fills.append(
            FillInput(
                side="sell",
                date=item.sell.date,
                price=Decimal(str(item.sell.price)),
                qty=item.sell.qty,
                fee=item.sell.fee,
                fee_commission_jpy=item.sell.fee_commission_jpy,
                fee_tax_jpy=item.sell.fee_tax_jpy,
                fee_other_jpy=item.sell.fee_other_jpy,
                fee_total_jpy=item.sell.fee_total_jpy,
            )
        )
    return TradeUpdate(
        position_side=item.position_side,
        data_quality="full",
        broker_profit_jpy=None,
        fills=fills,
    )


def _build_sbi_realized_trade_create(item: SbiRealizedImportCandidateRead) -> TradeCreate:
    return TradeCreate(
        market="JP",
        position_side="long",
        data_quality="realized_only",
        broker_profit_jpy=Decimal(str(item.realized_profit_jpy)),
        symbol=item.symbol,
        name=item.name,
        review_done=False,
        fills=[
            FillInput(side="buy", date=item.close_date, price=Decimal(str(item.avg_cost)), qty=item.qty, fee=0),
            FillInput(side="sell", date=item.close_date, price=Decimal(str(item.sell_price)), qty=item.qty, fee=0),
        ],
    )


def _build_sbi_realized_trade_update(item: SbiRealizedImportCandidateRead) -> TradeUpdate:
    return TradeUpdate(
        position_side="long",
        data_quality="realized_only",
        broker_profit_jpy=Decimal(str(item.realized_profit_jpy)),
        fills=[
            FillInput(side="buy", date=item.close_date, price=Decimal(str(item.avg_cost)), qty=item.qty, fee=0),
            FillInput(side="sell", date=item.close_date, price=Decimal(str(item.sell_price)), qty=item.qty, fee=0),
        ],
    )


def _clear_realized_only_fee_breakdown(trade: Trade) -> None:
    for fill in trade.fills:
        fill.fee = 0
        fill.fee_commission_jpy = None
        fill.fee_tax_jpy = None
        fill.fee_other_jpy = None
        fill.fee_total_jpy = None


def _sync_import_record(record: TradeImportRecord, *, broker: str, filename: Optional[str], item, trade_id: int) -> None:
    record.broker = broker
    record.source_name = filename
    record.source_signature = item.source_signature
    record.source_position_key = item.source_position_key
    record.source_lot_sequence = item.source_lot_sequence
    record.import_state = _candidate_import_state(item)
    record.is_partial_exit = bool(item.is_partial_exit)
    record.trade_id = trade_id


def _parse_preview_for_broker(broker: str, content: str, filename: Optional[str]) -> RakutenImportPreviewResponse:
    if broker == "rakuten":
        return parse_rakuten_domestic_csv(content, filename)
    if broker == "sbi":
        return parse_sbi_domestic_csv(content, filename)
    raise ValueError(f"unsupported broker: {broker}")


def _audit_for_broker(
    broker: str,
    tradehistory_content: str,
    *,
    tradehistory_filename: Optional[str],
    realized_content: str,
) -> RakutenImportAuditResponse:
    if broker == "rakuten":
        return audit_rakuten_tradehistory_against_realized(
            tradehistory_content,
            tradehistory_filename=tradehistory_filename,
            realized_content=realized_content,
        )
    if broker == "sbi":
        return audit_sbi_tradehistory_against_realized(
            tradehistory_content,
            tradehistory_filename=tradehistory_filename,
            realized_content=realized_content,
        )
    raise ValueError(f"unsupported broker: {broker}")


def _validate_broker(broker: str) -> str:
    normalized = str(broker or "").strip().lower()
    if normalized not in SUPPORTED_BROKERS:
        raise HTTPException(status_code=404, detail="unsupported broker")
    _ensure_broker_enabled(normalized)
    return normalized


def _session_read(session: ImportSession) -> ImportSessionRead:
    return ImportSessionRead(
        id=int(session.id),
        broker=session.broker,
        source_name=session.source_name,
        realized_source_name=session.realized_source_name,
        imported_at=session.imported_at,
        created_count=int(session.created_count or 0),
        updated_count=int(session.updated_count or 0),
        skipped_count=int(session.skipped_count or 0),
        error_count=int(session.error_count or 0),
        audit_gap_jpy=float(session.audit_gap_jpy) if session.audit_gap_jpy is not None else None,
    )


@router.post("/rakuten-jp/preview", response_model=RakutenImportPreviewResponse)
def preview_rakuten_jp_import(
    payload: RakutenImportPreviewRequest,
    db: Session = Depends(get_session),
    claims: dict = Depends(require_invited_auth),
):
    user_id = _scoped_user_id(claims)
    preview = parse_rakuten_domestic_csv(payload.content, payload.filename)
    return _mark_existing_candidates(db, preview, user_id=user_id)


@router.post("/{broker}/preview", response_model=RakutenImportPreviewResponse)
def preview_broker_import(
    broker: str,
    payload: RakutenImportPreviewRequest,
    db: Session = Depends(get_session),
    claims: dict = Depends(require_invited_auth),
):
    user_id = _scoped_user_id(claims)
    normalized = _validate_broker(broker)
    preview = _parse_preview_for_broker(normalized, payload.content, payload.filename)
    return _mark_existing_candidates(db, preview, user_id=user_id)


@router.post("/sbi/realized/preview", response_model=SbiRealizedImportPreviewResponse)
def preview_sbi_realized_import(
    payload: SbiRealizedImportPreviewRequest,
    db: Session = Depends(get_session),
    claims: dict = Depends(require_invited_auth),
):
    _ensure_broker_enabled("sbi")
    user_id = _scoped_user_id(claims)
    preview = parse_sbi_realized_only_csv(payload.content, payload.filename)
    return _mark_sbi_realized_candidates(db, preview, user_id=user_id)


@router.post("/rakuten-jp/audit", response_model=RakutenImportAuditResponse)
def audit_rakuten_jp_import(
    payload: RakutenImportAuditRequest,
    claims: dict = Depends(require_invited_auth),
):
    _scoped_user_id(claims)
    return audit_rakuten_tradehistory_against_realized(
        payload.tradehistory_content,
        tradehistory_filename=payload.tradehistory_filename,
        realized_content=payload.realized_content,
    )


@router.post("/{broker}/audit", response_model=RakutenImportAuditResponse)
def audit_broker_import(
    broker: str,
    payload: BrokerImportAuditRequest,
    claims: dict = Depends(require_invited_auth),
):
    _scoped_user_id(claims)
    normalized = _validate_broker(broker)
    return _audit_for_broker(
        normalized,
        payload.tradehistory_content,
        tradehistory_filename=payload.tradehistory_filename,
        realized_content=payload.realized_content,
    )


@router.post("/rakuten-jp/commit", response_model=RakutenImportCommitResponse)
def commit_rakuten_jp_import(
    payload: RakutenImportCommitRequest,
    db: Session = Depends(get_session),
    claims: dict = Depends(require_invited_auth),
):
    payload.broker = "rakuten"
    return _commit_broker_import(payload, db=db, claims=claims)


@router.post("/{broker}/commit", response_model=RakutenImportCommitResponse)
def commit_broker_import(
    broker: str,
    payload: RakutenImportCommitRequest,
    db: Session = Depends(get_session),
    claims: dict = Depends(require_invited_auth),
):
    payload.broker = _validate_broker(broker)
    return _commit_broker_import(payload, db=db, claims=claims)


@router.post("/sbi/realized/commit", response_model=SbiRealizedImportCommitResponse)
def commit_sbi_realized_import(
    payload: SbiRealizedImportCommitRequest,
    db: Session = Depends(get_session),
    claims: dict = Depends(require_invited_auth),
):
    _ensure_broker_enabled("sbi")
    user_id = _scoped_user_id(claims)
    created_trade_ids: list[int] = []
    updated_trade_ids: list[int] = []
    skipped: list[ImportIssueRead] = []
    errors: list[ImportIssueRead] = []

    for item in payload.items:
        try:
            if _find_detailed_trade_for_realized_candidate(db, item, user_id=user_id) is not None:
                skipped.append(
                    ImportIssueRead(
                        line=item.source_lines[0] if item.source_lines else None,
                        code="detailed_trade_exists",
                        message=f"{item.symbol} は詳細Tradeが既にあるため、補完Tradeは作成しません。",
                    )
                )
                continue

            existing_stmt = _record_query(
                select(TradeImportRecord).where(TradeImportRecord.source_signature == item.source_signature),
                user_id=user_id,
            ).order_by(TradeImportRecord.id.asc())
            existing_record = db.scalar(existing_stmt)
            if existing_record is not None and existing_record.trade_id is not None:
                existing_trade = fetch_trade(db, int(existing_record.trade_id), user_id=user_id)
                if existing_trade is not None:
                    update_trade_with_fills(db, existing_trade, _build_sbi_realized_trade_update(item))
                    _clear_realized_only_fee_breakdown(existing_trade)
                    existing_record.broker = "sbi"
                    existing_record.source_name = payload.filename
                    existing_record.source_signature = item.source_signature
                    existing_record.source_position_key = (
                        f"sbi-realized|{item.symbol}|{item.close_date}|{item.qty}|{item.sell_price}|{item.avg_cost}"
                    )
                    existing_record.source_lot_sequence = 1
                    existing_record.import_state = "realized_only"
                    existing_record.is_partial_exit = False
                    existing_record.trade_id = int(existing_trade.id)
                    db.commit()
                    updated_trade_ids.append(int(existing_trade.id))
                    continue

            trade = create_trade_with_fills(db, _build_sbi_realized_trade_create(item), user_id=user_id)
            trade.data_quality = "realized_only"
            trade.broker_profit_jpy = Decimal(str(item.realized_profit_jpy))
            db.flush()
            db.refresh(trade)
            _clear_realized_only_fee_breakdown(trade)
            db.add(
                TradeImportRecord(
                    broker="sbi",
                    source_name=payload.filename,
                    source_signature=item.source_signature,
                    source_position_key=f"sbi-realized|{item.symbol}|{item.close_date}|{item.qty}|{item.sell_price}|{item.avg_cost}",
                    source_lot_sequence=1,
                    import_state="realized_only",
                    is_partial_exit=False,
                    trade_id=trade.id,
                )
            )
            db.commit()
            created_trade_ids.append(int(trade.id))
        except Exception as exc:
            db.rollback()
            errors.append(
                ImportIssueRead(
                    line=item.source_lines[0] if item.source_lines else None,
                    code="commit_failed",
                    message=f"{item.symbol} の補完取込に失敗しました: {exc}",
                )
            )

    session = ImportSession(
        user_id=user_id,
        broker="sbi",
        source_name=payload.filename,
        realized_source_name=payload.filename,
        created_count=len(created_trade_ids),
        updated_count=len(updated_trade_ids),
        skipped_count=len(skipped),
        error_count=len(errors),
        audit_gap_jpy=None,
    )
    db.add(session)
    db.commit()

    return SbiRealizedImportCommitResponse(
        created_count=len(created_trade_ids),
        updated_count=len(updated_trade_ids),
        skipped_count=len(skipped),
        error_count=len(errors),
        created_trade_ids=created_trade_ids,
        updated_trade_ids=updated_trade_ids,
        skipped=skipped,
        errors=errors,
    )


def _commit_broker_import(
    payload: RakutenImportCommitRequest,
    *,
    db: Session,
    claims: dict,
) -> RakutenImportCommitResponse:
    user_id = _scoped_user_id(claims)
    broker = _validate_broker(payload.broker)
    fallback_counts = _fallback_collision_counts(payload.items)
    created_trade_ids: list[int] = []
    updated_trade_ids: list[int] = []
    skipped: list[ImportIssueRead] = []
    errors: list[ImportIssueRead] = []

    for item in payload.items:
        try:
            fallback_key = _candidate_fallback_key(item)
            allow_fallback = fallback_key is None or fallback_counts.get(fallback_key, 0) <= 1
            existing_record = _find_existing_import_record(db, item, user_id=user_id, allow_fallback=allow_fallback)
            if existing_record is None and broker == "sbi":
                existing_record = _find_realized_only_import_record_for_detailed_item(db, item, user_id=user_id)
            if existing_record is not None and existing_record.trade_id is not None:
                existing_trade = fetch_trade(db, int(existing_record.trade_id), user_id=user_id)
                if existing_trade is not None:
                    update_trade_with_fills(db, existing_trade, _build_trade_update(item))
                    _sync_import_record(existing_record, broker=broker, filename=payload.filename, item=item, trade_id=int(existing_trade.id))
                    db.commit()
                    updated_trade_ids.append(int(existing_trade.id))
                    continue

            trade = create_trade_with_fills(db, _build_trade_create(item), user_id=user_id)
            db.flush()
            if existing_record is not None:
                _sync_import_record(existing_record, broker=broker, filename=payload.filename, item=item, trade_id=int(trade.id))
            else:
                db.add(
                    TradeImportRecord(
                        broker=broker,
                        source_name=payload.filename,
                        source_signature=item.source_signature,
                        source_position_key=item.source_position_key,
                        source_lot_sequence=item.source_lot_sequence,
                        import_state=_candidate_import_state(item),
                        is_partial_exit=bool(item.is_partial_exit),
                        trade_id=trade.id,
                    )
                )
            db.commit()
            created_trade_ids.append(int(trade.id))
        except Exception as exc:
            db.rollback()
            errors.append(
                ImportIssueRead(
                    line=item.source_lines[0] if item.source_lines else None,
                    code="commit_failed",
                    message=f"{item.symbol} の取込に失敗しました: {exc}",
                )
            )
            continue

    session = ImportSession(
        user_id=user_id,
        broker=broker,
        source_name=payload.filename,
        realized_source_name=payload.realized_filename,
        created_count=len(created_trade_ids),
        updated_count=len(updated_trade_ids),
        skipped_count=len(skipped),
        error_count=len(errors),
        audit_gap_jpy=Decimal(str(payload.audit_gap_jpy)) if payload.audit_gap_jpy is not None else None,
    )
    db.add(session)
    db.commit()

    return RakutenImportCommitResponse(
        broker=broker,
        created_count=len(created_trade_ids),
        updated_count=len(updated_trade_ids),
        skipped_count=len(skipped),
        error_count=len(errors),
        created_trade_ids=created_trade_ids,
        updated_trade_ids=updated_trade_ids,
        skipped=skipped,
        errors=errors,
    )


@router.get("/sessions/latest", response_model=list[ImportSessionRead])
def latest_import_sessions(
    db: Session = Depends(get_session),
    claims: dict = Depends(require_invited_auth),
):
    user_id = _scoped_user_id(claims)
    rows: list[ImportSessionRead] = []
    for broker in sorted(_enabled_brokers()):
        stmt = select(ImportSession).where(ImportSession.broker == broker)
        if user_id is not None:
            stmt = stmt.where(ImportSession.user_id == user_id)
        session = db.scalar(stmt.order_by(ImportSession.imported_at.desc(), ImportSession.id.desc()).limit(1))
        if session is not None:
            rows.append(_session_read(session))
    return rows
