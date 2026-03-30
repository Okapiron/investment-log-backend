from decimal import Decimal
from typing import Optional

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.api.deps import get_session, require_invited_auth
from app.core.config import settings
from app.core.rakuten_csv import audit_rakuten_tradehistory_against_realized, parse_rakuten_domestic_csv
from app.crud.trades import create_trade_with_fills, fetch_trade, update_trade_with_fills
from app.db.models import TradeImportRecord
from app.schemas.imports import (
    ImportIssueRead,
    RakutenImportAuditRequest,
    RakutenImportAuditResponse,
    RakutenImportCommitRequest,
    RakutenImportCommitResponse,
    RakutenImportPreviewRequest,
    RakutenImportPreviewResponse,
)
from app.schemas.trade import FillInput, TradeCreate, TradeUpdate

router = APIRouter(prefix="/imports", tags=["imports"])


def _scoped_user_id(claims: dict) -> Optional[str]:
    if not settings.auth_enabled:
        return None
    sub = str((claims or {}).get("sub") or "").strip()
    return sub or None


def _mark_existing_signatures(db: Session, preview: RakutenImportPreviewResponse) -> RakutenImportPreviewResponse:
    signatures = [item.source_signature for item in preview.candidates]
    if not signatures:
        return preview
    existing = set(
        db.scalars(select(TradeImportRecord.source_signature).where(TradeImportRecord.source_signature.in_(signatures))).all()
    )
    for item in preview.candidates:
        item.already_imported = item.source_signature in existing
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
        fills=fills,
        notes_sell=None,
        notes_review=None,
        rating=None,
        review_done=False,
        reviewed_at=None,
    )


def _find_open_import_record(db: Session, item) -> Optional[TradeImportRecord]:
    if item.sell is None:
        return None
    return db.scalar(
        select(TradeImportRecord)
        .where(
            TradeImportRecord.source_position_key == item.source_position_key,
            TradeImportRecord.import_state == "open_remaining",
        )
        .order_by(TradeImportRecord.id.asc())
    )


@router.post("/rakuten-jp/preview", response_model=RakutenImportPreviewResponse)
def preview_rakuten_jp_import(
    payload: RakutenImportPreviewRequest,
    db: Session = Depends(get_session),
    claims: dict = Depends(require_invited_auth),
):
    _scoped_user_id(claims)
    preview = parse_rakuten_domestic_csv(payload.content, payload.filename)
    return _mark_existing_signatures(db, preview)


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


@router.post("/rakuten-jp/commit", response_model=RakutenImportCommitResponse)
def commit_rakuten_jp_import(
    payload: RakutenImportCommitRequest,
    db: Session = Depends(get_session),
    claims: dict = Depends(require_invited_auth),
):
    user_id = _scoped_user_id(claims)
    created_trade_ids: list[int] = []
    updated_trade_ids: list[int] = []
    skipped: list[ImportIssueRead] = []
    errors: list[ImportIssueRead] = []

    for item in payload.items:
        exists = db.scalar(select(TradeImportRecord).where(TradeImportRecord.source_signature == item.source_signature))
        if exists is not None:
            skipped.append(
                ImportIssueRead(
                    line=item.source_lines[0] if item.source_lines else None,
                    code="duplicate_import",
                    message=f"{item.symbol} は既に取り込み済みのためスキップしました。",
                )
            )
            continue

        try:
            existing_open_record = _find_open_import_record(db, item)
            if existing_open_record is not None and existing_open_record.trade_id is not None:
                existing_trade = fetch_trade(db, int(existing_open_record.trade_id), user_id=user_id)
                if existing_trade is not None:
                    update_trade_with_fills(db, existing_trade, _build_trade_update(item))
                    existing_open_record.source_name = payload.filename
                    existing_open_record.source_signature = item.source_signature
                    existing_open_record.source_position_key = item.source_position_key
                    existing_open_record.source_lot_sequence = item.source_lot_sequence
                    existing_open_record.import_state = "closed_round_trip"
                    existing_open_record.is_partial_exit = bool(item.is_partial_exit)
                    db.commit()
                    updated_trade_ids.append(int(existing_trade.id))
                    continue

            trade = create_trade_with_fills(db, _build_trade_create(item), user_id=user_id)
            db.flush()
            db.add(
                TradeImportRecord(
                    broker="rakuten",
                    source_name=payload.filename,
                    source_signature=item.source_signature,
                    source_position_key=item.source_position_key,
                    source_lot_sequence=item.source_lot_sequence,
                    import_state="closed_round_trip" if item.sell is not None else "open_remaining",
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

    return RakutenImportCommitResponse(
        broker="rakuten",
        created_count=len(created_trade_ids),
        updated_count=len(updated_trade_ids),
        skipped_count=len(skipped),
        error_count=len(errors),
        created_trade_ids=created_trade_ids,
        updated_trade_ids=updated_trade_ids,
        skipped=skipped,
        errors=errors,
    )
