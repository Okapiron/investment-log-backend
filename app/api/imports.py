from typing import Optional

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.api.deps import get_session, require_invited_auth
from app.core.config import settings
from app.core.rakuten_csv import parse_rakuten_domestic_csv
from app.crud.trades import create_trade_with_fills
from app.db.models import TradeImportRecord
from app.schemas.imports import (
    ImportIssueRead,
    RakutenImportCommitRequest,
    RakutenImportCommitResponse,
    RakutenImportPreviewRequest,
    RakutenImportPreviewResponse,
)
from app.schemas.trade import FillInput, TradeCreate

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


@router.post("/rakuten-jp/preview", response_model=RakutenImportPreviewResponse)
def preview_rakuten_jp_import(
    payload: RakutenImportPreviewRequest,
    db: Session = Depends(get_session),
    claims: dict = Depends(require_invited_auth),
):
    _scoped_user_id(claims)
    preview = parse_rakuten_domestic_csv(payload.content, payload.filename)
    return _mark_existing_signatures(db, preview)


@router.post("/rakuten-jp/commit", response_model=RakutenImportCommitResponse)
def commit_rakuten_jp_import(
    payload: RakutenImportCommitRequest,
    db: Session = Depends(get_session),
    claims: dict = Depends(require_invited_auth),
):
    user_id = _scoped_user_id(claims)
    created_trade_ids: list[int] = []
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
            trade = create_trade_with_fills(
                db,
                TradeCreate(
                    market="JP",
                    symbol=item.symbol,
                    name=item.name,
                    fills=[
                        FillInput(
                            side="buy",
                            date=item.buy.date,
                            price=int(item.buy.price),
                            qty=item.buy.qty,
                            fee=item.buy.fee,
                        ),
                        FillInput(
                            side="sell",
                            date=item.sell.date,
                            price=int(item.sell.price),
                            qty=item.sell.qty,
                            fee=item.sell.fee,
                        ),
                    ],
                ),
                user_id=user_id,
            )
            db.flush()
            db.add(
                TradeImportRecord(
                    broker="rakuten",
                    source_name=payload.filename,
                    source_signature=item.source_signature,
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
        skipped_count=len(skipped),
        error_count=len(errors),
        created_trade_ids=created_trade_ids,
        skipped=skipped,
        errors=errors,
    )
