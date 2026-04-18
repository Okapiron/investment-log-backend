from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.api.deps import get_session, require_invited_auth
from app.core.analysis import build_analysis_summary
from app.core.config import settings
from app.db.models import ImportSession, Trade
from app.schemas.analysis import AnalysisLatestImportRead, AnalysisSummaryRead

router = APIRouter(prefix="/analysis", tags=["analysis"])


def _scoped_user_id(claims: dict) -> Optional[str]:
    if not settings.auth_enabled:
        return None
    sub = str((claims or {}).get("sub") or "").strip()
    if not sub:
        raise HTTPException(status_code=401, detail="invalid auth token: sub is missing")
    return sub


@router.get("/summary", response_model=AnalysisSummaryRead)
def get_analysis_summary(
    db: Session = Depends(get_session),
    claims: dict = Depends(require_invited_auth),
):
    scoped_user_id = _scoped_user_id(claims)
    stmt = select(Trade).options(selectinload(Trade.fills))
    if scoped_user_id is not None:
        stmt = stmt.where(Trade.user_id == scoped_user_id)
    trades = list(db.scalars(stmt).all())
    session_stmt = select(ImportSession)
    if scoped_user_id is not None:
        session_stmt = session_stmt.where(ImportSession.user_id == scoped_user_id)
    latest_session = db.scalar(session_stmt.order_by(ImportSession.imported_at.desc(), ImportSession.id.desc()).limit(1))
    latest_import = None
    if latest_session is not None:
        latest_import = AnalysisLatestImportRead(
            broker=latest_session.broker,
            source_name=latest_session.source_name,
            imported_at=latest_session.imported_at,
            created_count=int(latest_session.created_count or 0),
            updated_count=int(latest_session.updated_count or 0),
            skipped_count=int(latest_session.skipped_count or 0),
            error_count=int(latest_session.error_count or 0),
            audit_gap_jpy=float(latest_session.audit_gap_jpy) if latest_session.audit_gap_jpy is not None else None,
        )
    return build_analysis_summary(trades, scoped_user_id, latest_import)
