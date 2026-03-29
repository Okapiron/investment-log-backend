from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.api.deps import get_session, require_invited_auth
from app.core.analysis import build_analysis_summary
from app.core.config import settings
from app.db.models import Trade
from app.schemas.analysis import AnalysisSummaryRead

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
    return build_analysis_summary(trades, scoped_user_id)
