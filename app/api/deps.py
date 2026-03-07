from datetime import datetime, timezone

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.invites import hash_invite_code, normalize_invite_code
from app.core.jwt_utils import decode_and_verify_hs256
from app.db.models import InviteCode
from app.db.session import get_db


def get_session(db: Session = Depends(get_db)) -> Session:
    return db


bearer_scheme = HTTPBearer(auto_error=False)


def require_auth(credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme)) -> dict:
    if not settings.auth_enabled:
        return {"sub": "dev-local-user"}

    secret = str(settings.supabase_jwt_secret or "").strip()
    if not secret:
        raise HTTPException(status_code=503, detail="auth is enabled but SUPABASE_JWT_SECRET is not configured")

    if credentials is None or not credentials.credentials:
        raise HTTPException(status_code=401, detail="authentication required")

    try:
        claims = decode_and_verify_hs256(credentials.credentials, secret)
    except ValueError as e:
        raise HTTPException(status_code=401, detail=f"invalid auth token: {e}")

    sub = str(claims.get("sub") or "").strip()
    if not sub:
        raise HTTPException(status_code=401, detail="invalid auth token: sub is missing")

    return claims


def _extract_invite_code_from_claims(claims: dict) -> str:
    candidates = []
    if isinstance(claims, dict):
        candidates.append(claims.get("invite_code"))
        user_meta = claims.get("user_metadata")
        if isinstance(user_meta, dict):
            candidates.append(user_meta.get("invite_code"))
        app_meta = claims.get("app_metadata")
        if isinstance(app_meta, dict):
            candidates.append(app_meta.get("invite_code"))
    for value in candidates:
        code = normalize_invite_code(value or "")
        if code:
            return code
    return ""


def require_invited_auth(
    claims: dict = Depends(require_auth),
    db: Session = Depends(get_db),
) -> dict:
    if not settings.auth_enabled or not settings.invite_code_required:
        return claims

    user_id = str((claims or {}).get("sub") or "").strip()
    if not user_id:
        raise HTTPException(status_code=401, detail="invalid auth token: sub is missing")

    existing = db.scalar(
        select(InviteCode).where(
            InviteCode.used_by_user_id == user_id,
            InviteCode.used_count > 0,
        )
    )
    if existing is not None:
        return claims

    raw_code = _extract_invite_code_from_claims(claims)
    if not raw_code:
        raise HTTPException(status_code=403, detail="招待コードが必要です")

    now = datetime.now(timezone.utc)
    code_hash = hash_invite_code(raw_code)
    invite = db.scalar(select(InviteCode).where(InviteCode.code_hash == code_hash))
    if invite is None:
        raise HTTPException(status_code=403, detail="招待コードが無効です")
    expires_at = invite.expires_at
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    if expires_at <= now:
        raise HTTPException(status_code=403, detail="招待コードの有効期限が切れています")
    if int(invite.used_count) >= int(invite.max_uses):
        raise HTTPException(status_code=403, detail="招待コードは使用済みです")
    if invite.used_by_user_id and invite.used_by_user_id != user_id:
        raise HTTPException(status_code=403, detail="招待コードは使用済みです")

    invite.used_count = int(invite.used_count) + 1
    invite.used_by_user_id = user_id
    db.add(invite)
    db.commit()
    return claims
