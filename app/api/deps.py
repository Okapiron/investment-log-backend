from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.jwt_utils import decode_and_verify_hs256
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
