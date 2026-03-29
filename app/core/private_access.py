import hmac

from fastapi import HTTPException, Request

from app.core.config import settings

PRIVATE_ACCESS_HEADER = "x-tradetrace-secret"


def private_mode_active() -> bool:
    return bool(settings.private_mode_enabled)


def verify_private_access_secret(secret: str) -> bool:
    expected = str(settings.private_mode_secret or "").strip()
    candidate = str(secret or "").strip()
    if not expected or not candidate:
        return False
    return hmac.compare_digest(candidate, expected)


def ensure_private_api_access(request: Request) -> None:
    if not private_mode_active():
        return
    if request.method.upper() == "OPTIONS":
        return

    secret = request.headers.get(PRIVATE_ACCESS_HEADER, "")
    if verify_private_access_secret(secret):
        return
    raise HTTPException(status_code=403, detail="private access required")
