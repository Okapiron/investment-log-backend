import logging
import uuid

from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from app.core.config import settings


logger = logging.getLogger("tradetrace.observability")

SECURITY_HEADERS = {
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "no-referrer",
}


def _apply_security_headers(response) -> None:
    for key, value in SECURITY_HEADERS.items():
        response.headers.setdefault(key, value)


def _apply_cache_policy(response, path: str) -> None:
    settings_prefix = f"{settings.api_prefix}/settings"
    if path == settings_prefix or path.startswith(f"{settings_prefix}/"):
        response.headers.setdefault("Cache-Control", "no-store")


class RequestIdMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        request_id = str(uuid.uuid4())
        request.state.request_id = request_id
        path = request.url.path or ""
        method = request.method

        try:
            response = await call_next(request)
        except Exception:
            logger.exception("request_failed request_id=%s method=%s path=%s", request_id, method, path)
            response = JSONResponse(
                {"detail": "internal server error", "request_id": request_id},
                status_code=500,
                headers={"X-Request-ID": request_id},
            )
            _apply_security_headers(response)
            _apply_cache_policy(response, path)
            return response

        response.headers["X-Request-ID"] = request_id
        _apply_security_headers(response)
        _apply_cache_policy(response, path)
        if int(response.status_code) >= 500:
            logger.error(
                "request_error request_id=%s method=%s path=%s status=%s",
                request_id,
                method,
                path,
                response.status_code,
            )
        return response
