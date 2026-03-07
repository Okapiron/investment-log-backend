import threading
import time
from collections import defaultdict

from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from app.core.config import settings
from app.core.jwt_utils import decode_and_verify_hs256


class SimpleRateLimitMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, max_per_minute: int = 120, api_prefix: str = "/api/v1"):
        super().__init__(app)
        self.max_per_minute = max(1, int(max_per_minute))
        self.api_prefix = str(api_prefix or "/api/v1")
        self._lock = threading.Lock()
        self._counts = defaultdict(int)
        self._minute = int(time.time() // 60)

    def _rollover_if_needed(self):
        cur = int(time.time() // 60)
        if cur == self._minute:
            return
        self._counts.clear()
        self._minute = cur

    def _resolve_principal_key(self, request) -> str:
        if settings.auth_enabled:
            secret = str(settings.supabase_jwt_secret or "").strip()
            auth_header = str(request.headers.get("authorization") or "").strip()
            if secret and auth_header.lower().startswith("bearer "):
                token = auth_header[7:].strip()
                if token:
                    try:
                        claims = decode_and_verify_hs256(token, secret)
                    except ValueError:
                        claims = {}
                    sub = str((claims or {}).get("sub") or "").strip()
                    if sub:
                        return f"user:{sub}"

        forwarded_for = str(request.headers.get("x-forwarded-for") or "").strip()
        if forwarded_for:
            client_ip = forwarded_for.split(",")[0].strip() or "unknown"
        else:
            client_ip = request.client.host if request.client else "unknown"
        return f"ip:{client_ip}"

    async def dispatch(self, request, call_next):
        path = request.url.path or ""
        method = (request.method or "").upper()
        if not path.startswith(self.api_prefix):
            return await call_next(request)
        if method == "OPTIONS":
            # CORS preflight should not consume user rate-limit budget.
            return await call_next(request)
        if path in {
            "/health",
            "/health/ready",
            f"{self.api_prefix}/health",
            f"{self.api_prefix}/health/ready",
        }:
            return await call_next(request)

        principal_key = self._resolve_principal_key(request)
        key = (principal_key, self._minute)

        with self._lock:
            self._rollover_if_needed()
            key = (principal_key, self._minute)
            self._counts[key] += 1
            used = int(self._counts[key])
            remaining = max(0, self.max_per_minute - used)
            retry_after = max(1, 60 - int(time.time() % 60))
            if self._counts[key] > self.max_per_minute:
                return JSONResponse(
                    {"detail": "rate limit exceeded"},
                    status_code=429,
                    headers={
                        "Retry-After": str(retry_after),
                        "X-RateLimit-Limit": str(self.max_per_minute),
                        "X-RateLimit-Remaining": "0",
                        "X-RateLimit-Reset": str(retry_after),
                    },
                )

        response = await call_next(request)
        response.headers["X-RateLimit-Limit"] = str(self.max_per_minute)
        response.headers["X-RateLimit-Remaining"] = str(remaining)
        response.headers["X-RateLimit-Reset"] = str(retry_after)
        return response
