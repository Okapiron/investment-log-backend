import threading
import time
from collections import defaultdict

from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware


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

        forwarded_for = str(request.headers.get("x-forwarded-for") or "").strip()
        if forwarded_for:
            client_ip = forwarded_for.split(",")[0].strip() or "unknown"
        else:
            client_ip = request.client.host if request.client else "unknown"
        key = (client_ip, self._minute)

        with self._lock:
            self._rollover_if_needed()
            key = (client_ip, self._minute)
            self._counts[key] += 1
            if self._counts[key] > self.max_per_minute:
                return JSONResponse(
                    {"detail": "rate limit exceeded"},
                    status_code=429,
                    headers={"Retry-After": "60"},
                )

        return await call_next(request)
