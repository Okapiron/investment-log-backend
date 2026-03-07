import logging
import uuid

from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware


logger = logging.getLogger("tradetrace.observability")


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
            return JSONResponse(
                {"detail": "internal server error", "request_id": request_id},
                status_code=500,
                headers={"X-Request-ID": request_id},
            )

        response.headers["X-Request-ID"] = request_id
        if int(response.status_code) >= 500:
            logger.error(
                "request_error request_id=%s method=%s path=%s status=%s",
                request_id,
                method,
                path,
                response.status_code,
            )
        return response
