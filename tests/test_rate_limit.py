from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from app.core.rate_limit import SimpleRateLimitMiddleware


def _build_test_app(max_per_minute: int = 1) -> TestClient:
    app = FastAPI()
    app.add_middleware(SimpleRateLimitMiddleware, max_per_minute=max_per_minute, api_prefix="/api/v1")

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.get("/api/v1/ping")
    def ping():
        return {"ok": True}

    @app.options("/api/v1/ping")
    def ping_options():
        return JSONResponse({"ok": True})

    return TestClient(app)


def test_options_preflight_does_not_consume_rate_limit():
    client = _build_test_app(max_per_minute=1)

    preflight = client.options("/api/v1/ping")
    assert preflight.status_code == 200

    first = client.get("/api/v1/ping")
    assert first.status_code == 200

    second = client.get("/api/v1/ping")
    assert second.status_code == 429


def test_health_endpoint_is_excluded_from_rate_limit():
    client = _build_test_app(max_per_minute=1)

    for _ in range(3):
        res = client.get("/health")
        assert res.status_code == 200

    first = client.get("/api/v1/ping")
    assert first.status_code == 200

    second = client.get("/api/v1/ping")
    assert second.status_code == 429
