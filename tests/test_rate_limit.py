from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from app.core.config import settings
from app.core.rate_limit import SimpleRateLimitMiddleware

import base64
import hashlib
import hmac
import json
import time


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def _build_hs256_jwt(payload: dict, secret: str) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    h_b64 = _b64url_encode(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    p_b64 = _b64url_encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signing_input = f"{h_b64}.{p_b64}".encode("utf-8")
    sig = hmac.new(secret.encode("utf-8"), signing_input, hashlib.sha256).digest()
    s_b64 = _b64url_encode(sig)
    return f"{h_b64}.{p_b64}.{s_b64}"


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
    retry_after = int(second.headers.get("retry-after") or "0")
    assert 1 <= retry_after <= 60


def test_health_endpoint_is_excluded_from_rate_limit():
    client = _build_test_app(max_per_minute=1)

    for _ in range(3):
        res = client.get("/health")
        assert res.status_code == 200

    first = client.get("/api/v1/ping")
    assert first.status_code == 200

    second = client.get("/api/v1/ping")
    assert second.status_code == 429


def test_rate_limit_uses_user_bucket_when_auth_enabled():
    client = _build_test_app(max_per_minute=1)

    prev_auth_enabled = settings.auth_enabled
    prev_secret = settings.supabase_jwt_secret
    settings.auth_enabled = True
    settings.supabase_jwt_secret = "test-secret"
    try:
        token_a = _build_hs256_jwt({"sub": "user-a", "exp": int(time.time()) + 3600}, "test-secret")
        token_b = _build_hs256_jwt({"sub": "user-b", "exp": int(time.time()) + 3600}, "test-secret")

        first_a = client.get(
            "/api/v1/ping",
            headers={"Authorization": f"Bearer {token_a}", "X-Forwarded-For": "203.0.113.10"},
        )
        assert first_a.status_code == 200

        first_b = client.get(
            "/api/v1/ping",
            headers={"Authorization": f"Bearer {token_b}", "X-Forwarded-For": "203.0.113.10"},
        )
        assert first_b.status_code == 200

        second_a = client.get(
            "/api/v1/ping",
            headers={"Authorization": f"Bearer {token_a}", "X-Forwarded-For": "203.0.113.10"},
        )
        assert second_a.status_code == 429
    finally:
        settings.auth_enabled = prev_auth_enabled
        settings.supabase_jwt_secret = prev_secret
