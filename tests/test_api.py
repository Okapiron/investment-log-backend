import base64
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
import time

from sqlalchemy import select

from app.core.config import settings
from app.core.invites import hash_invite_code
from app.db.models import InviteCode
from app.main import app, get_runtime_config_issues


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


def test_openapi(client):
    res = client.get("/openapi.json")
    assert res.status_code == 200
    data = res.json()
    assert data.get("info", {}).get("title") == settings.app_name
    assert "/api/v1/dashboard/monthly" in data["paths"]
    assert "/api/v1/monthly" in data["paths"]
    assert "/api/v1/snapshots/copy-latest" in data["paths"]
    assert "/api/v1/trades" in data["paths"]


def test_runtime_config_requires_release_fields_when_auth_enabled():
    prev_auth_enabled = settings.auth_enabled
    prev_supabase_url = settings.supabase_url
    prev_supabase_jwt_secret = settings.supabase_jwt_secret
    prev_ops_alert_target = settings.ops_alert_target
    prev_db_backup_strategy = settings.db_backup_strategy
    prev_cors_allow_origins = settings.cors_allow_origins

    try:
        settings.auth_enabled = True
        settings.supabase_url = ""
        settings.supabase_jwt_secret = ""
        settings.ops_alert_target = ""
        settings.db_backup_strategy = ""
        settings.cors_allow_origins = "*"

        errors, warnings = get_runtime_config_issues()
        assert "SUPABASE_URL is required when AUTH_ENABLED=true" in errors
        assert "SUPABASE_JWT_SECRET is required when AUTH_ENABLED=true" in errors
        assert "OPS_ALERT_TARGET is required when AUTH_ENABLED=true" in errors
        assert "DB_BACKUP_STRATEGY is required when AUTH_ENABLED=true" in errors
        assert "CORS_ALLOW_ORIGINS is wildcard in auth-enabled mode" in warnings
    finally:
        settings.auth_enabled = prev_auth_enabled
        settings.supabase_url = prev_supabase_url
        settings.supabase_jwt_secret = prev_supabase_jwt_secret
        settings.ops_alert_target = prev_ops_alert_target
        settings.db_backup_strategy = prev_db_backup_strategy
        settings.cors_allow_origins = prev_cors_allow_origins


def test_health_endpoints(client):
    res = client.get("/health")
    assert res.status_code == 200
    assert res.json()["status"] == "ok"
    assert str(res.json().get("version") or "").strip() != ""
    assert str(res.headers.get("x-request-id") or "").strip() != ""
    assert res.headers.get("x-content-type-options") == "nosniff"
    assert res.headers.get("x-frame-options") == "DENY"
    assert res.headers.get("referrer-policy") == "no-referrer"

    prefixed = client.get("/api/v1/health")
    assert prefixed.status_code == 200
    assert prefixed.json()["status"] == "ok"
    assert str(prefixed.json().get("version") or "").strip() != ""
    assert str(prefixed.headers.get("x-request-id") or "").strip() != ""
    assert prefixed.headers.get("x-content-type-options") == "nosniff"
    assert prefixed.headers.get("x-frame-options") == "DENY"
    assert prefixed.headers.get("referrer-policy") == "no-referrer"

    ready = client.get("/health/ready")
    assert ready.status_code == 200
    body = ready.json()
    assert body["status"] == "ok"
    assert body["db"] == "ok"
    assert str(body.get("version") or "").strip() != ""
    assert str(ready.headers.get("x-request-id") or "").strip() != ""
    assert ready.headers.get("x-content-type-options") == "nosniff"
    assert ready.headers.get("x-frame-options") == "DENY"
    assert ready.headers.get("referrer-policy") == "no-referrer"

    prefixed_ready = client.get("/api/v1/health/ready")
    assert prefixed_ready.status_code == 200
    prefixed_body = prefixed_ready.json()
    assert prefixed_body["status"] == "ok"
    assert prefixed_body["db"] == "ok"
    assert str(prefixed_body.get("version") or "").strip() != ""
    assert str(prefixed_ready.headers.get("x-request-id") or "").strip() != ""
    assert prefixed_ready.headers.get("x-content-type-options") == "nosniff"
    assert prefixed_ready.headers.get("x-frame-options") == "DENY"
    assert prefixed_ready.headers.get("referrer-policy") == "no-referrer"


def test_trades_requires_auth_when_enabled(client):
    settings.auth_enabled = True
    settings.supabase_jwt_secret = "test-secret"

    no_auth = client.get("/api/v1/trades")
    assert no_auth.status_code == 401

    token = _build_hs256_jwt({"sub": "user-1", "exp": int(time.time()) + 3600}, "test-secret")
    with_auth = client.get("/api/v1/trades", headers={"Authorization": f"Bearer {token}"})
    assert with_auth.status_code == 200


def test_trades_are_scoped_by_user_when_auth_enabled(client):
    settings.auth_enabled = True
    settings.supabase_jwt_secret = "test-secret"

    token_a = _build_hs256_jwt({"sub": "user-a", "exp": int(time.time()) + 3600}, "test-secret")
    token_b = _build_hs256_jwt({"sub": "user-b", "exp": int(time.time()) + 3600}, "test-secret")
    headers_a = {"Authorization": f"Bearer {token_a}"}
    headers_b = {"Authorization": f"Bearer {token_b}"}

    created = client.post(
        "/api/v1/trades",
        headers=headers_a,
        json={
            "market": "JP",
            "symbol": "UAAA",
            "fills": [
                {"side": "buy", "date": "2026-07-01", "price": 1000, "qty": 1, "fee": 0},
            ],
        },
    )
    assert created.status_code == 201
    trade_id = created.json()["id"]

    list_a = client.get("/api/v1/trades", headers=headers_a)
    assert list_a.status_code == 200
    assert list_a.json()["total"] == 1

    list_b = client.get("/api/v1/trades", headers=headers_b)
    assert list_b.status_code == 200
    assert list_b.json()["total"] == 0

    get_b = client.get(f"/api/v1/trades/{trade_id}", headers=headers_b)
    assert get_b.status_code == 404

    patch_b = client.patch(
        f"/api/v1/trades/{trade_id}",
        headers=headers_b,
        json={
            "buy_date": "2026-07-01",
            "buy_price": 1000,
            "buy_qty": 1,
        },
    )
    assert patch_b.status_code == 404

    delete_b = client.delete(f"/api/v1/trades/{trade_id}", headers=headers_b)
    assert delete_b.status_code == 404


def _insert_invite(code: str, days: int = 7) -> None:
    session_local = app.state.testing_session_local
    with session_local() as db:
        db.add(
            InviteCode(
                code_hash=hash_invite_code(code),
                expires_at=datetime.now(timezone.utc) + timedelta(days=days),
                max_uses=1,
                used_count=0,
                used_by_user_id=None,
            )
        )
        db.commit()


def test_trades_require_valid_invite_code_when_invite_required(client):
    settings.auth_enabled = True
    settings.invite_code_required = True
    settings.supabase_jwt_secret = "test-secret"

    missing_code_token = _build_hs256_jwt(
        {"sub": "user-no-code", "exp": int(time.time()) + 3600},
        "test-secret",
    )
    missing_code = client.get("/api/v1/trades", headers={"Authorization": f"Bearer {missing_code_token}"})
    assert missing_code.status_code == 403

    bad_code_token = _build_hs256_jwt(
        {"sub": "user-bad-code", "invite_code": "BADCODE99", "exp": int(time.time()) + 3600},
        "test-secret",
    )
    bad_code = client.get("/api/v1/trades", headers={"Authorization": f"Bearer {bad_code_token}"})
    assert bad_code.status_code == 403

    _insert_invite("GOODCODE99")
    good_code_token = _build_hs256_jwt(
        {"sub": "user-good-code", "invite_code": "GOODCODE99", "exp": int(time.time()) + 3600},
        "test-secret",
    )
    good_code = client.get("/api/v1/trades", headers={"Authorization": f"Bearer {good_code_token}"})
    assert good_code.status_code == 200


def test_invite_code_is_one_time_and_bound_to_first_user(client):
    settings.auth_enabled = True
    settings.invite_code_required = True
    settings.supabase_jwt_secret = "test-secret"
    _insert_invite("ONETIME999")

    token_a = _build_hs256_jwt(
        {"sub": "user-a", "invite_code": "ONETIME999", "exp": int(time.time()) + 3600},
        "test-secret",
    )
    token_b = _build_hs256_jwt(
        {"sub": "user-b", "invite_code": "ONETIME999", "exp": int(time.time()) + 3600},
        "test-secret",
    )

    first = client.get("/api/v1/trades", headers={"Authorization": f"Bearer {token_a}"})
    assert first.status_code == 200
    session_local = app.state.testing_session_local
    with session_local() as db:
        used_row = db.scalar(select(InviteCode).where(InviteCode.code_hash == hash_invite_code("ONETIME999")))
        assert used_row is not None
        assert used_row.used_by_user_id == "user-a"
        assert used_row.used_at is not None

    second = client.get("/api/v1/trades", headers={"Authorization": f"Bearer {token_b}"})
    assert second.status_code == 403

    again_same_user = client.get("/api/v1/trades", headers={"Authorization": f"Bearer {token_a}"})
    assert again_same_user.status_code == 200


def test_settings_export_and_delete_are_user_scoped(client):
    settings.auth_enabled = True
    settings.invite_code_required = False
    settings.supabase_jwt_secret = "test-secret"

    token_a = _build_hs256_jwt({"sub": "user-sa", "email": "a@example.com", "exp": int(time.time()) + 3600}, "test-secret")
    token_b = _build_hs256_jwt({"sub": "user-sb", "email": "b@example.com", "exp": int(time.time()) + 3600}, "test-secret")
    headers_a = {"Authorization": f"Bearer {token_a}"}
    headers_b = {"Authorization": f"Bearer {token_b}"}

    created_a = client.post(
        "/api/v1/trades",
        headers=headers_a,
        json={
            "market": "JP",
            "symbol": "SUA1",
            "fills": [{"side": "buy", "date": "2026-08-01", "price": 1000, "qty": 1, "fee": 0}],
        },
    )
    assert created_a.status_code == 201
    created_b = client.post(
        "/api/v1/trades",
        headers=headers_b,
        json={
            "market": "US",
            "symbol": "SUB1",
            "fills": [{"side": "buy", "date": "2026-08-01", "price": 100, "qty": 1, "fee": 0}],
        },
    )
    assert created_b.status_code == 201

    session_local = app.state.testing_session_local
    with session_local() as db:
        db.add(
            InviteCode(
                code_hash=hash_invite_code("DELETEU001"),
                expires_at=datetime.now(timezone.utc) + timedelta(days=5),
                max_uses=1,
                used_count=1,
                used_by_user_id="user-sa",
                used_at=datetime.now(timezone.utc),
            )
        )
        db.commit()

    me_a = client.get("/api/v1/settings/me", headers=headers_a)
    assert me_a.status_code == 200
    assert me_a.json()["user_id"] == "user-sa"
    assert me_a.json()["email"] == "a@example.com"
    assert me_a.headers.get("cache-control") == "no-store"

    export_a = client.get("/api/v1/settings/export", params={"format": "json"}, headers=headers_a)
    assert export_a.status_code == 200
    assert export_a.headers.get("cache-control") == "no-store"
    body_a = export_a.json()
    assert body_a["count"] == 1
    assert body_a["trades"][0]["symbol"] == "SUA1"

    delete_without_confirm = client.delete("/api/v1/settings/me", headers=headers_a)
    assert delete_without_confirm.status_code == 400
    assert delete_without_confirm.headers.get("cache-control") == "no-store"

    delete_without_confirm_text = client.delete(
        "/api/v1/settings/me",
        params={"confirm": "true"},
        headers=headers_a,
    )
    assert delete_without_confirm_text.status_code == 400
    assert delete_without_confirm_text.headers.get("cache-control") == "no-store"

    delete_a = client.delete(
        "/api/v1/settings/me",
        params={"confirm": "true", "confirm_text": "DELETE"},
        headers=headers_a,
    )
    assert delete_a.status_code == 200
    assert delete_a.headers.get("cache-control") == "no-store"
    assert delete_a.json()["deleted_trades"] == 1
    assert delete_a.json()["anonymized_invites"] == 1

    with session_local() as db:
        invite = db.scalar(select(InviteCode).where(InviteCode.code_hash == hash_invite_code("DELETEU001")))
        assert invite is not None
        assert invite.used_count == 1
        assert invite.used_by_user_id is None

    list_a = client.get("/api/v1/trades", headers=headers_a)
    assert list_a.status_code == 200
    assert list_a.json()["total"] == 0

    list_b = client.get("/api/v1/trades", headers=headers_b)
    assert list_b.status_code == 200
    assert list_b.json()["total"] == 1


def test_settings_runtime_available_when_auth_disabled(client):
    res = client.get("/api/v1/settings/runtime")
    assert res.status_code == 200
    assert res.headers.get("cache-control") == "no-store"
    body = res.json()
    assert body["status"] in {"ok", "ng"}
    assert body["db"] in {"ok", "ng"}
    assert body["release_status"] in {"ok", "warning", "error"}
    assert str(body.get("server_time_utc") or "").strip() != ""
    assert str(body.get("app_version") or "").strip() != ""
    assert body["auth_enabled"] is False
    assert body["invite_code_required"] is False
    assert body["invite_active_count"] is None
    assert body["invite_onboarding_ready"] is None
    assert body["config_errors"] == []
    assert body["config_warnings"] == []


def test_settings_runtime_requires_auth_when_auth_enabled(client):
    settings.auth_enabled = True
    settings.invite_code_required = False
    settings.supabase_jwt_secret = "test-secret"

    no_auth = client.get("/api/v1/settings/runtime")
    assert no_auth.status_code == 401
    assert no_auth.headers.get("cache-control") == "no-store"

    token = _build_hs256_jwt({"sub": "runtime-user", "exp": int(time.time()) + 3600}, "test-secret")
    with_auth = client.get("/api/v1/settings/runtime", headers={"Authorization": f"Bearer {token}"})
    assert with_auth.status_code == 200
    assert with_auth.headers.get("cache-control") == "no-store"
    payload = with_auth.json()
    assert payload["status"] in {"ok", "ng"}
    assert payload["db"] in {"ok", "ng"}
    assert payload["release_status"] in {"ok", "warning", "error"}
    assert str(payload.get("server_time_utc") or "").strip() != ""
    assert str(payload.get("app_version") or "").strip() != ""
    assert payload["auth_enabled"] is True
    assert payload["invite_code_required"] is False
    assert payload["invite_active_count"] is None
    assert payload["invite_onboarding_ready"] is None
    assert isinstance(payload.get("config_errors"), list)
    assert isinstance(payload.get("config_warnings"), list)


def test_settings_runtime_release_status_warning_when_only_warnings_exist(client):
    settings.auth_enabled = True
    settings.supabase_jwt_secret = "test-secret"
    settings.supabase_url = "https://demo.supabase.co"
    settings.ops_alert_target = "slack:#ops"
    settings.db_backup_strategy = "render-managed-daily"
    settings.cors_allow_origins = "*"

    token = _build_hs256_jwt({"sub": "runtime-warning", "exp": int(time.time()) + 3600}, "test-secret")
    res = client.get("/api/v1/settings/runtime", headers={"Authorization": f"Bearer {token}"})
    assert res.status_code == 200
    payload = res.json()
    assert payload["release_status"] == "warning"
    assert payload["config_errors"] == []
    assert any("CORS_ALLOW_ORIGINS is wildcard" in w for w in payload["config_warnings"])


def test_settings_runtime_release_status_error_when_required_config_missing(client):
    settings.auth_enabled = True
    settings.supabase_jwt_secret = "test-secret"
    settings.supabase_url = ""
    settings.ops_alert_target = ""
    settings.db_backup_strategy = ""
    settings.cors_allow_origins = "https://investment-log-frontend.vercel.app"

    token = _build_hs256_jwt({"sub": "runtime-error", "exp": int(time.time()) + 3600}, "test-secret")
    res = client.get("/api/v1/settings/runtime", headers={"Authorization": f"Bearer {token}"})
    assert res.status_code == 200
    payload = res.json()
    assert payload["release_status"] == "error"
    assert "SUPABASE_URL is required when AUTH_ENABLED=true" in payload["config_errors"]
    assert "OPS_ALERT_TARGET is required when AUTH_ENABLED=true" in payload["config_errors"]
    assert "DB_BACKUP_STRATEGY is required when AUTH_ENABLED=true" in payload["config_errors"]


def test_settings_runtime_invite_readiness_when_invite_required_and_no_active_codes(client):
    settings.auth_enabled = True
    settings.invite_code_required = True
    settings.supabase_jwt_secret = "test-secret"
    settings.supabase_url = "https://demo.supabase.co"
    settings.ops_alert_target = "slack:#ops"
    settings.db_backup_strategy = "render-managed-daily"
    settings.supabase_service_role_key = "service-key"
    settings.cors_allow_origins = "https://investment-log-frontend.vercel.app"
    settings.rate_limit_enabled = True

    session_local = app.state.testing_session_local
    with session_local() as db:
        db.add(
            InviteCode(
                code_hash=hash_invite_code("RUNTIMEUSED"),
                expires_at=datetime.now(timezone.utc) + timedelta(days=3),
                max_uses=1,
                used_count=1,
                used_by_user_id="runtime-invite",
                used_at=datetime.now(timezone.utc) - timedelta(minutes=1),
            )
        )
        db.commit()

    token = _build_hs256_jwt(
        {"sub": "runtime-invite", "invite_code": "RUNTIMEUSED", "exp": int(time.time()) + 3600},
        "test-secret",
    )
    res = client.get("/api/v1/settings/runtime", headers={"Authorization": f"Bearer {token}"})
    assert res.status_code == 200
    payload = res.json()
    assert payload["invite_code_required"] is True
    assert payload["invite_active_count"] == 0
    assert payload["invite_onboarding_ready"] is False
    assert any("no active invite codes found" in w for w in payload["config_warnings"])


def test_account_asset_snapshot_crud_and_dashboard(client):
    account = client.post(
        "/api/v1/accounts",
        json={"name": "楽天証券", "display_order": 1, "is_active": True},
    )
    assert account.status_code == 201
    account_id = account.json()["id"]

    asset = client.post(
        "/api/v1/assets",
        json={
            "account_id": account_id,
            "name": "全世界株",
            "asset_type": "fund",
            "currency": "JPY",
            "display_order": 1,
            "is_active": True,
        },
    )
    assert asset.status_code == 201
    asset_id = asset.json()["id"]

    snap = client.post(
        "/api/v1/snapshots",
        json={"month": "2026-01", "asset_id": asset_id, "value_jpy": 1000000, "memo": "m1"},
    )
    assert snap.status_code == 201
    body = snap.json()
    assert body["account_id"] == account_id

    latest = client.get("/api/v1/dashboard/latest")
    assert latest.status_code == 200
    latest_data = latest.json()
    assert latest_data["month"] == "2026-01"
    assert latest_data["total_jpy"] == 1000000
    assert latest_data["by_asset_type"][0]["asset_type"] == "fund"

    monthly = client.get("/api/v1/dashboard/monthly", params={"from": "2026-01", "to": "2026-01"})
    assert monthly.status_code == 200
    point = monthly.json()["points"][0]
    assert point["total_jpy"] == 1000000
    assert point["by_asset_type"]["fund"] == 1000000


def test_unique_and_integrity_conflict_returns_409(client):
    r1 = client.post("/api/v1/accounts", json={"name": "銀行A"})
    assert r1.status_code == 201

    r2 = client.post("/api/v1/accounts", json={"name": "銀行A"})
    assert r2.status_code == 409

    bad_asset = client.post(
        "/api/v1/assets",
        json={"account_id": 999, "name": "不正", "asset_type": "cash", "currency": "JPY"},
    )
    assert bad_asset.status_code == 409


def test_delete_referenced_account_returns_409(client):
    acc = client.post("/api/v1/accounts", json={"name": "証券口座"}).json()
    asset = client.post(
        "/api/v1/assets",
        json={"account_id": acc["id"], "name": "現金", "asset_type": "cash", "currency": "JPY"},
    )
    assert asset.status_code == 201

    delete_res = client.delete(f"/api/v1/accounts/{acc['id']}")
    assert delete_res.status_code == 409


def test_snapshot_duplicate_returns_409(client):
    acc = client.post("/api/v1/accounts", json={"name": "口座X"}).json()
    asset = client.post(
        "/api/v1/assets",
        json={"account_id": acc["id"], "name": "投信A", "asset_type": "fund", "currency": "JPY"},
    ).json()

    s1 = client.post(
        "/api/v1/snapshots",
        json={"month": "2026-02", "asset_id": asset["id"], "value_jpy": 100},
    )
    assert s1.status_code == 201

    s2 = client.post(
        "/api/v1/snapshots",
        json={"month": "2026-02", "asset_id": asset["id"], "value_jpy": 200},
    )
    assert s2.status_code == 409


def test_monthly_tree_api(client):
    acc_active = client.post("/api/v1/accounts", json={"name": "A1", "is_active": True}).json()
    acc_inactive = client.post("/api/v1/accounts", json={"name": "A2", "is_active": False}).json()

    active_asset = client.post(
        "/api/v1/assets",
        json={"account_id": acc_active["id"], "name": "FundA", "asset_type": "fund", "currency": "JPY", "is_active": True},
    ).json()
    client.post(
        "/api/v1/assets",
        json={"account_id": acc_active["id"], "name": "CashB", "asset_type": "cash", "currency": "JPY", "is_active": True},
    )
    client.post(
        "/api/v1/assets",
        json={"account_id": acc_active["id"], "name": "Hidden", "asset_type": "cash", "currency": "JPY", "is_active": False},
    )
    client.post(
        "/api/v1/assets",
        json={"account_id": acc_inactive["id"], "name": "Nope", "asset_type": "cash", "currency": "JPY", "is_active": True},
    )

    client.post(
        "/api/v1/snapshots",
        json={"month": "2026-02", "asset_id": active_asset["id"], "value_jpy": 123456},
    )

    res = client.get("/api/v1/monthly", params={"month": "2026-02"})
    assert res.status_code == 200
    data = res.json()
    assert data["month"] == "2026-02"
    assert len(data["accounts"]) == 1
    assert data["accounts"][0]["account_id"] == acc_active["id"]
    assert len(data["accounts"][0]["assets"]) == 2
    assert data["summary"]["filled"] == 1
    assert data["summary"]["missing"] == 1


def test_copy_latest_skips_existing(client):
    acc = client.post("/api/v1/accounts", json={"name": "CopyAcc"}).json()
    a1 = client.post(
        "/api/v1/assets",
        json={"account_id": acc["id"], "name": "F1", "asset_type": "fund", "currency": "JPY", "is_active": True},
    ).json()
    a2 = client.post(
        "/api/v1/assets",
        json={"account_id": acc["id"], "name": "C1", "asset_type": "cash", "currency": "JPY", "is_active": True},
    ).json()
    client.post(
        "/api/v1/assets",
        json={"account_id": acc["id"], "name": "X1", "asset_type": "other", "currency": "JPY", "is_active": False},
    )

    client.post("/api/v1/snapshots", json={"month": "2026-01", "asset_id": a1["id"], "value_jpy": 1000})
    client.post("/api/v1/snapshots", json={"month": "2026-01", "asset_id": a2["id"], "value_jpy": 2000})
    client.post("/api/v1/snapshots", json={"month": "2026-02", "asset_id": a1["id"], "value_jpy": 3000})

    copy = client.post("/api/v1/snapshots/copy-latest", json={"to_month": "2026-02"})
    assert copy.status_code == 200
    payload = copy.json()
    assert payload["from_month"] == "2026-02"
    assert payload["to_month"] == "2026-02"
    assert payload["created"] == 1
    assert payload["skipped"] == 1


def test_trades_crud_flow(client):
    payload = {
        "market": "JP",
        "symbol": "7203",
        "name": "Toyota",
        "notes_buy": "breakout",
        "notes_sell": "target hit",
        "notes_review": "good",
        "rating": 4,
        "tags": "swing,auto",
        "chart_image_url": "https://example.com/c.png",
        "fills": [
            {"side": "buy", "date": "2026-01-10", "price": 1000, "qty": 10, "fee": 100},
            {"side": "sell", "date": "2026-01-15", "price": 1200, "qty": 10, "fee": 100},
        ],
    }
    created = client.post("/api/v1/trades", json=payload)
    assert created.status_code == 201
    trade = created.json()
    trade_id = trade["id"]
    assert trade["opened_at"] == "2026-01-10"
    assert trade["closed_at"] == "2026-01-15"
    assert trade["profit_jpy"] == 1800
    assert trade["profit_usd"] is None
    assert trade["profit_currency"] == "JPY"
    assert trade["holding_days"] == 5
    assert trade["review_done"] is False
    assert trade["reviewed_at"] is None

    listed = client.get("/api/v1/trades", params={"market": "JP", "symbol": "720"})
    assert listed.status_code == 200
    list_body = listed.json()
    assert list_body["total"] == 1
    assert list_body["limit"] == 20
    assert list_body["offset"] == 0
    assert "stats" in list_body
    assert "pending_review_count" in list_body["stats"]
    assert list_body["items"][0]["id"] == trade_id

    detail = client.get(f"/api/v1/trades/{trade_id}")
    assert detail.status_code == 200
    assert len(detail.json()["fills"]) == 2

    updated = client.patch(
        f"/api/v1/trades/{trade_id}",
        json={
            "rating": 5,
            "fills": [
                {"side": "buy", "date": "2026-01-10", "price": 1000, "qty": 10, "fee": 50},
                {"side": "sell", "date": "2026-01-20", "price": 1300, "qty": 10, "fee": 50},
            ],
        },
    )
    assert updated.status_code == 200
    updated_body = updated.json()
    assert updated_body["rating"] == 5
    assert updated_body["closed_at"] == "2026-01-20"
    assert updated_body["profit_jpy"] == 2900
    assert updated_body["profit_usd"] is None
    assert updated_body["profit_currency"] == "JPY"
    assert updated_body["holding_days"] == 10

    deleted = client.delete(f"/api/v1/trades/{trade_id}")
    assert deleted.status_code == 204
    assert client.get(f"/api/v1/trades/{trade_id}").status_code == 404


def test_trades_validation_422(client):
    bad = client.post(
        "/api/v1/trades",
        json={
            "market": "JP",
            "symbol": "9984",
            "fills": [
                {"side": "buy", "date": "2026-01-10", "price": 1000, "qty": 10, "fee": 0},
                {"side": "sell", "date": "2026-01-09", "price": 900, "qty": 10, "fee": 0},
            ],
        },
    )
    assert bad.status_code == 422


def test_trades_create_open_position_buy_only(client):
    created = client.post(
        "/api/v1/trades",
        json={
            "market": "US",
            "symbol": "AAPL",
            "review_done": True,
            "reviewed_at": "2026-02-11",
            "fills": [
                {"side": "buy", "date": "2026-02-10", "price": 200, "qty": 5, "fee": 0},
            ],
        },
    )
    assert created.status_code == 201
    body = created.json()
    assert body["is_open"] is True
    assert body["closed_at"] is None
    assert body["profit_jpy"] is None
    assert body["profit_usd"] is None
    assert body["profit_currency"] == "USD"
    assert body["holding_days"] is None
    assert len(body["fills"]) == 1
    assert body["review_done"] is False
    assert body["reviewed_at"] is None


def test_trades_us_closed_returns_profit_usd(client):
    created = client.post(
        "/api/v1/trades",
        json={
            "market": "US",
            "symbol": "MSFT",
            "fills": [
                {"side": "buy", "date": "2026-02-10", "price": 100, "qty": 3, "fee": 1},
                {"side": "sell", "date": "2026-02-12", "price": 120, "qty": 3, "fee": 1},
            ],
        },
    )
    assert created.status_code == 201
    body = created.json()
    assert body["profit_jpy"] is None
    assert body["profit_usd"] == 58
    assert body["profit_currency"] == "USD"


def test_trades_list_pagination_limit_offset(client):
    for symbol in ["AAA", "BBB", "CCC"]:
        created = client.post(
            "/api/v1/trades",
            json={
                "market": "JP",
                "symbol": symbol,
                "fills": [
                    {"side": "buy", "date": "2026-02-01", "price": 100, "qty": 1, "fee": 0},
                    {"side": "sell", "date": "2026-02-02", "price": 110, "qty": 1, "fee": 0},
                ],
            },
        )
        assert created.status_code == 201

    listed = client.get(
        "/api/v1/trades",
        params={"limit": 1, "offset": 1, "sort": "buy_date", "sort_dir": "asc"},
    )
    assert listed.status_code == 200
    body = listed.json()
    assert body["limit"] == 1
    assert body["offset"] == 1
    assert body["total"] == 3
    assert len(body["items"]) == 1


def test_trades_status_filter_and_update(client):
    created = client.post(
        "/api/v1/trades",
        json={
            "market": "JP",
            "symbol": "R1",
            "tags": "swing",
            "rating": 3,
            "notes_buy": "buy note",
            "notes_sell": "sell note",
            "notes_review": "review note",
            "fills": [
                {"side": "buy", "date": "2026-03-01", "price": 100, "qty": 1, "fee": 0},
                {"side": "sell", "date": "2026-03-02", "price": 110, "qty": 1, "fee": 0},
            ],
        },
    )
    assert created.status_code == 201
    trade_id = created.json()["id"]

    open_created = client.post(
        "/api/v1/trades",
        json={
            "market": "US",
            "symbol": "QQQ",
            "fills": [
                {"side": "buy", "date": "2026-03-03", "price": 100, "qty": 1, "fee": 0},
            ],
        },
    )
    assert open_created.status_code == 201

    pending = client.get("/api/v1/trades", params={"status": "pending"})
    assert pending.status_code == 200
    pending_body = pending.json()
    assert pending_body["total"] == 1
    assert pending_body["stats"]["pending_review_count"] == 1

    updated = client.patch(
        f"/api/v1/trades/{trade_id}",
        json={"review_done": True, "reviewed_at": "2026-03-05"},
    )
    assert updated.status_code == 200
    assert updated.json()["review_done"] is True
    assert updated.json()["reviewed_at"] == "2026-03-05"

    done = client.get("/api/v1/trades", params={"status": "complete"})
    assert done.status_code == 200
    done_body = done.json()
    assert done_body["total"] == 1
    assert done_body["items"][0]["review_done"] is True
    assert done_body["stats"]["pending_review_count"] == 0


def test_review_done_requires_completion_requirements(client):
    created = client.post(
        "/api/v1/trades",
        json={
            "market": "JP",
            "symbol": "REQ1",
            "fills": [
                {"side": "buy", "date": "2026-03-01", "price": 100, "qty": 1, "fee": 0},
                {"side": "sell", "date": "2026-03-02", "price": 110, "qty": 1, "fee": 0},
            ],
        },
    )
    assert created.status_code == 201
    trade_id = created.json()["id"]

    review_done = client.patch(
        f"/api/v1/trades/{trade_id}",
        json={"review_done": True, "reviewed_at": "2026-03-05"},
    )
    assert review_done.status_code == 422
    assert "レビュー完了に必要な項目が不足しています" in str(review_done.json().get("detail"))


def test_regular_update_auto_resets_review_done_when_requirements_lost(client):
    created = client.post(
        "/api/v1/trades",
        json={
            "market": "JP",
            "symbol": "REQ2",
            "tags": "swing",
            "rating": 4,
            "notes_buy": "buy note",
            "notes_sell": "sell note",
            "notes_review": "review note",
            "fills": [
                {"side": "buy", "date": "2026-03-01", "price": 100, "qty": 1, "fee": 0},
                {"side": "sell", "date": "2026-03-02", "price": 110, "qty": 1, "fee": 0},
            ],
        },
    )
    assert created.status_code == 201
    trade_id = created.json()["id"]

    review_done = client.patch(
        f"/api/v1/trades/{trade_id}",
        json={"review_done": True, "reviewed_at": "2026-03-05"},
    )
    assert review_done.status_code == 200
    assert review_done.json()["review_done"] is True

    updated = client.patch(
        f"/api/v1/trades/{trade_id}",
        json={"tags": ""},
    )
    assert updated.status_code == 200
    body = updated.json()
    assert body["review_done"] is False
    assert body["reviewed_at"] is None


def test_trade_detail_update_can_close_open_trade_and_keep_review_pending(client):
    created = client.post(
        "/api/v1/trades",
        json={
            "market": "JP",
            "symbol": "7203",
            "fills": [
                {"side": "buy", "date": "2026-03-01", "price": 1000, "qty": 10, "fee": 0},
            ],
        },
    )
    assert created.status_code == 201
    trade_id = created.json()["id"]
    assert created.json()["review_done"] is False

    closed = client.patch(
        f"/api/v1/trades/{trade_id}",
        json={
            "buy_date": "2026-03-01",
            "buy_price": 1000,
            "buy_qty": 10,
            "sell_date": "2026-03-08",
            "sell_price": 1100,
            "sell_qty": 10,
            "notes_sell": "target hit",
            "notes_review": "after close",
            "rating": 4,
        },
    )
    assert closed.status_code == 200
    body = closed.json()
    assert body["is_open"] is False
    assert body["closed_at"] == "2026-03-08"
    assert body["review_done"] is False
    assert body["profit_jpy"] == 1000


def test_trade_detail_update_rejects_partial_sell_and_allows_reopen(client):
    open_created = client.post(
        "/api/v1/trades",
        json={
            "market": "US",
            "symbol": "AAPL",
            "fills": [
                {"side": "buy", "date": "2026-04-01", "price": 100, "qty": 5, "fee": 0},
            ],
        },
    )
    assert open_created.status_code == 201
    open_id = open_created.json()["id"]

    partial = client.patch(
        f"/api/v1/trades/{open_id}",
        json={
            "buy_date": "2026-04-01",
            "buy_price": 100,
            "buy_qty": 5,
            "sell_date": "2026-04-05",
        },
    )
    assert partial.status_code == 422

    closed_created = client.post(
        "/api/v1/trades",
        json={
            "market": "JP",
            "symbol": "6501",
            "rating": 4,
            "fills": [
                {"side": "buy", "date": "2026-04-01", "price": 1000, "qty": 1, "fee": 0},
                {"side": "sell", "date": "2026-04-02", "price": 1100, "qty": 1, "fee": 0},
            ],
        },
    )
    assert closed_created.status_code == 201
    closed_id = closed_created.json()["id"]

    reopen = client.patch(
        f"/api/v1/trades/{closed_id}",
        json={
            "buy_date": "2026-04-01",
            "buy_price": 1000,
            "buy_qty": 1,
        },
    )
    assert reopen.status_code == 200
    reopen_body = reopen.json()
    assert reopen_body["is_open"] is True
    assert reopen_body["closed_at"] is None
    assert reopen_body["profit_jpy"] is None
    assert reopen_body["rating"] is None

    mixed_review = client.patch(
        f"/api/v1/trades/{closed_id}",
        json={"rating": 5, "review_done": True},
    )
    assert mixed_review.status_code == 422


def test_trades_name_sort_groups_jp_and_us(client):
    payloads = [
        {
            "market": "JP",
            "symbol": "7203",
            "name": "トヨタ自動車",
            "fills": [
                {"side": "buy", "date": "2026-05-01", "price": 1000, "qty": 1, "fee": 0},
                {"side": "sell", "date": "2026-05-02", "price": 1010, "qty": 1, "fee": 0},
            ],
        },
        {
            "market": "JP",
            "symbol": "6479",
            "name": "アイシン",
            "fills": [
                {"side": "buy", "date": "2026-05-01", "price": 1000, "qty": 1, "fee": 0},
                {"side": "sell", "date": "2026-05-02", "price": 1010, "qty": 1, "fee": 0},
            ],
        },
        {
            "market": "US",
            "symbol": "AAPL",
            "name": "Apple Inc",
            "fills": [
                {"side": "buy", "date": "2026-05-01", "price": 100, "qty": 1, "fee": 0},
                {"side": "sell", "date": "2026-05-02", "price": 101, "qty": 1, "fee": 0},
            ],
        },
        {
            "market": "US",
            "symbol": "MSFT",
            "name": "Microsoft Corp",
            "fills": [
                {"side": "buy", "date": "2026-05-01", "price": 100, "qty": 1, "fee": 0},
                {"side": "sell", "date": "2026-05-02", "price": 101, "qty": 1, "fee": 0},
            ],
        },
    ]
    for p in payloads:
        res = client.post("/api/v1/trades", json=p)
        assert res.status_code == 201

    asc = client.get("/api/v1/trades", params={"sort": "name", "sort_dir": "asc", "limit": 20, "offset": 0})
    assert asc.status_code == 200
    asc_items = asc.json()["items"]
    assert [x["market"] for x in asc_items] == ["JP", "JP", "US", "US"]
    assert [x["symbol"] for x in asc_items] == ["6479", "7203", "AAPL", "MSFT"]

    desc = client.get("/api/v1/trades", params={"sort": "name", "sort_dir": "desc", "limit": 20, "offset": 0})
    assert desc.status_code == 200
    desc_items = desc.json()["items"]
    assert [x["market"] for x in desc_items] == ["US", "US", "JP", "JP"]
    assert [x["symbol"] for x in desc_items] == ["MSFT", "AAPL", "7203", "6479"]


def test_trades_status_sort(client):
    complete = client.post(
        "/api/v1/trades",
        json={
            "market": "JP",
            "symbol": "CMP",
            "tags": "swing",
            "rating": 4,
            "notes_buy": "buy note",
            "notes_sell": "sell note",
            "notes_review": "review note",
            "fills": [
                {"side": "buy", "date": "2026-06-01", "price": 1000, "qty": 1, "fee": 0},
                {"side": "sell", "date": "2026-06-02", "price": 1100, "qty": 1, "fee": 0},
            ],
        },
    )
    assert complete.status_code == 201
    complete_id = complete.json()["id"]
    review_done = client.patch(
        f"/api/v1/trades/{complete_id}",
        json={"review_done": True, "reviewed_at": "2026-06-03"},
    )
    assert review_done.status_code == 200

    pending = client.post(
        "/api/v1/trades",
        json={
            "market": "JP",
            "symbol": "PND",
            "fills": [
                {"side": "buy", "date": "2026-06-01", "price": 1000, "qty": 1, "fee": 0},
                {"side": "sell", "date": "2026-06-02", "price": 900, "qty": 1, "fee": 0},
            ],
        },
    )
    assert pending.status_code == 201

    open_trade = client.post(
        "/api/v1/trades",
        json={
            "market": "US",
            "symbol": "OPN",
            "fills": [
                {"side": "buy", "date": "2026-06-01", "price": 100, "qty": 1, "fee": 0},
            ],
        },
    )
    assert open_trade.status_code == 201

    asc = client.get("/api/v1/trades", params={"sort": "status", "sort_dir": "asc", "limit": 20, "offset": 0})
    assert asc.status_code == 200
    asc_symbols = [x["symbol"] for x in asc.json()["items"]]
    assert asc_symbols == ["CMP", "PND", "OPN"]

    desc = client.get("/api/v1/trades", params={"sort": "status", "sort_dir": "desc", "limit": 20, "offset": 0})
    assert desc.status_code == 200
    desc_symbols = [x["symbol"] for x in desc.json()["items"]]
    assert desc_symbols == ["OPN", "PND", "CMP"]
