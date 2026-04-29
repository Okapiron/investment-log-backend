import base64
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
import time
from typing import Optional
from urllib.error import HTTPError

from sqlalchemy import func, select

from app.core.config import settings
from app.core import analysis as analysis_core
from app.core import price_provider as price_provider_core
from app.core.invites import hash_invite_code
from app.db.models import InviteCode, TradeImportRecord
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
    assert "/api/v1/imports/rakuten-jp/preview" in data["paths"]
    assert "/api/v1/imports/rakuten-jp/audit" in data["paths"]
    assert "/api/v1/imports/{broker}/preview" in data["paths"]
    assert "/api/v1/imports/{broker}/audit" in data["paths"]
    assert "/api/v1/imports/sbi/realized/preview" in data["paths"]
    assert "/api/v1/imports/sbi/realized/commit" in data["paths"]
    assert "/api/v1/imports/sessions/latest" in data["paths"]
    assert "/api/v1/analysis/summary" in data["paths"]


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
        assert "OPS_ALERT_TARGET is empty in auth-enabled mode" in warnings
        assert "DB_BACKUP_STRATEGY is required when AUTH_ENABLED=true" in errors
        assert "CORS_ALLOW_ORIGINS is wildcard in auth-enabled mode" in warnings
    finally:
        settings.auth_enabled = prev_auth_enabled
        settings.supabase_url = prev_supabase_url
        settings.supabase_jwt_secret = prev_supabase_jwt_secret
        settings.ops_alert_target = prev_ops_alert_target
        settings.db_backup_strategy = prev_db_backup_strategy
        settings.cors_allow_origins = prev_cors_allow_origins


def test_runtime_config_requires_private_mode_secret():
    prev_private_mode_enabled = settings.private_mode_enabled
    prev_private_mode_secret = settings.private_mode_secret
    prev_auth_enabled = settings.auth_enabled

    try:
        settings.private_mode_enabled = True
        settings.private_mode_secret = ""
        settings.auth_enabled = False

        errors, warnings = get_runtime_config_issues()
        assert "PRIVATE_MODE_SECRET is required when PRIVATE_MODE_ENABLED=true" in errors
        assert warnings == []
    finally:
        settings.private_mode_enabled = prev_private_mode_enabled
        settings.private_mode_secret = prev_private_mode_secret
        settings.auth_enabled = prev_auth_enabled


def test_runtime_config_public_v1_requires_auth_and_disables_private_mode():
    prev_public_v1_mode = settings.public_v1_mode
    prev_private_mode_enabled = settings.private_mode_enabled
    prev_auth_enabled = settings.auth_enabled
    prev_import_sbi_enabled = settings.import_sbi_enabled
    prev_price_provider = settings.price_provider
    prev_allow_unofficial_price_source = settings.allow_unofficial_price_source

    try:
        settings.public_v1_mode = True
        settings.private_mode_enabled = True
        settings.auth_enabled = False
        settings.import_sbi_enabled = True
        settings.price_provider = "yahoo_unofficial"
        settings.allow_unofficial_price_source = False

        errors, warnings = get_runtime_config_issues()
        assert "PRIVATE_MODE_ENABLED must be false when PUBLIC_V1_MODE=true" in errors
        assert "AUTH_ENABLED must be true when PUBLIC_V1_MODE=true" in errors
        assert "PRICE_PROVIDER cannot be yahoo_unofficial when ALLOW_UNOFFICIAL_PRICE_SOURCE=false" in errors
        assert "IMPORT_SBI_ENABLED is true in PUBLIC_V1_MODE (recommended false or beta-only UI)" in warnings
    finally:
        settings.public_v1_mode = prev_public_v1_mode
        settings.private_mode_enabled = prev_private_mode_enabled
        settings.auth_enabled = prev_auth_enabled
        settings.import_sbi_enabled = prev_import_sbi_enabled
        settings.price_provider = prev_price_provider
        settings.allow_unofficial_price_source = prev_allow_unofficial_price_source


def test_prices_api_returns_503_when_disabled(client):
    prev_price_api_enabled = settings.price_api_enabled
    try:
        settings.price_api_enabled = False
        res = client.get("/api/v1/prices?market=JP&symbol=7203&interval=1d")
        assert res.status_code == 503
        assert res.json()["detail"] == "price api is disabled"
    finally:
        settings.price_api_enabled = prev_price_api_enabled


def test_prices_api_blocks_unofficial_provider_when_disabled(client):
    prev_price_api_enabled = settings.price_api_enabled
    prev_price_provider = settings.price_provider
    prev_allow_unofficial_price_source = settings.allow_unofficial_price_source
    try:
        settings.price_api_enabled = True
        settings.price_provider = "yahoo_unofficial"
        settings.allow_unofficial_price_source = False
        res = client.get("/api/v1/prices?market=JP&symbol=7203&interval=1d")
        assert res.status_code == 503
        assert res.json()["detail"] == "unofficial price source is disabled"
    finally:
        settings.price_api_enabled = prev_price_api_enabled
        settings.price_provider = prev_price_provider
        settings.allow_unofficial_price_source = prev_allow_unofficial_price_source


def test_import_endpoints_reject_sbi_when_disabled(client):
    prev_import_sbi_enabled = settings.import_sbi_enabled
    try:
        settings.import_sbi_enabled = False
        res_preview = client.post(
            "/api/v1/imports/sbi/preview",
            json={"filename": "sbi.csv", "content": "date,symbol,side,qty,price\n"},
        )
        assert res_preview.status_code == 404
        assert res_preview.json()["detail"] == "unsupported broker"

        res_realized_preview = client.post(
            "/api/v1/imports/sbi/realized/preview",
            json={"filename": "sbi_realized.csv", "content": "date,symbol,qty,price,avg_cost,realized_profit\n"},
        )
        assert res_realized_preview.status_code == 404
        assert res_realized_preview.json()["detail"] == "unsupported broker"

        rakuten_preview = client.post(
            "/api/v1/imports/rakuten-jp/preview",
            json={"filename": "rakuten.csv", "content": "約定日,銘柄コード,銘柄,売買,約定数量,約定単価,手数料,取引区分\n"},
        )
        assert rakuten_preview.status_code == 200
    finally:
        settings.import_sbi_enabled = prev_import_sbi_enabled


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


def _create_trade(client, payload: dict, headers: Optional[dict] = None):
    res = client.post("/api/v1/trades", json=payload, headers=headers or {})
    assert res.status_code == 201
    return res


def test_analysis_summary_insufficient_data_returns_stats_only(client):
    _create_trade(
        client,
        {
            "market": "JP",
            "symbol": "AAA1",
            "rating": 3,
            "tags": "順張り,押し目",
            "fills": [
                {"side": "buy", "date": "2026-03-01", "price": 1000, "qty": 1, "fee": 0},
                {"side": "sell", "date": "2026-03-05", "price": 1100, "qty": 1, "fee": 0},
            ],
        },
    )
    _create_trade(
        client,
        {
            "market": "US",
            "symbol": "AAPL",
            "fills": [
                {"side": "buy", "date": "2026-03-10", "price": 10.5, "qty": 2, "fee": 0},
            ],
        },
    )

    res = client.get("/api/v1/analysis/summary")
    assert res.status_code == 200
    body = res.json()
    assert body["headline_summary"] is not None
    assert body["top_improvement"] is not None
    assert body["summary"] is not None
    assert len(body["win_patterns"]) >= 1
    assert len(body["loss_patterns"]) >= 1
    assert len(body["actions"]) >= 1
    assert len(body["diagnoses"]) == 3
    assert body["stats"]["closed_trade_count"] == 1
    assert body["stats"]["open_trade_count"] == 1
    assert body["stats"]["win_trade_count"] == 1
    assert body["stats"]["primary_market"] == "JP"
    assert len(body["stats"]["holding_buckets"]) == 4
    assert len(body["review_gaps"]) >= 1
    assert body["data_sufficiency"]["enough_data"] is False
    assert body["data_sufficiency"]["llm_status"] == "rule_based"


def test_analysis_summary_generates_llm_sections_when_configured(client, monkeypatch):
    prev_key = settings.openai_api_key
    settings.openai_api_key = "test-key"

    for idx in range(5):
        _create_trade(
            client,
            {
                "market": "JP" if idx % 2 == 0 else "US",
                "symbol": f"T{idx}",
                "rating": 4,
                "tags": "順張り,決算",
                "notes_buy": "エントリー理由",
                "notes_sell": "利確理由",
                "notes_review": "振り返りメモ",
                "fills": [
                    {"side": "buy", "date": f"2026-03-0{idx + 1}", "price": 100 + idx, "qty": 1, "fee": 0},
                    {"side": "sell", "date": f"2026-03-1{idx + 1}", "price": 110 + idx, "qty": 1, "fee": 0},
                ],
            },
        )

    def fake_generate(stats, closed, user_key):
        return (
            "全体として利確は安定していますが、同じ理由の再現性確認が必要です。",
            ["勝ちトレードは順張りタグに偏っています。"],
            ["負けパターンはまだ十分に観測されていません。"],
            ["エントリー理由と利確理由の組み合わせを継続記録してください。"],
            user_key,
        )

    monkeypatch.setattr(analysis_core, "_generate_llm_sections", fake_generate)

    try:
        res = client.get("/api/v1/analysis/summary")
        assert res.status_code == 200
        body = res.json()
        assert body["headline_summary"] is not None
        assert body["top_improvement"] is not None
        assert body["summary"].startswith("全体として利確")
        assert body["data_sufficiency"]["enough_data"] is True
        assert body["data_sufficiency"]["llm_status"] == "generated"
        assert len(body["diagnoses"]) == 3
        assert len(body["win_patterns"]) == 1
        assert len(body["actions"]) == 1
    finally:
        settings.openai_api_key = prev_key


def test_analysis_summary_uses_mock_mode_without_openai_key(client):
    prev_mock = settings.analysis_mock_enabled
    settings.analysis_mock_enabled = True

    for idx in range(5):
        _create_trade(
            client,
            {
                "market": "JP",
                "symbol": f"M{idx}",
                "tags": "順張り,反発",
                "fills": [
                    {"side": "buy", "date": f"2026-05-0{idx + 1}", "price": 1000, "qty": 1, "fee": 0},
                    {"side": "sell", "date": f"2026-05-1{idx + 1}", "price": 1050, "qty": 1, "fee": 0},
                ],
            },
        )

    try:
        res = client.get("/api/v1/analysis/summary")
        assert res.status_code == 200
        body = res.json()
        assert body["headline_summary"] is not None
        assert body["top_improvement"] is not None
        assert body["data_sufficiency"]["llm_status"] == "mock"
        assert "テスト用のAI要約" in body["summary"]
        assert len(body["diagnoses"]) == 3
        assert len(body["actions"]) == 3
    finally:
        settings.analysis_mock_enabled = prev_mock


def test_analysis_summary_falls_back_to_stats_when_llm_fails(client, monkeypatch):
    prev_key = settings.openai_api_key
    settings.openai_api_key = "test-key"

    for idx in range(5):
        _create_trade(
            client,
            {
                "market": "JP",
                "symbol": f"F{idx}",
                "fills": [
                    {"side": "buy", "date": f"2026-04-0{idx + 1}", "price": 1000, "qty": 1, "fee": 0},
                    {"side": "sell", "date": f"2026-04-1{idx + 1}", "price": 900, "qty": 1, "fee": 0},
                ],
            },
        )

    def fake_failure(stats, closed, user_key):
        raise RuntimeError("boom")

    monkeypatch.setattr(analysis_core, "_generate_llm_sections", fake_failure)

    try:
        res = client.get("/api/v1/analysis/summary")
        assert res.status_code == 200
        body = res.json()
        assert body["headline_summary"] is not None
        assert body["top_improvement"] is not None
        assert body["summary"] is not None
        assert len(body["diagnoses"]) == 3
        assert len(body["actions"]) >= 1
        assert body["data_sufficiency"]["llm_status"] == "fallback"
        assert body["stats"]["loss_trade_count"] == 5
    finally:
        settings.openai_api_key = prev_key


def test_analysis_summary_uses_rule_based_sections_without_openai(client):
    for idx in range(5):
        _create_trade(
            client,
            {
                "market": "JP",
                "symbol": f"R{idx}",
                "fills": [
                    {"side": "buy", "date": f"2026-06-0{idx + 1}", "price": 1000, "qty": 1, "fee": 0},
                    {"side": "sell", "date": f"2026-06-1{idx + 1}", "price": 980 if idx % 2 else 1100, "qty": 1, "fee": 0},
                ],
            },
        )

    res = client.get("/api/v1/analysis/summary")
    assert res.status_code == 200
    body = res.json()
    assert body["headline_summary"] is not None
    assert body["top_improvement"] is not None
    assert body["data_sufficiency"]["llm_status"] == "rule_based"
    assert body["summary"] is not None
    assert len(body["diagnoses"]) == 3
    assert len(body["win_patterns"]) >= 1
    assert len(body["loss_patterns"]) >= 1
    assert len(body["actions"]) >= 1
    assert body["stats"]["avg_win_profit_amount"] is not None
    assert body["stats"]["avg_loss_amount"] is not None
    assert body["stats"]["recent_closed_trade_count"] == 5
    assert len(body["stats"]["holding_buckets"]) == 4


def test_analysis_summary_selects_top_improvement_for_heaviest_losses(client):
    for idx in range(3):
        _create_trade(
            client,
            {
                "market": "JP",
                "symbol": f"LW{idx}",
                "fills": [
                    {"side": "buy", "date": f"2026-07-0{idx + 1}", "price": 1000, "qty": 1, "fee": 0},
                    {"side": "sell", "date": f"2026-07-1{idx + 1}", "price": 1100, "qty": 1, "fee": 0},
                ],
            },
        )
    for idx in range(3):
        _create_trade(
            client,
            {
                "market": "JP",
                "symbol": f"LL{idx}",
                "fills": [
                    {"side": "buy", "date": f"2026-08-0{idx + 1}", "price": 1000, "qty": 1, "fee": 0},
                    {"side": "sell", "date": f"2026-08-1{idx + 1}", "price": 200, "qty": 1, "fee": 0},
                ],
            },
        )

    res = client.get("/api/v1/analysis/summary")
    assert res.status_code == 200
    body = res.json()
    assert body["top_improvement"]["key"] == "pnl_structure"
    assert "大きな負け" in body["top_improvement"]["message"]


def test_analysis_summary_selects_top_improvement_for_holding_distortion(client):
    _create_trade(
        client,
        {
            "market": "JP",
            "symbol": "HW1",
            "fills": [
                {"side": "buy", "date": "2026-09-01", "price": 1000, "qty": 1, "fee": 0},
                {"side": "sell", "date": "2026-09-03", "price": 1200, "qty": 1, "fee": 0},
            ],
        },
    )
    _create_trade(
        client,
        {
            "market": "JP",
            "symbol": "HL1",
            "fills": [
                {"side": "buy", "date": "2026-09-01", "price": 1000, "qty": 1, "fee": 0},
                {"side": "sell", "date": "2026-10-10", "price": 700, "qty": 1, "fee": 0},
            ],
        },
    )
    _create_trade(
        client,
        {
            "market": "JP",
            "symbol": "HL2",
            "fills": [
                {"side": "buy", "date": "2026-09-02", "price": 1000, "qty": 1, "fee": 0},
                {"side": "sell", "date": "2026-10-15", "price": 650, "qty": 1, "fee": 0},
            ],
        },
    )
    _create_trade(
        client,
        {
            "market": "JP",
            "symbol": "HL3",
            "fills": [
                {"side": "buy", "date": "2026-09-03", "price": 1000, "qty": 1, "fee": 0},
                {"side": "sell", "date": "2026-10-20", "price": 680, "qty": 1, "fee": 0},
            ],
        },
    )
    _create_trade(
        client,
        {
            "market": "JP",
            "symbol": "HW2",
            "fills": [
                {"side": "buy", "date": "2026-09-04", "price": 1000, "qty": 1, "fee": 0},
                {"side": "sell", "date": "2026-09-06", "price": 1180, "qty": 1, "fee": 0},
            ],
        },
    )

    res = client.get("/api/v1/analysis/summary")
    assert res.status_code == 200
    body = res.json()
    assert body["top_improvement"]["key"] == "holding_execution"
    assert "保有" in body["headline_summary"]


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


def _rewrite_import_record_lineage(trade_id: int, *, signature: str, position_key: str, lot_sequence: int) -> None:
    session_local = app.state.testing_session_local
    with session_local() as db:
        record = db.scalar(select(TradeImportRecord).where(TradeImportRecord.trade_id == trade_id))
        assert record is not None
        record.source_signature = signature
        record.source_position_key = position_key
        record.source_lot_sequence = lot_sequence
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
    assert "OPS_ALERT_TARGET is empty in auth-enabled mode" in payload["config_warnings"]
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
            "market": "JP",
            "symbol": "7203",
            "fills": [
                {"side": "buy", "date": "2026-04-01", "price": 1000, "qty": 5, "fee": 0},
            ],
        },
    )
    assert open_created.status_code == 201
    open_id = open_created.json()["id"]

    partial = client.patch(
        f"/api/v1/trades/{open_id}",
        json={
            "buy_date": "2026-04-01",
            "buy_price": 1000,
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


def test_us_trade_detail_is_available(client):
    created = client.post(
        "/api/v1/trades",
        json={
            "market": "US",
            "symbol": "AAPL",
            "fills": [
                {"side": "buy", "date": "2026-06-01", "price": 100, "qty": 1, "fee": 0},
            ],
        },
    )
    assert created.status_code == 201
    trade_id = created.json()["id"]

    detail = client.get(f"/api/v1/trades/{trade_id}")
    assert detail.status_code == 200
    assert detail.json()["market"] == "US"

def test_private_mode_blocks_api_without_secret(client):
    prev_enabled = settings.private_mode_enabled
    prev_secret = settings.private_mode_secret

    try:
        settings.private_mode_enabled = True
        settings.private_mode_secret = "test-secret"

        blocked = client.get("/api/v1/trades")
        assert blocked.status_code == 403
        assert blocked.json()["detail"] == "private access required"

        allowed = client.get("/api/v1/trades", headers={"X-TradeTrace-Secret": "test-secret"})
        assert allowed.status_code == 200

        blocked_prices = client.get("/api/v1/prices", params={"market": "US", "symbol": "AAPL"})
        assert blocked_prices.status_code == 403
    finally:
        settings.private_mode_enabled = prev_enabled
        settings.private_mode_secret = prev_secret


def test_private_mode_delete_all_data_clears_trades_and_import_records(client):
    prev_enabled = settings.private_mode_enabled
    prev_secret = settings.private_mode_secret

    try:
        settings.private_mode_enabled = True
        settings.private_mode_secret = "test-secret"
        headers = {"X-TradeTrace-Secret": "test-secret"}

        created = client.post(
            "/api/v1/trades",
            json={
                "market": "JP",
                "symbol": "7203",
                "fills": [
                    {"side": "buy", "date": "2026-03-01", "price": 2500, "qty": 1, "fee": 0},
                    {"side": "sell", "date": "2026-03-10", "price": 2600, "qty": 1, "fee": 0},
                ],
            },
            headers=headers,
        )
        assert created.status_code == 201
        trade_id = created.json()["id"]

        session_local = app.state.testing_session_local
        with session_local() as db:
            db.add(
                TradeImportRecord(
                    broker="rakuten",
                    source_name="tradehistory.csv",
                    source_signature="sig-1",
                    source_position_key="pos-1",
                    source_lot_sequence=1,
                    import_state="closed_round_trip",
                    is_partial_exit=False,
                    trade_id=trade_id,
                )
            )
            db.add(
                TradeImportRecord(
                    broker="rakuten",
                    source_name="tradehistory.csv",
                    source_signature="sig-orphan",
                    source_position_key="pos-orphan",
                    source_lot_sequence=2,
                    import_state="open_remaining",
                    is_partial_exit=True,
                    trade_id=None,
                )
            )
            db.commit()

        deleted = client.delete(
            "/api/v1/settings/me",
            params={"confirm": "true", "confirm_text": "DELETE"},
            headers=headers,
        )
        assert deleted.status_code == 200
        body = deleted.json()
        assert body["deleted_trades"] == 1
        assert body["deleted_import_records"] == 2
        assert body["anonymized_invites"] == 0

        list_after = client.get("/api/v1/trades", headers=headers)
        assert list_after.status_code == 200
        assert list_after.json()["total"] == 0

        with session_local() as db:
            remaining = db.scalar(select(func.count()).select_from(TradeImportRecord))
            assert int(remaining or 0) == 0
    finally:
        settings.private_mode_enabled = prev_enabled
        settings.private_mode_secret = prev_secret


def test_rakuten_import_preview_and_commit_create_trade(client):
    csv_content = """約定日,銘柄コード,銘柄,売買,約定数量,約定単価,手数料,取引区分
2026/03/01,7203,トヨタ自動車,買,100,2500,275,現物
2026/03/10,7203,トヨタ自動車,売,100,2600,275,現物
"""

    preview = client.post(
        "/api/v1/imports/rakuten-jp/preview",
        json={"filename": "rakuten.csv", "content": csv_content},
    )
    assert preview.status_code == 200
    body = preview.json()
    assert body["candidate_count"] == 1
    assert body["error_count"] == 0
    assert body["candidates"][0]["symbol"] == "7203"
    assert body["candidates"][0]["buy"]["qty"] == 100

    commit = client.post(
        "/api/v1/imports/rakuten-jp/commit",
        json={"filename": "rakuten.csv", "items": body["candidates"]},
    )
    assert commit.status_code == 200
    commit_body = commit.json()
    assert commit_body["created_count"] == 1
    assert commit_body["error_count"] == 0

    trades = client.get("/api/v1/trades")
    assert trades.status_code == 200
    assert trades.json()["total"] == 1
    assert trades.json()["items"][0]["symbol"] == "7203"


def test_rakuten_import_commit_reimports_same_csv_as_update_and_preserves_user_fields(client):
    csv_content = """約定日,銘柄コード,銘柄,売買,約定数量,約定単価,手数料,取引区分
2026/03/01,6758,ソニーグループ,買,100,3000,275,現物
2026/03/15,6758,ソニーグループ,売,100,3200,275,現物
"""

    preview = client.post(
        "/api/v1/imports/rakuten-jp/preview",
        json={"filename": "rakuten.csv", "content": csv_content},
    )
    items = preview.json()["candidates"]

    first = client.post("/api/v1/imports/rakuten-jp/commit", json={"filename": "rakuten.csv", "items": items})
    assert first.status_code == 200
    assert first.json()["created_count"] == 1

    trade_id = first.json()["created_trade_ids"][0]
    edited = client.patch(
        f"/api/v1/trades/{trade_id}",
        json={
            "notes_buy": "長期テーマでエントリー",
            "notes_sell": "イベント前に利確",
            "notes_review": "次回は分割利確も検討",
            "tags": "成長期待,成功",
            "rating": 5,
            "chart_image_url": "https://example.com/chart.png",
        },
    )
    assert edited.status_code == 200
    review_done = client.patch(
        f"/api/v1/trades/{trade_id}",
        json={"review_done": True, "reviewed_at": "2026-03-20"},
    )
    assert review_done.status_code == 200
    assert review_done.json()["review_done"] is True

    second = client.post("/api/v1/imports/rakuten-jp/commit", json={"filename": "rakuten.csv", "items": items})
    assert second.status_code == 200
    second_body = second.json()
    assert second_body["created_count"] == 0
    assert second_body["updated_count"] == 1
    assert second_body["skipped_count"] == 0

    detail = client.get(f"/api/v1/trades/{trade_id}")
    assert detail.status_code == 200
    body = detail.json()
    assert body["notes_buy"] == "長期テーマでエントリー"
    assert body["notes_sell"] == "イベント前に利確"
    assert body["notes_review"] == "次回は分割利確も検討"
    assert body["tags"] == "成長期待,成功"
    assert body["rating"] == 5
    assert body["review_done"] is True
    assert body["reviewed_at"] == "2026-03-20"
    assert body["chart_image_url"] == "https://example.com/chart.png"


def test_rakuten_import_preview_and_commit_support_partial_exit(client):
    csv_content = """約定日,銘柄コード,銘柄,売買,約定数量,約定単価,手数料,取引区分
2026/03/01,9432,ＮＴＴ,買,200,150,0,現物
2026/03/05,9432,ＮＴＴ,売,100,160,0,現物
"""

    preview = client.post(
        "/api/v1/imports/rakuten-jp/preview",
        json={"filename": "rakuten.csv", "content": csv_content},
    )
    assert preview.status_code == 200
    body = preview.json()
    assert body["candidate_count"] == 2
    assert body["error_count"] == 0
    assert body["skipped_count"] == 0

    closed = next(item for item in body["candidates"] if item["sell"] is not None)
    open_item = next(item for item in body["candidates"] if item["sell"] is None)
    assert closed["buy"]["qty"] == 100
    assert closed["sell"]["qty"] == 100
    assert closed["is_partial_exit"] is True
    assert closed["remaining_qty_after_sell"] == 100
    assert open_item["buy"]["qty"] == 100
    assert open_item["is_partial_exit"] is True

    commit = client.post(
        "/api/v1/imports/rakuten-jp/commit",
        json={"filename": "rakuten.csv", "items": body["candidates"]},
    )
    assert commit.status_code == 200
    commit_body = commit.json()
    assert commit_body["created_count"] == 2
    assert commit_body["updated_count"] == 0
    assert commit_body["error_count"] == 0

    trades = client.get("/api/v1/trades")
    assert trades.status_code == 200
    items = trades.json()["items"]
    assert len(items) == 2
    assert sum(1 for item in items if item["is_open"]) == 1
    assert sum(1 for item in items if item["is_partial_exit"]) == 2


def test_rakuten_import_reimport_closes_existing_open_remaining(client):
    first_csv = """約定日,銘柄コード,銘柄,売買,約定数量,約定単価,手数料,取引区分
2026/03/01,9432,ＮＴＴ,買,200,150,0,現物
2026/03/05,9432,ＮＴＴ,売,100,160,0,現物
"""
    wider_csv = """約定日,銘柄コード,銘柄,売買,約定数量,約定単価,手数料,取引区分
2026/03/01,9432,ＮＴＴ,買,200,150,0,現物
2026/03/05,9432,ＮＴＴ,売,100,160,0,現物
2026/03/20,9432,ＮＴＴ,売,100,170,0,現物
"""

    preview_first = client.post(
        "/api/v1/imports/rakuten-jp/preview",
        json={"filename": "rakuten_partial.csv", "content": first_csv},
    )
    assert preview_first.status_code == 200
    commit_first = client.post(
        "/api/v1/imports/rakuten-jp/commit",
        json={"filename": "rakuten_partial.csv", "items": preview_first.json()["candidates"]},
    )
    assert commit_first.status_code == 200
    assert commit_first.json()["created_count"] == 2

    preview_wider = client.post(
        "/api/v1/imports/rakuten-jp/preview",
        json={"filename": "rakuten_wider.csv", "content": wider_csv},
    )
    assert preview_wider.status_code == 200
    wider_body = preview_wider.json()
    assert wider_body["candidate_count"] == 2
    assert sum(1 for item in wider_body["candidates"] if item["already_imported"]) == 2

    commit_wider = client.post(
        "/api/v1/imports/rakuten-jp/commit",
        json={"filename": "rakuten_wider.csv", "items": wider_body["candidates"]},
    )
    assert commit_wider.status_code == 200
    commit_wider_body = commit_wider.json()
    assert commit_wider_body["created_count"] == 0
    assert commit_wider_body["updated_count"] == 2
    assert commit_wider_body["skipped_count"] == 0
    assert commit_wider_body["error_count"] == 0

    trades = client.get("/api/v1/trades")
    assert trades.status_code == 200
    items = trades.json()["items"]
    assert len(items) == 2
    assert all(item["is_open"] is False for item in items)
    assert sorted((item["closed_at"] for item in items if item["closed_at"])) == ["2026-03-05", "2026-03-20"]


def test_rakuten_import_fallback_distinguishes_same_day_same_qty_trades_by_price(client):
    items = [
        {
            "source_signature": "price-sig-1",
            "source_position_key": "price-pos-1",
            "source_lot_sequence": 1,
            "symbol": "7203",
            "name": "トヨタ自動車",
            "market": "JP",
            "position_side": "long",
            "buy": {"date": "2026-03-01", "price": 2500, "qty": 100, "fee": 100, "fee_total_jpy": 100},
            "sell": {"date": "2026-03-10", "price": 2600, "qty": 100, "fee": 100, "fee_total_jpy": 100},
            "source_lines": [1, 3],
            "already_imported": False,
            "is_partial_exit": False,
            "remaining_qty_after_sell": 0,
        },
        {
            "source_signature": "price-sig-2",
            "source_position_key": "price-pos-2",
            "source_lot_sequence": 1,
            "symbol": "7203",
            "name": "トヨタ自動車",
            "market": "JP",
            "position_side": "long",
            "buy": {"date": "2026-03-01", "price": 2520, "qty": 100, "fee": 100, "fee_total_jpy": 100},
            "sell": {"date": "2026-03-10", "price": 2620, "qty": 100, "fee": 100, "fee_total_jpy": 100},
            "source_lines": [2, 4],
            "already_imported": False,
            "is_partial_exit": False,
            "remaining_qty_after_sell": 0,
        },
    ]

    first_commit = client.post("/api/v1/imports/rakuten-jp/commit", json={"filename": "same_day_price.csv", "items": items})
    assert first_commit.status_code == 200
    trade_ids = first_commit.json()["created_trade_ids"]
    assert len(trade_ids) == 2

    first_trade_id, second_trade_id = trade_ids
    assert client.patch(f"/api/v1/trades/{first_trade_id}", json={"notes_buy": "first-lot"}).status_code == 200
    assert client.patch(f"/api/v1/trades/{second_trade_id}", json={"notes_buy": "second-lot"}).status_code == 200

    _rewrite_import_record_lineage(first_trade_id, signature="legacy-price-1", position_key="legacy-pos-price-1", lot_sequence=101)
    _rewrite_import_record_lineage(second_trade_id, signature="legacy-price-2", position_key="legacy-pos-price-2", lot_sequence=102)

    second_commit = client.post(
        "/api/v1/imports/rakuten-jp/commit",
        json={"filename": "same_day_price.csv", "items": items},
    )
    assert second_commit.status_code == 200
    assert second_commit.json()["created_count"] == 0
    assert second_commit.json()["updated_count"] == 2

    first_detail = client.get(f"/api/v1/trades/{first_trade_id}")
    second_detail = client.get(f"/api/v1/trades/{second_trade_id}")
    assert first_detail.status_code == 200
    assert second_detail.status_code == 200

    first_body = first_detail.json()
    second_body = second_detail.json()
    first_buy = next(fill for fill in first_body["fills"] if fill["side"] == "buy")
    first_sell = next(fill for fill in first_body["fills"] if fill["side"] == "sell")
    second_buy = next(fill for fill in second_body["fills"] if fill["side"] == "buy")
    second_sell = next(fill for fill in second_body["fills"] if fill["side"] == "sell")

    assert first_body["notes_buy"] == "first-lot"
    assert first_buy["price"] == 2500.0
    assert first_sell["price"] == 2600.0
    assert second_body["notes_buy"] == "second-lot"
    assert second_buy["price"] == 2520.0
    assert second_sell["price"] == 2620.0


def test_rakuten_import_fallback_distinguishes_same_day_same_price_trades_by_fee(client):
    items = [
        {
            "source_signature": "fee-sig-1",
            "source_position_key": "fee-pos-1",
            "source_lot_sequence": 1,
            "symbol": "7203",
            "name": "トヨタ自動車",
            "market": "JP",
            "position_side": "long",
            "buy": {"date": "2026-03-01", "price": 2500, "qty": 100, "fee": 100, "fee_total_jpy": 100},
            "sell": {"date": "2026-03-10", "price": 2600, "qty": 100, "fee": 100, "fee_total_jpy": 100},
            "source_lines": [1, 3],
            "already_imported": False,
            "is_partial_exit": False,
            "remaining_qty_after_sell": 0,
        },
        {
            "source_signature": "fee-sig-2",
            "source_position_key": "fee-pos-2",
            "source_lot_sequence": 1,
            "symbol": "7203",
            "name": "トヨタ自動車",
            "market": "JP",
            "position_side": "long",
            "buy": {"date": "2026-03-01", "price": 2500, "qty": 100, "fee": 200, "fee_total_jpy": 200},
            "sell": {"date": "2026-03-10", "price": 2600, "qty": 100, "fee": 200, "fee_total_jpy": 200},
            "source_lines": [2, 4],
            "already_imported": False,
            "is_partial_exit": False,
            "remaining_qty_after_sell": 0,
        },
    ]

    first_commit = client.post("/api/v1/imports/rakuten-jp/commit", json={"filename": "same_day_fee.csv", "items": items})
    assert first_commit.status_code == 200
    trade_ids = first_commit.json()["created_trade_ids"]
    assert len(trade_ids) == 2

    first_trade_id, second_trade_id = trade_ids
    assert client.patch(f"/api/v1/trades/{first_trade_id}", json={"notes_buy": "fee-100"}).status_code == 200
    assert client.patch(f"/api/v1/trades/{second_trade_id}", json={"notes_buy": "fee-200"}).status_code == 200

    _rewrite_import_record_lineage(first_trade_id, signature="legacy-fee-1", position_key="legacy-pos-fee-1", lot_sequence=201)
    _rewrite_import_record_lineage(second_trade_id, signature="legacy-fee-2", position_key="legacy-pos-fee-2", lot_sequence=202)

    second_commit = client.post(
        "/api/v1/imports/rakuten-jp/commit",
        json={"filename": "same_day_fee.csv", "items": items},
    )
    assert second_commit.status_code == 200
    assert second_commit.json()["created_count"] == 0
    assert second_commit.json()["updated_count"] == 2

    first_detail = client.get(f"/api/v1/trades/{first_trade_id}")
    second_detail = client.get(f"/api/v1/trades/{second_trade_id}")
    assert first_detail.status_code == 200
    assert second_detail.status_code == 200

    first_body = first_detail.json()
    second_body = second_detail.json()
    first_buy = next(fill for fill in first_body["fills"] if fill["side"] == "buy")
    first_sell = next(fill for fill in first_body["fills"] if fill["side"] == "sell")
    second_buy = next(fill for fill in second_body["fills"] if fill["side"] == "buy")
    second_sell = next(fill for fill in second_body["fills"] if fill["side"] == "sell")

    assert first_body["notes_buy"] == "fee-100"
    assert first_buy["fee_total_jpy"] == 100
    assert first_sell["fee_total_jpy"] == 100
    assert second_body["notes_buy"] == "fee-200"
    assert second_buy["fee_total_jpy"] == 200
    assert second_sell["fee_total_jpy"] == 200


def test_rakuten_import_open_remaining_fallback_matches_existing_trade_by_open_price_and_fee(client):
    items = [
        {
            "source_signature": "open-sig-1",
            "source_position_key": "open-pos-1",
            "source_lot_sequence": 1,
            "symbol": "7203",
            "name": "トヨタ自動車",
            "market": "JP",
            "position_side": "long",
            "buy": {"date": "2026-03-01", "price": 2500, "qty": 100, "fee": 175, "fee_total_jpy": 175},
            "sell": None,
            "source_lines": [1],
            "already_imported": False,
            "is_partial_exit": False,
            "remaining_qty_after_sell": 100,
        }
    ]

    first_commit = client.post("/api/v1/imports/rakuten-jp/commit", json={"filename": "open_remaining.csv", "items": items})
    assert first_commit.status_code == 200
    trade_id = first_commit.json()["created_trade_ids"][0]

    assert client.patch(f"/api/v1/trades/{trade_id}", json={"notes_buy": "holding-note"}).status_code == 200
    _rewrite_import_record_lineage(trade_id, signature="legacy-open-1", position_key="legacy-pos-open-1", lot_sequence=301)

    second_commit = client.post(
        "/api/v1/imports/rakuten-jp/commit",
        json={"filename": "open_remaining.csv", "items": items},
    )
    assert second_commit.status_code == 200
    assert second_commit.json()["created_count"] == 0
    assert second_commit.json()["updated_count"] == 1

    detail = client.get(f"/api/v1/trades/{trade_id}")
    assert detail.status_code == 200
    body = detail.json()
    buy_fill = next(fill for fill in body["fills"] if fill["side"] == "buy")
    assert body["notes_buy"] == "holding-note"
    assert body["is_open"] is True
    assert body["closed_at"] in ("", None)
    assert buy_fill["price"] == 2500.0
    assert buy_fill["fee_total_jpy"] == 175


def test_rakuten_import_commit_does_not_collapse_identical_fallback_shape_sibling_lots(client):
    items = [
        {
            "source_signature": "same-shape-sig-1",
            "source_position_key": "same-shape-pos",
            "source_lot_sequence": 1,
            "symbol": "7014",
            "name": "名村造船所",
            "market": "JP",
            "position_side": "long",
            "buy": {"date": "2024-06-03", "price": 2411.6, "qty": 100, "fee": 0, "fee_total_jpy": 0},
            "sell": {"date": "2024-06-04", "price": 2500, "qty": 100, "fee": 0, "fee_total_jpy": 0},
            "source_lines": [1, 2],
            "already_imported": False,
            "is_partial_exit": True,
            "remaining_qty_after_sell": 200,
        },
        {
            "source_signature": "same-shape-sig-2",
            "source_position_key": "same-shape-pos",
            "source_lot_sequence": 2,
            "symbol": "7014",
            "name": "名村造船所",
            "market": "JP",
            "position_side": "long",
            "buy": {"date": "2024-06-03", "price": 2411.6, "qty": 100, "fee": 0, "fee_total_jpy": 0},
            "sell": {"date": "2024-06-04", "price": 2500, "qty": 100, "fee": 0, "fee_total_jpy": 0},
            "source_lines": [3, 4],
            "already_imported": False,
            "is_partial_exit": True,
            "remaining_qty_after_sell": 100,
        },
    ]

    first_commit = client.post(
        "/api/v1/imports/rakuten-jp/commit",
        json={"filename": "same_shape.csv", "items": items},
    )
    assert first_commit.status_code == 200
    first_body = first_commit.json()
    assert first_body["created_count"] == 2
    assert first_body["updated_count"] == 0
    assert first_body["error_count"] == 0

    trades = client.get("/api/v1/trades")
    assert trades.status_code == 200
    assert trades.json()["total"] == 2

    second_commit = client.post(
        "/api/v1/imports/rakuten-jp/commit",
        json={"filename": "same_shape.csv", "items": items},
    )
    assert second_commit.status_code == 200
    second_body = second_commit.json()
    assert second_body["created_count"] == 0
    assert second_body["updated_count"] == 2
    assert second_body["error_count"] == 0


def test_rakuten_import_commit_with_wider_csv_does_not_delete_unmatched_existing_import_trade(client):
    older_csv = """約定日,銘柄コード,銘柄,売買,約定数量,約定単価,手数料,取引区分
2026/02/01,9984,ソフトバンクグループ,買,100,8000,0,現物
2026/02/10,9984,ソフトバンクグループ,売,100,8200,0,現物
"""
    newer_csv = """約定日,銘柄コード,銘柄,売買,約定数量,約定単価,手数料,取引区分
2026/03/01,6758,ソニーグループ,買,100,3000,0,現物
2026/03/10,6758,ソニーグループ,売,100,3200,0,現物
"""

    preview_old = client.post("/api/v1/imports/rakuten-jp/preview", json={"filename": "older.csv", "content": older_csv})
    assert preview_old.status_code == 200
    commit_old = client.post(
        "/api/v1/imports/rakuten-jp/commit",
        json={"filename": "older.csv", "items": preview_old.json()["candidates"]},
    )
    assert commit_old.status_code == 200
    assert commit_old.json()["created_count"] == 1

    preview_new = client.post("/api/v1/imports/rakuten-jp/preview", json={"filename": "newer.csv", "content": newer_csv})
    assert preview_new.status_code == 200
    commit_new = client.post(
        "/api/v1/imports/rakuten-jp/commit",
        json={"filename": "newer.csv", "items": preview_new.json()["candidates"]},
    )
    assert commit_new.status_code == 200
    assert commit_new.json()["created_count"] == 1

    trades = client.get("/api/v1/trades")
    assert trades.status_code == 200
    body = trades.json()
    assert body["total"] == 2
    assert sorted(item["symbol"] for item in body["items"]) == ["6758", "9984"]


def test_rakuten_import_manual_trade_is_not_upsert_target(client):
    manual = client.post(
        "/api/v1/trades",
        json={
            "market": "JP",
            "symbol": "7203",
            "fills": [
                {"side": "buy", "date": "2026-03-01", "price": 2500, "qty": 100, "fee": 0},
                {"side": "sell", "date": "2026-03-10", "price": 2600, "qty": 100, "fee": 0},
            ],
        },
    )
    assert manual.status_code == 201

    csv_content = """約定日,銘柄コード,銘柄,売買,約定数量,約定単価,手数料,取引区分
2026/03/01,7203,トヨタ自動車,買,100,2500,0,現物
2026/03/10,7203,トヨタ自動車,売,100,2600,0,現物
"""
    preview = client.post("/api/v1/imports/rakuten-jp/preview", json={"filename": "rakuten.csv", "content": csv_content})
    assert preview.status_code == 200
    assert preview.json()["candidates"][0]["already_imported"] is False

    commit = client.post(
        "/api/v1/imports/rakuten-jp/commit",
        json={"filename": "rakuten.csv", "items": preview.json()["candidates"]},
    )
    assert commit.status_code == 200
    assert commit.json()["created_count"] == 1
    assert commit.json()["updated_count"] == 0

    trades = client.get("/api/v1/trades")
    assert trades.status_code == 200
    assert trades.json()["total"] == 2


def test_rakuten_import_preview_supports_two_closed_trades_from_split_exit(client):
    csv_content = """約定日,銘柄コード,銘柄,売買,約定数量,約定単価,手数料,取引区分
2026/03/01,9432,ＮＴＴ,買,200,150,0,現物
2026/03/05,9432,ＮＴＴ,売,100,160,0,現物
2026/03/20,9432,ＮＴＴ,売,100,170,0,現物
"""

    preview = client.post(
        "/api/v1/imports/rakuten-jp/preview",
        json={"filename": "rakuten.csv", "content": csv_content},
    )
    assert preview.status_code == 200
    body = preview.json()
    assert body["candidate_count"] == 2
    assert body["error_count"] == 0
    assert all(item["sell"] is not None for item in body["candidates"])
    assert [item["sell"]["date"] for item in body["candidates"]] == ["2026-03-05", "2026-03-20"]


def test_rakuten_import_preview_and_commit_support_margin_long(client):
    csv_content = """約定日,銘柄コード,銘柄,売買,約定数量,約定単価,手数料,取引区分
2026/03/01,7203,トヨタ自動車,買,100,2500,275,信用新規買
2026/03/10,7203,トヨタ自動車,売,100,2600,275,信用返済売
"""

    preview = client.post(
        "/api/v1/imports/rakuten-jp/preview",
        json={"filename": "rakuten_margin_long.csv", "content": csv_content},
    )
    assert preview.status_code == 200
    body = preview.json()
    assert body["candidate_count"] == 1
    assert body["skipped_count"] == 0
    assert body["error_count"] == 0
    assert body["candidates"][0]["symbol"] == "7203"
    assert body["candidates"][0]["buy"]["price"] == 2500
    assert body["candidates"][0]["sell"]["price"] == 2600

    commit = client.post(
        "/api/v1/imports/rakuten-jp/commit",
        json={"filename": "rakuten_margin_long.csv", "items": body["candidates"]},
    )
    assert commit.status_code == 200
    commit_body = commit.json()
    assert commit_body["created_count"] == 1
    assert commit_body["error_count"] == 0


def test_rakuten_import_preview_supports_split_credit_columns(client):
    csv_content = """約定日,銘柄コード,銘柄,取引,売買,信用区分,約定数量,約定単価,手数料
2026/03/01,7203,トヨタ自動車,信用,買,新規,100,2500,275
2026/03/10,7203,トヨタ自動車,信用,売,返済,100,2600,275
"""

    preview = client.post(
        "/api/v1/imports/rakuten-jp/preview",
        json={"filename": "rakuten_margin_split_columns.csv", "content": csv_content},
    )
    assert preview.status_code == 200
    body = preview.json()
    assert body["candidate_count"] == 1
    assert body["skipped_count"] == 0
    assert body["error_count"] == 0
    assert body["candidates"][0]["symbol"] == "7203"


def test_rakuten_import_preview_supports_actual_rakuten_header_labels(client):
    csv_content = """約定日,受渡日,銘柄コード,銘柄名,市場名称,口座区分,取引区分,売買区分,信用区分,弁済期限,数量［株］,単価［円］,手数料［円］,税金等［円］,諸費用［円］,税区分,受渡金額［円］,建約定日,建単価［円］,建手数料［円］,建手数料消費税［円］,金利（支払）〔円〕,金利（受取）〔円〕,逆日歩／特別空売り料（支払）〔円〕,逆日歩（受取）〔円〕,貸株料,事務管理費〔円〕（税抜）,名義書換料〔円〕（税抜）
2026/2/9,2026/2/12,5801,古河電工,JAX,特定,信用新規,買建,一般,無期限,100,15500.0,0,0,0,-,-,-,0.0,0,0,0,0,0,0,0,0,0
2026/2/10,2026/2/13,5801,古河電工,東証,特定,信用返済,売埋,一般,無期限,100,21400.0,0,0,237,源徴あり,589763,2026/2/9,15500.0,0,0,237,0,0,0,0,0,0
"""

    preview = client.post(
        "/api/v1/imports/rakuten-jp/preview",
        json={"filename": "rakuten_actual_headers.csv", "content": csv_content},
    )
    assert preview.status_code == 200
    body = preview.json()
    assert body["candidate_count"] == 1
    assert body["skipped_count"] == 0
    assert body["error_count"] == 0
    assert body["candidates"][0]["symbol"] == "5801"
    assert body["candidates"][0]["sell"]["fee"] == 237


def test_rakuten_import_preview_credit_close_uses_build_info_without_buy_row(client):
    csv_content = """約定日,受渡日,銘柄コード,銘柄名,市場名称,口座区分,取引区分,売買区分,信用区分,弁済期限,数量［株］,単価［円］,手数料［円］,税金等［円］,諸費用［円］,税区分,受渡金額［円］,建約定日,建単価［円］,建手数料［円］,建手数料消費税［円］
2026/3/18,2026/3/21,2644,ＧＸ半導体日株,東証,特定,信用返済,売埋,一般,無期限,30,3213.0,0,100,1418,源徴あり,-858,2026/1/22,3191.0,0,0
"""

    preview = client.post(
        "/api/v1/imports/rakuten-jp/preview",
        json={"filename": "rakuten_credit_close_only.csv", "content": csv_content},
    )
    assert preview.status_code == 200
    body = preview.json()
    assert body["candidate_count"] == 1
    assert body["skipped_count"] == 0
    assert body["error_count"] == 0
    item = body["candidates"][0]
    assert item["symbol"] == "2644"
    assert item["buy"]["date"] == "2026-01-22"
    assert item["buy"]["price"] == 3191
    assert item["sell"]["price"] == 3213
    assert item["sell"]["fee"] == 1518


def test_rakuten_import_audit_matches_realized_pl(client):
    tradehistory_csv = """約定日,受渡日,銘柄コード,銘柄名,市場名称,口座区分,取引区分,売買区分,信用区分,弁済期限,数量［株］,単価［円］,手数料［円］,税金等［円］,諸費用［円］,税区分,受渡金額［円］,建約定日,建単価［円］,建手数料［円］,建手数料消費税［円］
2026/3/18,2026/3/21,2644,ＧＸ半導体日株,東証,特定,信用返済,売埋,一般,無期限,30,3213.0,0,100,1418,源徴あり,-858,2026/1/22,3191.0,0,0
"""
    realized_csv = """約定日,受渡日,銘柄コード,銘柄名,口座,信用区分,取引,数量[株],売却/決済単価[円],売却/決済額[円],平均取得価額[円],実現損益[円]
2026/3/18,2026/3/21,2644,ＧＸ半導体日株,特定,一般,売埋,30,3213.0,96390,3191.00,-858
"""

    audit = client.post(
        "/api/v1/imports/rakuten-jp/audit",
        json={
            "tradehistory_filename": "tradehistory.csv",
            "tradehistory_content": tradehistory_csv,
            "realized_filename": "realized.csv",
            "realized_content": realized_csv,
        },
    )
    assert audit.status_code == 200
    body = audit.json()
    assert body["matched_count"] == 1
    assert body["gap_jpy"] == 0
    assert body["missing_in_tt"] == []
    assert body["pnl_mismatch"] == []
    assert body["unmatched_tt"] == []


def test_rakuten_import_audit_rounds_spot_average_cost_to_broker_basis(client):
    tradehistory_csv = """約定日,受渡日,銘柄コード,銘柄名,市場名称,口座区分,取引区分,売買区分,信用区分,弁済期限,数量［株］,単価［円］,手数料［円］,税金等［円］,諸費用［円］,税区分,受渡金額［円］,建約定日,建単価［円］,建手数料［円］,建手数料消費税［円］
2023/10/13,2023/10/17,6227,ＡＩメカテック,東証,特定,現物,買付,-,-,100,3755.0,0,0,0,一般,375500,-,0.0,0,0
2023/10/13,2023/10/17,6227,ＡＩメカテック,東証,特定,現物,売付,-,-,100,3730.0,0,0,0,一般,373000,-,0.0,0,0
2023/10/17,2023/10/19,6227,ＡＩメカテック,東証,特定,現物,買付,-,-,100,3995.0,0,0,0,一般,399500,-,0.0,0,0
2023/10/17,2023/10/19,6227,ＡＩメカテック,東証,特定,現物,買付,-,-,100,3950.0,0,0,0,一般,395000,-,0.0,0,0
2023/10/17,2023/10/19,6227,ＡＩメカテック,東証,特定,現物,売付,-,-,100,3965.0,0,0,0,一般,396500,-,0.0,0,0
2023/10/18,2023/10/20,6227,ＡＩメカテック,東証,特定,現物,売付,-,-,100,4245.0,504,50,0,一般,423946,-,0.0,0,0
"""
    realized_csv = """約定日,受渡日,銘柄コード,銘柄名,口座,信用区分,取引,数量[株],売却/決済単価[円],売却/決済額[円],平均取得価額[円],実現損益[円]
2023/10/13,2023/10/17,6227,ＡＩメカテック,特定,-,売付,100,3730.0,373000,3755.0,-2500
2023/10/17,2023/10/19,6227,ＡＩメカテック,特定,-,売付,100,3965.0,396500,3973.0,-800
2023/10/18,2023/10/20,6227,ＡＩメカテック,特定,-,売付,100,4245.0,423946,3973.0,26646
"""

    audit = client.post(
        "/api/v1/imports/rakuten-jp/audit",
        json={
            "tradehistory_filename": "tradehistory.csv",
            "tradehistory_content": tradehistory_csv,
            "realized_filename": "realized.csv",
            "realized_content": realized_csv,
        },
    )
    assert audit.status_code == 200
    body = audit.json()
    assert body["matched_count"] == 3
    assert body["gap_jpy"] == 0
    assert body["pnl_mismatch"] == []
    assert body["missing_in_tt"] == []
    assert body["unmatched_tt"] == []


def test_rakuten_import_commit_preserves_decimal_credit_prices(client):
    tradehistory_csv = """約定日,受渡日,銘柄コード,銘柄名,市場名称,口座区分,取引区分,売買区分,信用区分,弁済期限,数量［株］,単価［円］,手数料［円］,税金等［円］,諸費用［円］,税区分,受渡金額［円］,建約定日,建単価［円］,建手数料［円］,建手数料消費税［円］
2026/3/18,2026/3/21,8002,丸紅,東証,特定,信用返済,売埋,一般,無期限,100,5849.0,0,0,1790,源徴あり,-29060,2026/2/12,6122.7,0,0
"""
    realized_csv = """約定日,受渡日,銘柄コード,銘柄名,口座,信用区分,取引,数量[株],売却/決済単価[円],売却/決済額[円],平均取得価額[円],実現損益[円]
2026/3/18,2026/3/21,8002,丸紅,特定,一般,売埋,100,5849.0,584900,6122.70,-29160
"""

    preview = client.post(
        "/api/v1/imports/rakuten-jp/preview",
        json={"filename": "tradehistory.csv", "content": tradehistory_csv},
    )
    assert preview.status_code == 200
    preview_body = preview.json()
    assert preview_body["candidate_count"] == 1
    assert preview_body["candidates"][0]["buy"]["price"] == 6122.7
    assert preview_body["candidates"][0]["sell"]["price"] == 5849.0

    commit = client.post(
        "/api/v1/imports/rakuten-jp/commit",
        json={"filename": "tradehistory.csv", "items": preview_body["candidates"]},
    )
    assert commit.status_code == 200
    assert commit.json()["created_count"] == 1

    trades = client.get("/api/v1/trades")
    assert trades.status_code == 200
    item = trades.json()["items"][0]
    buy = next(fill for fill in item["fills"] if fill["side"] == "buy")
    sell = next(fill for fill in item["fills"] if fill["side"] == "sell")
    assert buy["price"] == 6122.7
    assert sell["price"] == 5849.0
    assert item["profit_jpy"] == -29160.0

    audit = client.post(
        "/api/v1/imports/rakuten-jp/audit",
        json={
            "tradehistory_filename": "tradehistory.csv",
            "tradehistory_content": tradehistory_csv,
            "realized_filename": "realized.csv",
            "realized_content": realized_csv,
        },
    )
    assert audit.status_code == 200
    body = audit.json()
    assert body["matched_count"] == 1
    assert body["gap_jpy"] == 0
    assert body["pnl_mismatch"] == []


def test_rakuten_import_preview_and_commit_support_margin_short(client):
    csv_content = """約定日,銘柄コード,銘柄,売買,約定数量,約定単価,手数料,取引区分
2026/03/01,7203,トヨタ自動車,売,100,2500,275,信用新規売
2026/03/10,7203,トヨタ自動車,買,100,2400,275,信用返済買
"""

    preview = client.post(
        "/api/v1/imports/rakuten-jp/preview",
        json={"filename": "rakuten_margin_short.csv", "content": csv_content},
    )
    assert preview.status_code == 200
    body = preview.json()
    assert body["candidate_count"] == 1
    assert body["error_count"] == 0
    assert body["skipped_count"] == 0
    assert body["candidates"][0]["position_side"] == "short"
    assert body["candidates"][0]["buy"]["price"] == 2400
    assert body["candidates"][0]["sell"]["price"] == 2500

    commit = client.post(
        "/api/v1/imports/rakuten-jp/commit",
        json={"filename": "rakuten_margin_short.csv", "items": body["candidates"]},
    )
    assert commit.status_code == 200
    assert commit.json()["created_count"] == 1


def test_sbi_import_preview_commit_audit_and_latest_session(client):
    execution_csv = """約定日,銘柄コード,銘柄名,取引,信用区分,売買,数量,約定単価,手数料,税金等,諸費用,建約定日,建単価
2026/04/01,7203,トヨタ自動車,現物,,買,100,3000,100,10,0,,
2026/04/10,7203,トヨタ自動車,現物,,売,100,3100,100,10,0,,
2026/04/02,9984,ソフトバンクG,信用,新規,買,100,7000,200,20,0,,
2026/04/12,9984,ソフトバンクG,信用,返済,売,100,6900,200,20,30,2026/04/02,7000
2026/04/03,7011,三菱重工業,信用,新規,売,100,2000,100,10,0,,
2026/04/13,7011,三菱重工業,信用,返済,買,100,1900,100,10,0,2026/04/03,2000
"""
    realized_csv = """約定日,銘柄コード,銘柄名,数量,売却単価,平均取得価額,実現損益
2026/04/10,7203,トヨタ自動車,100,3100,3000,9780
2026/04/12,9984,ソフトバンクG,100,6900,7000,-10470
2026/04/13,7011,三菱重工業,100,1900,2000,9780
"""

    preview = client.post("/api/v1/imports/sbi/preview", json={"filename": "sbi_exec.csv", "content": execution_csv})
    assert preview.status_code == 200
    preview_body = preview.json()
    assert preview_body["broker"] == "sbi"
    assert preview_body["candidate_count"] == 3
    assert {item["position_side"] for item in preview_body["candidates"]} == {"long", "short"}

    audit = client.post(
        "/api/v1/imports/sbi/audit",
        json={
            "tradehistory_filename": "sbi_exec.csv",
            "tradehistory_content": execution_csv,
            "realized_filename": "sbi_realized.csv",
            "realized_content": realized_csv,
        },
    )
    assert audit.status_code == 200
    audit_body = audit.json()
    assert audit_body["gap_jpy"] == 0
    assert audit_body["matched_count"] == 3

    commit = client.post(
        "/api/v1/imports/sbi/commit",
        json={
            "broker": "sbi",
            "filename": "sbi_exec.csv",
            "realized_filename": "sbi_realized.csv",
            "audit_gap_jpy": audit_body["gap_jpy"],
            "items": preview_body["candidates"],
        },
    )
    assert commit.status_code == 200
    commit_body = commit.json()
    assert commit_body["broker"] == "sbi"
    assert commit_body["created_count"] == 3
    assert commit_body["updated_count"] == 0

    second = client.post(
        "/api/v1/imports/sbi/commit",
        json={"broker": "sbi", "filename": "sbi_exec.csv", "items": preview_body["candidates"]},
    )
    assert second.status_code == 200
    assert second.json()["created_count"] == 0
    assert second.json()["updated_count"] == 3

    latest = client.get("/api/v1/imports/sessions/latest")
    assert latest.status_code == 200
    latest_body = latest.json()
    sbi_session = next(item for item in latest_body if item["broker"] == "sbi")
    assert sbi_session["source_name"] == "sbi_exec.csv"
    assert sbi_session["created_count"] == 0
    assert sbi_session["updated_count"] == 3


def test_sbi_import_parses_actual_csv_preamble_format(client):
    execution_csv = """
約定履歴照会 

商品指定,約定開始年月日,約定終了年月日,明細数,明細指定開始,明細指定終了
"株式現物","2024年01月01日","2026年04月18日","4","1","4"

（注）明細数はご指定された期間の合計です。

約定日,銘柄,銘柄コード,市場,取引,期限,預り,課税,約定数量,約定単価,手数料/諸経費等,税額,受渡日,受渡金額/決済損益
"2025/05/12","三菱商事","8058","--",株式現物買,"--"," 特定 ","--",10,2778,--,--,"2025/05/14",27780
"2025/10/14","信越化学工業","4063","--",株式現物売,"--"," 特定 ","申告",10,4933,--,--,"2025/10/16",49330
"2025/10/14","上新電機","8173","--",株式現物売,"--"," 特定 ","申告",1,2498,--,--,"2025/10/16",2498
"2026/01/16","東京海上ホールディングス","8766","--",株式現物売,"--"," 特定 ","申告",10,6000,--,--,"2026/01/20",60000
"""
    realized_csv = """"国内株式"

"検索件数","3"
"約定日","2024/1/1-2026/4/18"
"種類","現物"
"口座","すべて"

"商品","実現損益(税引前・円)","利益金額(円)","損失金額(円)"
"現物","+37,236","37,236","0"
"合計","+37,236","37,236","0"

"約定日","口座","銘柄名","取引","数量","売却/決済額","単価","平均取得価額","実現損益(税引前・円)"
"2026/1/16","特定","東京海上ホールディングス 8766","売却","10","60,000","6,000","3,006","+29,940"
"2025/10/14","特定","信越化学工業 4063","売却","10","49,330","4,933","4,251","+6,820"
"2025/10/14","特定","上新電機 8173","売却","1","2,498","2,498","2,022","+476"
"""

    preview = client.post("/api/v1/imports/sbi/preview", json={"filename": "SaveFile.csv", "content": execution_csv})
    assert preview.status_code == 200
    preview_body = preview.json()
    assert preview_body["candidate_count"] == 1
    assert preview_body["error_count"] == 0
    assert {item["code"] for item in preview_body["skipped"]} == {"sell_without_buy"}

    audit = client.post(
        "/api/v1/imports/sbi/audit",
        json={
            "tradehistory_filename": "SaveFile.csv",
            "tradehistory_content": execution_csv,
            "realized_filename": "DOMESTIC_STOCK.csv",
            "realized_content": realized_csv,
        },
    )
    assert audit.status_code == 200
    audit_body = audit.json()
    assert audit_body["preview_candidate_count"] == 1
    assert audit_body["tt_reconstructed_count"] == 0
    assert audit_body["rakuten_row_count"] == 3
    assert audit_body["rakuten_total_jpy"] == 37236
    assert audit_body["gap_jpy"] == -37236
    assert len(audit_body["missing_in_tt"]) == 3


def test_sbi_realized_only_preview_commit_idempotent_and_analysis(client):
    realized_csv = """"国内株式"

"検索件数","3"
"約定日","2024/1/1-2026/4/18"
"種類","現物"
"口座","すべて"

"商品","実現損益(税引前・円)","利益金額(円)","損失金額(円)"
"現物","+37,236","37,236","0"
"合計","+37,236","37,236","0"

"約定日","口座","銘柄名","取引","数量","売却/決済額","単価","平均取得価額","実現損益(税引前・円)"
"2026/1/16","特定","東京海上ホールディングス 8766","売却","10","60,000","6,000","3,006","+29,940"
"2025/10/14","特定","信越化学工業 4063","売却","10","49,330","4,933","4,251","+6,820"
"2025/10/14","特定","上新電機 8173","売却","1","2,498","2,498","2,022","+476"
"""

    preview = client.post("/api/v1/imports/sbi/realized/preview", json={"filename": "DOMESTIC_STOCK.csv", "content": realized_csv})
    assert preview.status_code == 200
    preview_body = preview.json()
    assert preview_body["candidate_count"] == 3
    assert preview_body["create_count"] == 3
    assert preview_body["update_count"] == 0
    assert preview_body["detailed_skip_count"] == 0

    commit = client.post(
        "/api/v1/imports/sbi/realized/commit",
        json={"filename": "DOMESTIC_STOCK.csv", "items": preview_body["candidates"]},
    )
    assert commit.status_code == 200
    commit_body = commit.json()
    assert commit_body["created_count"] == 3
    assert commit_body["updated_count"] == 0
    assert commit_body["skipped_count"] == 0

    trades = client.get("/api/v1/trades?limit=100")
    assert trades.status_code == 200
    trades_body = trades.json()
    assert trades_body["total"] == 3
    assert round(trades_body["stats"]["total_profit_jpy"]) == 37236
    assert trades_body["stats"]["avg_holding_days"] is None
    assert {item["data_quality"] for item in trades_body["items"]} == {"realized_only"}
    assert all(item["holding_days"] is None for item in trades_body["items"])

    detail = client.get(f"/api/v1/trades/{trades_body['items'][0]['id']}")
    assert detail.status_code == 200
    detail_body = detail.json()
    assert detail_body["data_quality"] == "realized_only"
    assert detail_body["broker_profit_jpy"] is not None
    assert detail_body["profit_jpy"] == detail_body["broker_profit_jpy"]
    assert detail_body["holding_days"] is None
    assert all(fill["fee_total_jpy"] is None for fill in detail_body["fills"])

    analysis = client.get("/api/v1/analysis/summary")
    assert analysis.status_code == 200
    stats = analysis.json()["stats"]
    assert stats["closed_trade_count"] == 3
    assert stats["realized_only_trade_count"] == 3
    assert stats["holding_analysis_trade_count"] == 0
    assert stats["avg_holding_days"] is None

    second = client.post(
        "/api/v1/imports/sbi/realized/commit",
        json={"filename": "DOMESTIC_STOCK.csv", "items": preview_body["candidates"]},
    )
    assert second.status_code == 200
    assert second.json()["created_count"] == 0
    assert second.json()["updated_count"] == 3
    assert client.get("/api/v1/trades?limit=100").json()["total"] == 3


def test_sbi_detailed_import_upgrades_realized_only_trade(client):
    realized_csv = """"国内株式"

"約定日","口座","銘柄名","取引","数量","売却/決済額","単価","平均取得価額","実現損益(税引前・円)"
"2026/1/16","特定","東京海上ホールディングス 8766","売却","10","60,000","6,000","3,006","+29,940"
"""
    realized_preview = client.post("/api/v1/imports/sbi/realized/preview", json={"filename": "realized.csv", "content": realized_csv})
    assert realized_preview.status_code == 200
    realized_commit = client.post(
        "/api/v1/imports/sbi/realized/commit",
        json={"filename": "realized.csv", "items": realized_preview.json()["candidates"]},
    )
    assert realized_commit.status_code == 200
    trade_id = realized_commit.json()["created_trade_ids"][0]
    patch = client.patch(f"/api/v1/trades/{trade_id}", json={"tags": "確認済", "notes_review": "補完時のメモ"})
    assert patch.status_code == 200

    execution_csv = """約定日,銘柄,銘柄コード,市場,取引,期限,預り,課税,約定数量,約定単価,手数料/諸経費等,税額,受渡日,受渡金額/決済損益
"2025/12/01","東京海上ホールディングス","8766","--",株式現物買,"--"," 特定 ","--",10,3006,--,--,"2025/12/03",30060
"2026/01/16","東京海上ホールディングス","8766","--",株式現物売,"--"," 特定 ","申告",10,6000,--,--,"2026/01/20",60000
"""
    detailed_preview = client.post("/api/v1/imports/sbi/preview", json={"filename": "SaveFile.csv", "content": execution_csv})
    assert detailed_preview.status_code == 200
    assert detailed_preview.json()["candidate_count"] == 1
    detailed_commit = client.post(
        "/api/v1/imports/sbi/commit",
        json={"broker": "sbi", "filename": "SaveFile.csv", "items": detailed_preview.json()["candidates"]},
    )
    assert detailed_commit.status_code == 200
    assert detailed_commit.json()["created_count"] == 0
    assert detailed_commit.json()["updated_count"] == 1
    assert detailed_commit.json()["updated_trade_ids"] == [trade_id]

    detail = client.get(f"/api/v1/trades/{trade_id}").json()
    assert detail["data_quality"] == "full"
    assert detail["broker_profit_jpy"] is None
    assert detail["opened_at"] == "2025-12-01"
    assert detail["holding_days"] == 46
    assert detail["tags"] == "確認済"
    assert detail["notes_review"] == "補完時のメモ"


def test_analysis_summary_includes_latest_import_focus(client):
    execution_csv = """約定日,銘柄コード,銘柄名,取引,信用区分,売買,数量,約定単価,手数料,税金等,諸費用
2026/04/01,7203,トヨタ自動車,現物,,買,100,3000,0,0,0
2026/04/10,7203,トヨタ自動車,現物,,売,100,2900,0,0,0
"""
    preview = client.post("/api/v1/imports/sbi/preview", json={"filename": "sbi_exec.csv", "content": execution_csv})
    assert preview.status_code == 200
    commit = client.post(
        "/api/v1/imports/sbi/commit",
        json={"broker": "sbi", "filename": "sbi_exec.csv", "audit_gap_jpy": 12, "items": preview.json()["candidates"]},
    )
    assert commit.status_code == 200

    summary = client.get("/api/v1/analysis/summary")
    assert summary.status_code == 200
    body = summary.json()
    assert body["latest_import"]["broker"] == "sbi"
    assert body["latest_import"]["source_name"] == "sbi_exec.csv"
    assert body["import_review_focus"]

def test_prices_route_returns_bars_from_yahoo_provider(client, monkeypatch):
    prev_provider = settings.price_provider
    prev_base_url = settings.yahoo_chart_base_url
    prev_user_agent = settings.yahoo_user_agent
    settings.price_provider = "yahoo_unofficial"
    settings.yahoo_chart_base_url = "https://query1.finance.yahoo.com/v8/finance/chart"
    settings.yahoo_user_agent = "test-agent"

    class _FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return json.dumps(
                {
                    "chart": {
                        "result": [
                            {
                                "timestamp": [1774483200, 1774569600],
                                "indicators": {
                                    "quote": [
                                        {
                                            "open": [990, 1000],
                                            "high": [1002, 1010],
                                            "low": [985, 995],
                                            "close": [1000, 1005],
                                            "volume": [8000, 10000],
                                        }
                                    ]
                                },
                            }
                        ],
                        "error": None,
                    }
                }
            ).encode("utf-8")

    monkeypatch.setattr(price_provider_core, "urlopen", lambda *args, **kwargs: _FakeResponse())
    price_provider_core._CACHE.clear()

    try:
        res = client.get("/api/v1/prices", params={"market": "JP", "symbol": "7203", "interval": "1d"})
        assert res.status_code == 200
        body = res.json()
        assert body["market"] == "JP"
        assert body["symbol"] == "7203"
        assert [bar["time"] for bar in body["bars"]] == ["2026-03-26", "2026-03-27"]
    finally:
        settings.price_provider = prev_provider
        settings.yahoo_chart_base_url = prev_base_url
        settings.yahoo_user_agent = prev_user_agent
        price_provider_core._CACHE.clear()


def test_prices_route_returns_us_bars_without_exchange(client, monkeypatch):
    prev_provider = settings.price_provider
    settings.price_provider = "yahoo_unofficial"

    class _FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return json.dumps(
                {
                    "chart": {
                        "result": [
                            {
                                "timestamp": [1774483200, 1774569600],
                                "indicators": {
                                    "quote": [
                                        {
                                            "open": [200, 204],
                                            "high": [205, 206],
                                            "low": [198, 202],
                                            "close": [204, 205],
                                            "volume": [150000, 120000],
                                        }
                                    ]
                                },
                            }
                        ],
                        "error": None,
                    }
                }
            ).encode("utf-8")

    monkeypatch.setattr(price_provider_core, "urlopen", lambda *args, **kwargs: _FakeResponse())
    price_provider_core._CACHE.clear()

    try:
        res = client.get("/api/v1/prices", params={"market": "US", "symbol": "AAPL", "interval": "1d"})
        assert res.status_code == 200
        body = res.json()
        assert body["market"] == "US"
        assert body["symbol"] == "AAPL"
        assert [bar["time"] for bar in body["bars"]] == ["2026-03-26", "2026-03-27"]
    finally:
        settings.price_provider = prev_provider
        price_provider_core._CACHE.clear()


def test_prices_route_returns_stale_cache_when_yahoo_fails(client, monkeypatch):
    prev_provider = settings.price_provider
    settings.price_provider = "yahoo_unofficial"

    class _SuccessResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return json.dumps(
                {
                    "chart": {
                        "result": [
                            {
                                "timestamp": [1774483200],
                                "indicators": {
                                    "quote": [
                                        {
                                            "open": [200],
                                            "high": [205],
                                            "low": [198],
                                            "close": [204],
                                            "volume": [150000],
                                        }
                                    ]
                                },
                            }
                        ],
                        "error": None,
                    }
                }
            ).encode("utf-8")

    call_count = {"value": 0}

    def _fake_urlopen(*args, **kwargs):
        call_count["value"] += 1
        if call_count["value"] == 1:
            return _SuccessResponse()
        raise HTTPError(url="https://query1.finance.yahoo.com", code=503, msg="down", hdrs=None, fp=None)

    monkeypatch.setattr(price_provider_core, "urlopen", _fake_urlopen)
    price_provider_core._CACHE.clear()

    try:
        first = client.get("/api/v1/prices", params={"market": "US", "symbol": "AAPL", "interval": "1d"})
        assert first.status_code == 200
        second = client.get("/api/v1/prices", params={"market": "US", "symbol": "AAPL", "interval": "1d"})
        assert second.status_code == 200
        assert second.json()["bars"][0]["close"] == 204
    finally:
        settings.price_provider = prev_provider
        price_provider_core._CACHE.clear()
