from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request


def _normalize_base(base: str) -> str:
    text = str(base or "").strip()
    if not text:
        raise ValueError("--base is required")
    return text.rstrip("/")


def _normalize_prefix(prefix: str) -> str:
    raw = str(prefix or "").strip()
    if not raw:
        return ""
    return raw if raw.startswith("/") else f"/{raw}"


def _request_json(url: str) -> tuple[int, dict, dict]:
    try:
        with urllib.request.urlopen(url, timeout=10) as res:
            status = int(res.status)
            headers = {k.lower(): v for k, v in dict(res.headers).items()}
            body = res.read().decode("utf-8")
            data = json.loads(body) if body else {}
            return status, data if isinstance(data, dict) else {}, headers
    except urllib.error.HTTPError as e:
        payload = {}
        headers = {k.lower(): v for k, v in dict(e.headers).items()} if e.headers else {}
        try:
            payload = json.loads(e.read().decode("utf-8"))
            if not isinstance(payload, dict):
                payload = {}
        except Exception:
            payload = {}
        return int(e.code), payload, headers


def _check(condition: bool, label: str, detail: str = "") -> bool:
    status = "PASS" if condition else "FAIL"
    suffix = f" ({detail})" if detail else ""
    print(f"[{status}] {label}{suffix}")
    return condition


def main() -> int:
    parser = argparse.ArgumentParser(description="Run release smoke checks against a TradeTrace backend.")
    parser.add_argument("--base", default="http://127.0.0.1:8000", help="backend base URL (default: http://127.0.0.1:8000)")
    parser.add_argument("--api-prefix", default="/api/v1", help="API prefix (default: /api/v1)")
    parser.add_argument(
        "--expect-auth-required",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="expect /trades without auth to be rejected (default: true)",
    )
    parser.add_argument(
        "--expect-rate-limit-headers",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="expect X-RateLimit-* headers on /trades response (default: false)",
    )
    args = parser.parse_args()

    base = _normalize_base(args.base)
    prefix = _normalize_prefix(args.api_prefix)

    ok = True

    status, payload, headers = _request_json(f"{base}/health")
    ok &= _check(status == 200 and payload.get("status") == "ok", "health", f"status={status}")
    ok &= _check(bool(str(headers.get("x-request-id") or "").strip()), "x-request-id header on /health")
    ok &= _check(headers.get("x-content-type-options") == "nosniff", "x-content-type-options header")
    ok &= _check(headers.get("x-frame-options") == "DENY", "x-frame-options header")
    ok &= _check(headers.get("referrer-policy") == "no-referrer", "referrer-policy header")

    status, payload, _ = _request_json(f"{base}{prefix}/health/ready")
    db_ok = payload.get("db") == "ok"
    ok &= _check(status == 200 and payload.get("status") == "ok" and db_ok, "health/ready", f"status={status}")

    status, payload, _ = _request_json(f"{base}/openapi.json")
    has_trades = False
    if status == 200:
        paths = payload.get("paths")
        has_trades = isinstance(paths, dict) and f"{prefix}/trades" in paths
    ok &= _check(status == 200 and has_trades, "openapi includes trades path", f"status={status}")

    settings_status, settings_body, settings_headers = _request_json(f"{base}{prefix}/settings/me")
    no_store = str(settings_headers.get("cache-control") or "").strip().lower() == "no-store"
    ok &= _check(no_store, "settings/me returns no-store cache policy")
    if args.expect_auth_required:
        settings_auth_ok = settings_status in {401, 403}
        ok &= _check(settings_auth_ok, "settings/me requires auth", f"status={settings_status}")
    else:
        settings_open_ok = settings_status == 200 and bool(str(settings_body.get("user_id") or "").strip())
        ok &= _check(settings_open_ok, "settings/me available in auth-off mode", f"status={settings_status}")

    status, _, trades_headers = _request_json(f"{base}{prefix}/trades")
    has_rate_headers = (
        bool(str(trades_headers.get("x-ratelimit-limit") or "").strip())
        and bool(str(trades_headers.get("x-ratelimit-remaining") or "").strip())
        and bool(str(trades_headers.get("x-ratelimit-reset") or "").strip())
    )
    if args.expect_rate_limit_headers:
        ok &= _check(has_rate_headers, "rate-limit headers present on trades response")
    else:
        _check(True, "rate-limit header check skipped (not expected)")
    if args.expect_auth_required:
        auth_ok = status in {401, 403}
        ok &= _check(auth_ok, "trades requires auth", f"status={status}")
    else:
        auth_ok = status == 200
        ok &= _check(auth_ok, "trades available without auth", f"status={status}")

    print("SMOKE CHECK: OK" if ok else "SMOKE CHECK: FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
