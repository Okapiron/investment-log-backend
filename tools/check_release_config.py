from pathlib import Path
import argparse
from datetime import datetime, timezone
from urllib.parse import urlparse
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import inspect, text

from app.core.config import settings
from app.db.session import engine


def _is_empty(value) -> bool:
    return str(value or "").strip() == ""


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate release runtime config and schema.")
    parser.add_argument(
        "--strict",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="treat warnings as failure (default: false)",
    )
    args = parser.parse_args()

    errors = []
    warnings = []

    if settings.auth_enabled:
        db_url = str(settings.database_url or "").strip().lower()
        if db_url.startswith("sqlite"):
            warnings.append("database_url is SQLite in auth-enabled mode (Postgres is recommended for public release)")
        if _is_empty(settings.supabase_url):
            errors.append("SUPABASE_URL is required when AUTH_ENABLED=true")
        else:
            supabase_url = str(settings.supabase_url or "").strip()
            parsed_supabase = urlparse(supabase_url)
            if parsed_supabase.scheme.lower() != "https":
                warnings.append("SUPABASE_URL should use https")
            host = str(parsed_supabase.hostname or "").lower()
            if host and not host.endswith(".supabase.co"):
                warnings.append("SUPABASE_URL host does not look like *.supabase.co")
        if _is_empty(settings.supabase_jwt_secret):
            errors.append("SUPABASE_JWT_SECRET is required when AUTH_ENABLED=true")
        if _is_empty(settings.ops_alert_target):
            errors.append("OPS_ALERT_TARGET is required when AUTH_ENABLED=true")
        if _is_empty(settings.db_backup_strategy):
            errors.append("DB_BACKUP_STRATEGY is required when AUTH_ENABLED=true")
        if settings.invite_code_required and _is_empty(settings.supabase_service_role_key):
            warnings.append("SUPABASE_SERVICE_ROLE_KEY is empty (auth user delete will be skipped)")
        if not settings.rate_limit_enabled:
            warnings.append("RATE_LIMIT_ENABLED is false in auth-enabled mode")

    origins = [v.strip() for v in str(settings.cors_allow_origins or "").split(",") if v.strip()]
    if settings.auth_enabled and ("*" in origins or len(origins) == 0):
        warnings.append("CORS_ALLOW_ORIGINS is wildcard/empty in auth-enabled mode")
    if settings.auth_enabled:
        for origin in origins:
            if origin == "*":
                continue
            parsed = urlparse(origin)
            scheme = str(parsed.scheme or "").lower()
            host = str(parsed.hostname or "").lower()
            is_localhost = host in {"localhost", "127.0.0.1"}
            if scheme == "http" and not is_localhost:
                warnings.append(f"CORS origin should be https in auth-enabled mode: {origin}")

    if settings.rate_limit_enabled and int(settings.rate_limit_per_minute) < 30:
        warnings.append("RATE_LIMIT_PER_MINUTE is very low")

    if settings.auth_enabled:
        try:
            with engine.connect() as conn:
                inspector = inspect(conn)
                tables = set(inspector.get_table_names())

                if "trades" not in tables:
                    errors.append("trades table is missing")
                else:
                    trade_cols = {c.get("name") for c in inspector.get_columns("trades")}
                    if "user_id" not in trade_cols:
                        errors.append("trades.user_id is missing (run alembic upgrade head)")

                if settings.invite_code_required:
                    if "invite_codes" not in tables:
                        errors.append("invite_codes table is missing (run alembic upgrade head)")
                    else:
                        invite_cols = {c.get("name") for c in inspector.get_columns("invite_codes")}
                        if "used_at" not in invite_cols:
                            warnings.append("invite_codes.used_at is missing (latest migration not applied)")
                        active_invites = int(
                            conn.execute(
                                text(
                                    """
                                    SELECT COUNT(1)
                                    FROM invite_codes
                                    WHERE expires_at > :now_ts
                                      AND used_count < max_uses
                                    """
                                ),
                                {"now_ts": datetime.now(timezone.utc)},
                            ).scalar()
                            or 0
                        )
                        if active_invites <= 0:
                            warnings.append("no active invite codes found (invite onboarding will be blocked)")
        except Exception as e:
            errors.append(f"database schema check failed: {e}")

    if errors:
        print("CONFIG CHECK: FAILED")
        for e in errors:
            print(f"- ERROR: {e}")
        for w in warnings:
            print(f"- WARN: {w}")
        return 1

    if args.strict and warnings:
        print("CONFIG CHECK: FAILED (strict mode)")
        for w in warnings:
            print(f"- WARN: {w}")
        return 1

    print("CONFIG CHECK: OK")
    for w in warnings:
        print(f"- WARN: {w}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
