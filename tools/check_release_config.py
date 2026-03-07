from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import inspect

from app.core.config import settings
from app.db.session import engine


def _is_empty(value) -> bool:
    return str(value or "").strip() == ""


def main() -> int:
    errors = []
    warnings = []

    if settings.auth_enabled:
        if _is_empty(settings.supabase_url):
            errors.append("SUPABASE_URL is required when AUTH_ENABLED=true")
        if _is_empty(settings.supabase_jwt_secret):
            errors.append("SUPABASE_JWT_SECRET is required when AUTH_ENABLED=true")
        if _is_empty(settings.ops_alert_target):
            errors.append("OPS_ALERT_TARGET is required when AUTH_ENABLED=true")
        if _is_empty(settings.db_backup_strategy):
            errors.append("DB_BACKUP_STRATEGY is required when AUTH_ENABLED=true")
        if settings.invite_code_required and _is_empty(settings.supabase_service_role_key):
            warnings.append("SUPABASE_SERVICE_ROLE_KEY is empty (auth user delete will be skipped)")

    origins = [v.strip() for v in str(settings.cors_allow_origins or "").split(",") if v.strip()]
    if settings.auth_enabled and ("*" in origins or len(origins) == 0):
        warnings.append("CORS_ALLOW_ORIGINS is wildcard/empty in auth-enabled mode")

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
        except Exception as e:
            errors.append(f"database schema check failed: {e}")

    if errors:
        print("CONFIG CHECK: FAILED")
        for e in errors:
            print(f"- ERROR: {e}")
        for w in warnings:
            print(f"- WARN: {w}")
        return 1

    print("CONFIG CHECK: OK")
    for w in warnings:
        print(f"- WARN: {w}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
