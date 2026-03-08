from __future__ import annotations

from urllib.parse import urlparse


def parse_cors_origins(raw: str) -> list[str]:
    text_value = str(raw or "").strip()
    if not text_value:
        return ["*"]
    if text_value == "*":
        return ["*"]
    values = [v.strip() for v in text_value.split(",") if v.strip()]
    return values or ["*"]


def _is_blank(value: str) -> bool:
    return str(value or "").strip() == ""


def evaluate_runtime_config_issues(settings) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []

    if settings.auth_enabled:
        db_url = str(settings.database_url or "").strip().lower()
        if db_url.startswith("sqlite"):
            warnings.append("database_url is SQLite in auth-enabled mode (Postgres is recommended for public release)")
        app_version = str(settings.app_version or "").strip().lower()
        if app_version in {"", "dev-local"}:
            warnings.append("APP_VERSION is not set for release (current value looks like local default)")

        if _is_blank(settings.supabase_url):
            errors.append("SUPABASE_URL is required when AUTH_ENABLED=true")
        else:
            supabase_url = str(settings.supabase_url or "").strip()
            parsed_supabase = urlparse(supabase_url)
            if parsed_supabase.scheme.lower() != "https":
                warnings.append("SUPABASE_URL should use https")
            host = str(parsed_supabase.hostname or "").lower()
            if host and not host.endswith(".supabase.co"):
                warnings.append("SUPABASE_URL host does not look like *.supabase.co")
        if _is_blank(settings.supabase_jwt_secret):
            errors.append("SUPABASE_JWT_SECRET is required when AUTH_ENABLED=true")
        if _is_blank(settings.ops_alert_target):
            warnings.append("OPS_ALERT_TARGET is empty in auth-enabled mode")
        if _is_blank(settings.db_backup_strategy):
            errors.append("DB_BACKUP_STRATEGY is required when AUTH_ENABLED=true")
        if settings.invite_code_required and _is_blank(settings.supabase_service_role_key):
            warnings.append("SUPABASE_SERVICE_ROLE_KEY is empty (auth user delete will be skipped)")
        if not settings.rate_limit_enabled:
            warnings.append("RATE_LIMIT_ENABLED is false in auth-enabled mode")

        parsed_origins = parse_cors_origins(settings.cors_allow_origins)
        if parsed_origins == ["*"]:
            warnings.append("CORS_ALLOW_ORIGINS is wildcard in auth-enabled mode")
        else:
            for origin in parsed_origins:
                parsed = urlparse(origin)
                scheme = str(parsed.scheme or "").lower()
                host = str(parsed.hostname or "").lower()
                is_localhost = host in {"localhost", "127.0.0.1"}
                if scheme == "http" and not is_localhost:
                    warnings.append(f"CORS origin should be https in auth-enabled mode: {origin}")

    if settings.rate_limit_enabled and int(settings.rate_limit_per_minute) < 30:
        warnings.append("RATE_LIMIT_PER_MINUTE is very low")

    return errors, warnings
