from __future__ import annotations


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
        if _is_blank(settings.supabase_url):
            errors.append("SUPABASE_URL is required when AUTH_ENABLED=true")
        if _is_blank(settings.supabase_jwt_secret):
            errors.append("SUPABASE_JWT_SECRET is required when AUTH_ENABLED=true")
        if _is_blank(settings.ops_alert_target):
            errors.append("OPS_ALERT_TARGET is required when AUTH_ENABLED=true")
        if _is_blank(settings.db_backup_strategy):
            errors.append("DB_BACKUP_STRATEGY is required when AUTH_ENABLED=true")

        parsed_origins = parse_cors_origins(settings.cors_allow_origins)
        if parsed_origins == ["*"]:
            warnings.append("CORS_ALLOW_ORIGINS is wildcard in auth-enabled mode")

    return errors, warnings
