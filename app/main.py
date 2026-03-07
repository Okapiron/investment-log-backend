import logging

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import inspect, text

from app.api import accounts, assets, dashboard, monthly, prices, settings as settings_api, snapshots, trades
from app.core.config import settings
from app.core.observability import RequestIdMiddleware
from app.core.rate_limit import SimpleRateLimitMiddleware
from app.db.base import Base
from app.db.session import engine

app = FastAPI(title=settings.app_name)
logger = logging.getLogger("tradetrace.app")
app.add_middleware(RequestIdMiddleware)


def _parse_cors_origins(raw: str) -> list[str]:
    text_value = str(raw or "").strip()
    if not text_value:
        return ["*"]
    if text_value == "*":
        return ["*"]
    values = [v.strip() for v in text_value.split(",") if v.strip()]
    return values or ["*"]


def _is_blank(value: str) -> bool:
    return str(value or "").strip() == ""


def get_runtime_config_issues() -> tuple[list[str], list[str]]:
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

        parsed_origins = _parse_cors_origins(settings.cors_allow_origins)
        if parsed_origins == ["*"]:
            warnings.append("CORS_ALLOW_ORIGINS is wildcard in auth-enabled mode")

    return errors, warnings


def _validate_runtime_config() -> None:
    errors, warnings = get_runtime_config_issues()
    for item in warnings:
        logger.warning("CONFIG WARNING: %s", item)
    if errors:
        raise RuntimeError("Invalid runtime config: " + "; ".join(errors))


cors_origins = _parse_cors_origins(settings.cors_allow_origins)

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=False if cors_origins == ["*"] else True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if settings.rate_limit_enabled:
    app.add_middleware(
        SimpleRateLimitMiddleware,
        max_per_minute=settings.rate_limit_per_minute,
        api_prefix=settings.api_prefix,
    )

app.include_router(accounts.router, prefix=settings.api_prefix)
app.include_router(assets.router, prefix=settings.api_prefix)
app.include_router(snapshots.router, prefix=settings.api_prefix)
app.include_router(dashboard.router, prefix=settings.api_prefix)
app.include_router(monthly.router, prefix=settings.api_prefix)
app.include_router(trades.router, prefix=settings.api_prefix)
app.include_router(prices.router, prefix=settings.api_prefix)
app.include_router(settings_api.router, prefix=settings.api_prefix)


@app.get("/health")
@app.get(f"{settings.api_prefix}/health")
def health():
    return {"status": "ok"}


@app.get("/health/ready")
@app.get(f"{settings.api_prefix}/health/ready")
def health_ready():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception:
        raise HTTPException(status_code=503, detail="database unavailable")
    return {"status": "ok", "db": "ok"}


def _ensure_trade_user_id_column() -> None:
    # Backward-compatible schema patch for existing beta DBs.
    with engine.begin() as conn:
        inspector = inspect(conn)
        if "trades" not in inspector.get_table_names():
            return

        cols = {c.get("name") for c in inspector.get_columns("trades")}
        if "user_id" not in cols:
            conn.execute(text("ALTER TABLE trades ADD COLUMN user_id VARCHAR"))

        indexes = {i.get("name") for i in inspector.get_indexes("trades")}
        if "idx_trades_user_id" not in indexes:
            conn.execute(text("CREATE INDEX idx_trades_user_id ON trades (user_id)"))


def _ensure_invite_code_columns() -> None:
    # Backward-compatible schema patch for existing beta DBs.
    with engine.begin() as conn:
        inspector = inspect(conn)
        if "invite_codes" not in inspector.get_table_names():
            return

        cols = {c.get("name") for c in inspector.get_columns("invite_codes")}
        if "used_at" not in cols:
            conn.execute(text("ALTER TABLE invite_codes ADD COLUMN used_at TIMESTAMP"))

        indexes = {i.get("name") for i in inspector.get_indexes("invite_codes")}
        if "idx_invite_codes_used_at" not in indexes:
            conn.execute(text("CREATE INDEX idx_invite_codes_used_at ON invite_codes (used_at)"))


@app.on_event("startup")
def startup():
    _validate_runtime_config()
    Base.metadata.create_all(bind=engine)
    _ensure_trade_user_id_column()
    _ensure_invite_code_columns()
