import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import inspect, text

from app.api import accounts, analysis, assets, dashboard, monthly, prices, settings as settings_api, snapshots, trades
from app.core.config import settings
from app.core.observability import RequestIdMiddleware
from app.core.private_access import ensure_private_api_access
from app.core.rate_limit import SimpleRateLimitMiddleware
from app.core.runtime_config import evaluate_runtime_config_issues, parse_cors_origins
from app.db.base import Base
from app.db.session import engine

logger = logging.getLogger("tradetrace.app")


def get_runtime_config_issues() -> tuple[list[str], list[str]]:
    return evaluate_runtime_config_issues(settings)


def _validate_runtime_config() -> None:
    errors, warnings = get_runtime_config_issues()
    for item in warnings:
        logger.warning("CONFIG WARNING: %s", item)
    if errors:
        raise RuntimeError("Invalid runtime config: " + "; ".join(errors))


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


def _run_startup_tasks() -> None:
    _validate_runtime_config()
    Base.metadata.create_all(bind=engine)
    _ensure_trade_user_id_column()
    _ensure_invite_code_columns()


@asynccontextmanager
async def lifespan(_: FastAPI):
    _run_startup_tasks()
    yield


app = FastAPI(title=settings.app_name, lifespan=lifespan)
app.add_middleware(RequestIdMiddleware)

cors_origins = parse_cors_origins(settings.cors_allow_origins)

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


@app.middleware("http")
async def private_api_access_middleware(request: Request, call_next):
    if request.url.path.startswith(settings.api_prefix):
        try:
            ensure_private_api_access(request)
        except HTTPException as exc:
            return JSONResponse({"detail": exc.detail}, status_code=exc.status_code)
    return await call_next(request)


@app.get("/health")
@app.get(f"{settings.api_prefix}/health")
def health():
    return {"status": "ok", "version": settings.app_version}


@app.get("/health/ready")
@app.get(f"{settings.api_prefix}/health/ready")
def health_ready():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception:
        raise HTTPException(status_code=503, detail="database unavailable")
    return {"status": "ok", "db": "ok", "version": settings.app_version}

app.include_router(accounts.router, prefix=settings.api_prefix)
app.include_router(assets.router, prefix=settings.api_prefix)
app.include_router(snapshots.router, prefix=settings.api_prefix)
app.include_router(dashboard.router, prefix=settings.api_prefix)
app.include_router(monthly.router, prefix=settings.api_prefix)
app.include_router(trades.router, prefix=settings.api_prefix)
app.include_router(analysis.router, prefix=settings.api_prefix)
app.include_router(prices.router, prefix=settings.api_prefix)
app.include_router(settings_api.router, prefix=settings.api_prefix)
