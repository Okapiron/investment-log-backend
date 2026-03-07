from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import inspect, text

from app.api import accounts, assets, dashboard, monthly, prices, settings as settings_api, snapshots, trades
from app.core.config import settings
from app.core.rate_limit import SimpleRateLimitMiddleware
from app.db.base import Base
from app.db.session import engine

app = FastAPI(title=settings.app_name)


def _parse_cors_origins(raw: str) -> list[str]:
    text_value = str(raw or "").strip()
    if not text_value:
        return ["*"]
    if text_value == "*":
        return ["*"]
    values = [v.strip() for v in text_value.split(",") if v.strip()]
    return values or ["*"]


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
def health():
    return {"status": "ok"}


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


@app.on_event("startup")
def startup():
    Base.metadata.create_all(bind=engine)
    _ensure_trade_user_id_column()
