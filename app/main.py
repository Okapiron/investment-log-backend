from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api import accounts, assets, dashboard, monthly, prices, snapshots, trades
from app.core.config import settings
from app.db.base import Base
from app.db.session import engine

app = FastAPI(title=settings.app_name)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(accounts.router, prefix=settings.api_prefix)
app.include_router(assets.router, prefix=settings.api_prefix)
app.include_router(snapshots.router, prefix=settings.api_prefix)
app.include_router(dashboard.router, prefix=settings.api_prefix)
app.include_router(monthly.router, prefix=settings.api_prefix)
app.include_router(trades.router, prefix=settings.api_prefix)
app.include_router(prices.router, prefix=settings.api_prefix)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.on_event("startup")
def startup():
    Base.metadata.create_all(bind=engine)
