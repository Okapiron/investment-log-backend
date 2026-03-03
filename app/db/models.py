from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.constants import ASSET_TYPES
from app.db.base import Base


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc)
    )


class Account(TimestampMixin, Base):
    __tablename__ = "accounts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    institution: Mapped[Optional[str]] = mapped_column(String)
    note: Mapped[Optional[str]] = mapped_column(Text)
    display_order: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    is_active: Mapped[bool] = mapped_column(default=True, nullable=False)

    assets: Mapped[list["Asset"]] = relationship(back_populates="account")
    snapshots: Mapped[list["Snapshot"]] = relationship(back_populates="account")


class Asset(TimestampMixin, Base):
    __tablename__ = "assets"
    __table_args__ = (
        UniqueConstraint("account_id", "name", name="uq_assets_account_name"),
        CheckConstraint(f"asset_type IN ({','.join(repr(v) for v in ASSET_TYPES)})", name="ck_assets_type"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id", ondelete="RESTRICT"), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    asset_type: Mapped[str] = mapped_column(String, nullable=False, index=True)
    currency: Mapped[str] = mapped_column(String, default="JPY", nullable=False, index=True)
    ticker: Mapped[Optional[str]] = mapped_column(String)
    note: Mapped[Optional[str]] = mapped_column(Text)
    display_order: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    is_active: Mapped[bool] = mapped_column(default=True, nullable=False)

    account: Mapped[Account] = relationship(back_populates="assets")
    snapshots: Mapped[list["Snapshot"]] = relationship(back_populates="asset")


class Snapshot(TimestampMixin, Base):
    __tablename__ = "snapshots"
    __table_args__ = (
        UniqueConstraint("month", "asset_id", name="uq_snapshots_month_asset"),
        CheckConstraint("length(month) = 7", name="ck_snapshots_month_len"),
        CheckConstraint("value_jpy >= 0", name="ck_snapshots_value_nonnegative"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    month: Mapped[str] = mapped_column(String, nullable=False, index=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id", ondelete="RESTRICT"), nullable=False, index=True)
    asset_id: Mapped[int] = mapped_column(ForeignKey("assets.id", ondelete="RESTRICT"), nullable=False, index=True)
    value_jpy: Mapped[int] = mapped_column(Integer, nullable=False)
    memo: Mapped[Optional[str]] = mapped_column(Text)

    account: Mapped[Account] = relationship(back_populates="snapshots")
    asset: Mapped[Asset] = relationship(back_populates="snapshots")


class Trade(Base):
    __tablename__ = "trades"
    __table_args__ = (
        CheckConstraint("market IN ('JP','US')", name="ck_trades_market"),
        CheckConstraint("(rating IS NULL) OR (rating BETWEEN 1 AND 5)", name="ck_trades_rating"),
        Index("idx_trades_market", "market"),
        Index("idx_trades_symbol", "symbol"),
        Index("idx_trades_opened_at", "opened_at"),
        Index("idx_trades_closed_at", "closed_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    market: Mapped[str] = mapped_column(String, nullable=False)
    symbol: Mapped[str] = mapped_column(String, nullable=False)
    name: Mapped[Optional[str]] = mapped_column(String)
    notes_buy: Mapped[Optional[str]] = mapped_column(Text)
    notes_sell: Mapped[Optional[str]] = mapped_column(Text)
    notes_review: Mapped[Optional[str]] = mapped_column(Text)
    rating: Mapped[Optional[int]] = mapped_column(Integer)
    tags: Mapped[Optional[str]] = mapped_column(Text)
    chart_image_url: Mapped[Optional[str]] = mapped_column(Text)
    opened_at: Mapped[str] = mapped_column(String, nullable=False)
    closed_at: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[str] = mapped_column(String, nullable=False, default=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: Mapped[str] = mapped_column(
        String,
        nullable=False,
        default=lambda: datetime.now(timezone.utc).isoformat(),
        onupdate=lambda: datetime.now(timezone.utc).isoformat(),
    )

    fills: Mapped[list["Fill"]] = relationship(
        back_populates="trade",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class Fill(Base):
    __tablename__ = "fills"
    __table_args__ = (
        UniqueConstraint("trade_id", "side", name="uq_fills_trade_side"),
        CheckConstraint("side IN ('buy','sell')", name="ck_fills_side"),
        CheckConstraint("price >= 0", name="ck_fills_price_nonnegative"),
        CheckConstraint("qty >= 1", name="ck_fills_qty_positive"),
        CheckConstraint("(fee IS NULL) OR (fee >= 0)", name="ck_fills_fee_nonnegative"),
        Index("idx_fills_trade_id", "trade_id"),
        Index("idx_fills_date", "date"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trade_id: Mapped[int] = mapped_column(ForeignKey("trades.id", ondelete="CASCADE"), nullable=False)
    side: Mapped[str] = mapped_column(String, nullable=False)
    date: Mapped[str] = mapped_column(String, nullable=False)
    price: Mapped[int] = mapped_column(Integer, nullable=False)
    qty: Mapped[int] = mapped_column(Integer, nullable=False)
    fee: Mapped[Optional[int]] = mapped_column(Integer)

    trade: Mapped[Trade] = relationship(back_populates="fills")
