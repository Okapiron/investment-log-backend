"""add trades and fills

Revision ID: 20260220_0002
Revises: 20260219_0001
Create Date: 2026-02-20
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "20260220_0002"
down_revision: Union[str, None] = "20260219_0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "trades",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("market", sa.String(), nullable=False),
        sa.Column("symbol", sa.String(), nullable=False),
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("notes_buy", sa.Text(), nullable=True),
        sa.Column("notes_sell", sa.Text(), nullable=True),
        sa.Column("notes_review", sa.Text(), nullable=True),
        sa.Column("rating", sa.Integer(), nullable=True),
        sa.Column("tags", sa.Text(), nullable=True),
        sa.Column("chart_image_url", sa.Text(), nullable=True),
        sa.Column("opened_at", sa.String(), nullable=False),
        sa.Column("closed_at", sa.String(), nullable=False),
        sa.Column("created_at", sa.String(), nullable=False),
        sa.Column("updated_at", sa.String(), nullable=False),
        sa.CheckConstraint("market IN ('JP','US')", name="ck_trades_market"),
        sa.CheckConstraint("(rating IS NULL) OR (rating BETWEEN 1 AND 5)", name="ck_trades_rating"),
    )
    op.create_index("idx_trades_market", "trades", ["market"])
    op.create_index("idx_trades_symbol", "trades", ["symbol"])
    op.create_index("idx_trades_opened_at", "trades", ["opened_at"])
    op.create_index("idx_trades_closed_at", "trades", ["closed_at"])

    op.create_table(
        "fills",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("trade_id", sa.Integer(), nullable=False),
        sa.Column("side", sa.String(), nullable=False),
        sa.Column("date", sa.String(), nullable=False),
        sa.Column("price", sa.Integer(), nullable=False),
        sa.Column("qty", sa.Integer(), nullable=False),
        sa.Column("fee", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(["trade_id"], ["trades.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("trade_id", "side", name="uq_fills_trade_side"),
        sa.CheckConstraint("side IN ('buy','sell')", name="ck_fills_side"),
        sa.CheckConstraint("price >= 0", name="ck_fills_price_nonnegative"),
        sa.CheckConstraint("qty >= 1", name="ck_fills_qty_positive"),
        sa.CheckConstraint("(fee IS NULL) OR (fee >= 0)", name="ck_fills_fee_nonnegative"),
    )
    op.create_index("idx_fills_trade_id", "fills", ["trade_id"])
    op.create_index("idx_fills_date", "fills", ["date"])


def downgrade() -> None:
    op.drop_index("idx_fills_date", table_name="fills")
    op.drop_index("idx_fills_trade_id", table_name="fills")
    op.drop_table("fills")

    op.drop_index("idx_trades_closed_at", table_name="trades")
    op.drop_index("idx_trades_opened_at", table_name="trades")
    op.drop_index("idx_trades_symbol", table_name="trades")
    op.drop_index("idx_trades_market", table_name="trades")
    op.drop_table("trades")
