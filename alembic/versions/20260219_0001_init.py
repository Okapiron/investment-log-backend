"""init schema

Revision ID: 20260219_0001
Revises:
Create Date: 2026-02-19
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "20260219_0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "accounts",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(), nullable=False, unique=True),
        sa.Column("institution", sa.String(), nullable=True),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("display_order", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("1")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("idx_accounts_display_order", "accounts", ["display_order"])

    op.create_table(
        "assets",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("asset_type", sa.String(), nullable=False),
        sa.Column("currency", sa.String(), nullable=False, server_default="JPY"),
        sa.Column("ticker", sa.String(), nullable=True),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("display_order", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("1")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], ondelete="RESTRICT"),
        sa.UniqueConstraint("account_id", "name", name="uq_assets_account_name"),
        sa.CheckConstraint("asset_type IN ('cash','stock','fund','bond','crypto','other')", name="ck_assets_type"),
    )
    op.create_index("idx_assets_account", "assets", ["account_id"])
    op.create_index("idx_assets_type", "assets", ["asset_type"])
    op.create_index("idx_assets_currency", "assets", ["currency"])

    op.create_table(
        "snapshots",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("month", sa.String(), nullable=False),
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("asset_id", sa.Integer(), nullable=False),
        sa.Column("value_jpy", sa.Integer(), nullable=False),
        sa.Column("memo", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["asset_id"], ["assets.id"], ondelete="RESTRICT"),
        sa.UniqueConstraint("month", "asset_id", name="uq_snapshots_month_asset"),
        sa.CheckConstraint("length(month) = 7", name="ck_snapshots_month_len"),
        sa.CheckConstraint("value_jpy >= 0", name="ck_snapshots_value_nonnegative"),
    )
    op.create_index("idx_snapshots_month", "snapshots", ["month"])
    op.create_index("idx_snapshots_account", "snapshots", ["account_id"])
    op.create_index("idx_snapshots_asset", "snapshots", ["asset_id"])
    op.create_index("idx_snapshots_month_account", "snapshots", ["month", "account_id"])


def downgrade() -> None:
    op.drop_index("idx_snapshots_month_account", table_name="snapshots")
    op.drop_index("idx_snapshots_asset", table_name="snapshots")
    op.drop_index("idx_snapshots_account", table_name="snapshots")
    op.drop_index("idx_snapshots_month", table_name="snapshots")
    op.drop_table("snapshots")

    op.drop_index("idx_assets_currency", table_name="assets")
    op.drop_index("idx_assets_type", table_name="assets")
    op.drop_index("idx_assets_account", table_name="assets")
    op.drop_table("assets")

    op.drop_index("idx_accounts_display_order", table_name="accounts")
    op.drop_table("accounts")
