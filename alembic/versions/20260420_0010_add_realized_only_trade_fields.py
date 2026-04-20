"""add realized only trade fields

Revision ID: 20260420_0010
Revises: 20260418_0009
Create Date: 2026-04-20 00:10:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "20260420_0010"
down_revision = "20260418_0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("trades", sa.Column("data_quality", sa.String(), nullable=False, server_default="full"))
    op.add_column("trades", sa.Column("broker_profit_jpy", sa.Numeric(14, 2), nullable=True))
    op.create_check_constraint("ck_trades_data_quality", "trades", "data_quality IN ('full','realized_only')")
    op.alter_column("trades", "data_quality", server_default=None)


def downgrade() -> None:
    op.drop_constraint("ck_trades_data_quality", "trades", type_="check")
    op.drop_column("trades", "broker_profit_jpy")
    op.drop_column("trades", "data_quality")
