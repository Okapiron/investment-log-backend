"""add user_id to trades

Revision ID: 20260307_0004
Revises: 20260305_0003
Create Date: 2026-03-07
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "20260307_0004"
down_revision: Union[str, None] = "20260305_0003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("trades", sa.Column("user_id", sa.String(), nullable=True))
    op.create_index("idx_trades_user_id", "trades", ["user_id"], unique=False)


def downgrade() -> None:
    op.drop_index("idx_trades_user_id", table_name="trades")
    op.drop_column("trades", "user_id")
