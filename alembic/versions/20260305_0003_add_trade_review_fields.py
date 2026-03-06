"""add review fields to trades

Revision ID: 20260305_0003
Revises: 20260220_0002
Create Date: 2026-03-05
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "20260305_0003"
down_revision: Union[str, None] = "20260220_0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "trades",
        sa.Column("review_done", sa.Boolean(), nullable=False, server_default=sa.text("0")),
    )
    op.add_column("trades", sa.Column("reviewed_at", sa.String(), nullable=True))
    op.alter_column("trades", "review_done", server_default=None)


def downgrade() -> None:
    op.drop_column("trades", "reviewed_at")
    op.drop_column("trades", "review_done")
