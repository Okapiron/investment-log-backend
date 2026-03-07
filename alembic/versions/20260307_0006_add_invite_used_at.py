"""add used_at to invite_codes

Revision ID: 20260307_0006
Revises: 20260307_0005
Create Date: 2026-03-08
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "20260307_0006"
down_revision: Union[str, None] = "20260307_0005"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("invite_codes", sa.Column("used_at", sa.DateTime(timezone=True), nullable=True))
    op.create_index("idx_invite_codes_used_at", "invite_codes", ["used_at"], unique=False)


def downgrade() -> None:
    op.drop_index("idx_invite_codes_used_at", table_name="invite_codes")
    op.drop_column("invite_codes", "used_at")
