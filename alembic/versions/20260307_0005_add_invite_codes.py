"""add invite_codes table

Revision ID: 20260307_0005
Revises: 20260307_0004
Create Date: 2026-03-07
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "20260307_0005"
down_revision: Union[str, None] = "20260307_0004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "invite_codes",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("code_hash", sa.String(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("max_uses", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("used_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("used_by_user_id", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("max_uses >= 1", name="ck_invite_codes_max_uses"),
        sa.CheckConstraint("used_count >= 0", name="ck_invite_codes_used_count_nonnegative"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("code_hash", name="uq_invite_codes_code_hash"),
    )
    op.create_index("idx_invite_codes_code_hash", "invite_codes", ["code_hash"], unique=False)
    op.create_index("idx_invite_codes_expires_at", "invite_codes", ["expires_at"], unique=False)
    op.create_index("idx_invite_codes_used_by_user_id", "invite_codes", ["used_by_user_id"], unique=False)

    op.alter_column("invite_codes", "max_uses", server_default=None)
    op.alter_column("invite_codes", "used_count", server_default=None)


def downgrade() -> None:
    op.drop_index("idx_invite_codes_used_by_user_id", table_name="invite_codes")
    op.drop_index("idx_invite_codes_expires_at", table_name="invite_codes")
    op.drop_index("idx_invite_codes_code_hash", table_name="invite_codes")
    op.drop_table("invite_codes")
