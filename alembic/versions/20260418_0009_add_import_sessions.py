"""add import sessions

Revision ID: 20260418_0009
Revises: 20260330_0008
Create Date: 2026-04-18
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "20260418_0009"
down_revision: Union[str, None] = "20260330_0008"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "import_sessions",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("user_id", sa.String(), nullable=True),
        sa.Column("broker", sa.String(), nullable=False),
        sa.Column("source_name", sa.String(), nullable=True),
        sa.Column("realized_source_name", sa.String(), nullable=True),
        sa.Column("imported_at", sa.String(), nullable=False),
        sa.Column("created_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("updated_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("skipped_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("audit_gap_jpy", sa.Numeric(14, 2), nullable=True),
    )
    op.create_index("idx_import_sessions_user_broker", "import_sessions", ["user_id", "broker"])
    op.create_index("idx_import_sessions_imported_at", "import_sessions", ["imported_at"])


def downgrade() -> None:
    op.drop_index("idx_import_sessions_imported_at", table_name="import_sessions")
    op.drop_index("idx_import_sessions_user_broker", table_name="import_sessions")
    op.drop_table("import_sessions")
