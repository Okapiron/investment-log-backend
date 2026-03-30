"""add lineage fields to trade_import_records

Revision ID: 20260330_0008
Revises: 20260313_0007
Create Date: 2026-03-30
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "20260330_0008"
down_revision: Union[str, None] = "20260313_0007"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("trade_import_records", sa.Column("source_position_key", sa.String(), nullable=True))
    op.add_column("trade_import_records", sa.Column("source_lot_sequence", sa.Integer(), nullable=True))
    op.add_column(
        "trade_import_records",
        sa.Column("import_state", sa.String(), nullable=False, server_default="closed_round_trip"),
    )
    op.add_column(
        "trade_import_records",
        sa.Column("is_partial_exit", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.create_index(
        "idx_trade_import_records_position_state",
        "trade_import_records",
        ["source_position_key", "import_state"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("idx_trade_import_records_position_state", table_name="trade_import_records")
    op.drop_column("trade_import_records", "is_partial_exit")
    op.drop_column("trade_import_records", "import_state")
    op.drop_column("trade_import_records", "source_lot_sequence")
    op.drop_column("trade_import_records", "source_position_key")
