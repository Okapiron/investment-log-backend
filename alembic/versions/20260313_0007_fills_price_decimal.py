"""make fills.price decimal(12,2)

Revision ID: 20260313_0007
Revises: 20260307_0006
Create Date: 2026-03-13
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "20260313_0007"
down_revision: Union[str, None] = "20260307_0006"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    dialect = bind.dialect.name

    if dialect == "postgresql":
        op.alter_column(
            "fills",
            "price",
            existing_type=sa.Integer(),
            type_=sa.Numeric(12, 2),
            existing_nullable=False,
            postgresql_using="price::numeric(12,2)",
        )
    else:
        with op.batch_alter_table("fills") as batch_op:
            batch_op.alter_column(
                "price",
                existing_type=sa.Integer(),
                type_=sa.Numeric(12, 2),
                existing_nullable=False,
            )


def downgrade() -> None:
    bind = op.get_bind()
    dialect = bind.dialect.name

    if dialect == "postgresql":
        op.alter_column(
            "fills",
            "price",
            existing_type=sa.Numeric(12, 2),
            type_=sa.Integer(),
            existing_nullable=False,
            postgresql_using="ROUND(price)::integer",
        )
    else:
        with op.batch_alter_table("fills") as batch_op:
            batch_op.alter_column(
                "price",
                existing_type=sa.Numeric(12, 2),
                type_=sa.Integer(),
                existing_nullable=False,
            )
