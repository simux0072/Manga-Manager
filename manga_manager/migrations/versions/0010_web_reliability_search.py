"""repair source counters and extend catalog search

Revision ID: 0010_web_reliability_search
Revises: 0009_canonical_provider_unique
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0010_web_reliability_search"
down_revision: Union[str, None] = "0009_canonical_provider_unique"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        "UPDATE source_state_v2 SET consecutive_failures = 0 WHERE consecutive_failures IS NULL"
    )
    with op.batch_alter_table("source_state_v2") as batch:
        batch.alter_column(
            "consecutive_failures",
            existing_type=sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        )
    if op.get_bind().dialect.name == "postgresql":
        op.execute(
            "CREATE INDEX IF NOT EXISTS ix_series_v2_description_trgm "
            "ON series_v2 USING gin (description gin_trgm_ops)"
        )
        op.execute(
            "CREATE INDEX IF NOT EXISTS ix_series_alias_v2_display_trgm "
            "ON series_alias_v2 USING gin (display_value gin_trgm_ops)"
        )


def downgrade() -> None:
    if op.get_bind().dialect.name == "postgresql":
        op.execute("DROP INDEX IF EXISTS ix_series_alias_v2_display_trgm")
        op.execute("DROP INDEX IF EXISTS ix_series_v2_description_trgm")
    with op.batch_alter_table("source_state_v2") as batch:
        batch.alter_column(
            "consecutive_failures",
            existing_type=sa.Integer(),
            nullable=True,
            server_default=None,
        )
