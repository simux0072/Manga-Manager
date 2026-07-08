"""retry timestamps and external ids

Revision ID: 9d37ef72a2c1
Revises: 6abc529a4b8a
Create Date: 2026-07-07 23:30:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "9d37ef72a2c1"
down_revision = "6abc529a4b8a"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("series", sa.Column("external_ids", sa.Text(), nullable=False, server_default=""))
    op.add_column(
        "source_series", sa.Column("external_ids", sa.Text(), nullable=False, server_default="")
    )
    op.add_column("download_job", sa.Column("retry_after", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column("download_job", "retry_after")
    op.drop_column("source_series", "external_ids")
    op.drop_column("series", "external_ids")
