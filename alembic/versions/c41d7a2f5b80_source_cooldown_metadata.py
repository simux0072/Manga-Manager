"""source cooldown and metadata

Revision ID: c41d7a2f5b80
Revises: a9c2d6e8f104
Create Date: 2026-07-09 16:45:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "c41d7a2f5b80"
down_revision = "a9c2d6e8f104"
branch_labels = None
depends_on = None


def upgrade() -> None:
    inspector = inspect(op.get_bind())
    tables = set(inspector.get_table_names())
    if "source_series" in tables:
        columns = {column["name"] for column in inspector.get_columns("source_series")}
        if "metadata_json" not in columns:
            op.add_column(
                "source_series",
                sa.Column("metadata_json", sa.Text(), nullable=False, server_default=""),
            )
    if "source_health" in tables:
        columns = {column["name"] for column in inspector.get_columns("source_health")}
        if "download_cooldown_until" not in columns:
            op.add_column(
                "source_health",
                sa.Column("download_cooldown_until", sa.DateTime(timezone=True), nullable=True),
            )
        if "download_cooldown_reason" not in columns:
            op.add_column(
                "source_health",
                sa.Column("download_cooldown_reason", sa.Text(), nullable=False, server_default=""),
            )


def downgrade() -> None:
    inspector = inspect(op.get_bind())
    tables = set(inspector.get_table_names())
    if "source_health" in tables:
        columns = {column["name"] for column in inspector.get_columns("source_health")}
        if "download_cooldown_reason" in columns:
            op.drop_column("source_health", "download_cooldown_reason")
        if "download_cooldown_until" in columns:
            op.drop_column("source_health", "download_cooldown_until")
    if "source_series" in tables:
        columns = {column["name"] for column in inspector.get_columns("source_series")}
        if "metadata_json" in columns:
            op.drop_column("source_series", "metadata_json")
