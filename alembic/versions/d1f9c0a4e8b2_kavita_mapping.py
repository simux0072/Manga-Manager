"""kavita mapping

Revision ID: d1f9c0a4e8b2
Revises: b7f3a1d2c9e4
Create Date: 2026-07-08 18:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "d1f9c0a4e8b2"
down_revision = "b7f3a1d2c9e4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    tables = set(inspector.get_table_names())
    if "series" in tables:
        op.add_column("series", sa.Column("kavita_series_id", sa.Integer(), nullable=True))
        op.add_column("series", sa.Column("kavita_library_id", sa.Integer(), nullable=True))
        op.add_column("series", sa.Column("kavita_synced_at", sa.DateTime(timezone=True), nullable=True))
        op.create_index(op.f("ix_series_kavita_series_id"), "series", ["kavita_series_id"])
    if "chapter" in tables:
        op.add_column("chapter", sa.Column("kavita_chapter_id", sa.Integer(), nullable=True))
        op.add_column("chapter", sa.Column("kavita_volume_id", sa.Integer(), nullable=True))
        op.add_column("chapter", sa.Column("kavita_mapped_at", sa.DateTime(timezone=True), nullable=True))
        op.create_index(op.f("ix_chapter_kavita_chapter_id"), "chapter", ["kavita_chapter_id"])


def downgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    tables = set(inspector.get_table_names())
    if "chapter" in tables:
        op.drop_index(op.f("ix_chapter_kavita_chapter_id"), table_name="chapter")
        op.drop_column("chapter", "kavita_mapped_at")
        op.drop_column("chapter", "kavita_volume_id")
        op.drop_column("chapter", "kavita_chapter_id")
    if "series" in tables:
        op.drop_index(op.f("ix_series_kavita_series_id"), table_name="series")
        op.drop_column("series", "kavita_synced_at")
        op.drop_column("series", "kavita_library_id")
        op.drop_column("series", "kavita_series_id")
