"""Kavita catalog mappings

Revision ID: 0005_kavita_mapping
Revises: 0004_artifact_storage
Create Date: 2026-07-10 16:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "0005_kavita_mapping"
down_revision: Union[str, None] = "0004_artifact_storage"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("series_v2", sa.Column("kavita_series_id", sa.Integer(), nullable=True))
    op.add_column("series_v2", sa.Column("kavita_library_id", sa.Integer(), nullable=True))
    op.add_column(
        "series_v2", sa.Column("kavita_synced_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.create_index("ix_series_v2_kavita_series_id", "series_v2", ["kavita_series_id"])
    op.add_column("chapter_v2", sa.Column("kavita_chapter_id", sa.Integer(), nullable=True))
    op.add_column("chapter_v2", sa.Column("kavita_volume_id", sa.Integer(), nullable=True))
    op.add_column(
        "chapter_v2", sa.Column("kavita_mapped_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.create_index("ix_chapter_v2_kavita_chapter_id", "chapter_v2", ["kavita_chapter_id"])


def downgrade() -> None:
    op.drop_index("ix_chapter_v2_kavita_chapter_id", table_name="chapter_v2")
    op.drop_column("chapter_v2", "kavita_mapped_at")
    op.drop_column("chapter_v2", "kavita_volume_id")
    op.drop_column("chapter_v2", "kavita_chapter_id")
    op.drop_index("ix_series_v2_kavita_series_id", table_name="series_v2")
    op.drop_column("series_v2", "kavita_synced_at")
    op.drop_column("series_v2", "kavita_library_id")
    op.drop_column("series_v2", "kavita_series_id")
