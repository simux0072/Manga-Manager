"""chapter fingerprints

Revision ID: 3e7b91a5c2d0
Revises: c41d7a2f5b80
Create Date: 2026-07-09 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "3e7b91a5c2d0"
down_revision: Union[str, None] = "c41d7a2f5b80"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "chapter_fingerprint",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("source_series_id", sa.Integer(), nullable=False),
        sa.Column("chapter_release_id", sa.Integer(), nullable=False),
        sa.Column("source", sa.String(length=50), nullable=False),
        sa.Column("chapter_number", sa.String(length=40), nullable=False),
        sa.Column("page_index", sa.Integer(), nullable=False),
        sa.Column("segment_index", sa.Integer(), nullable=False),
        sa.Column("algorithm", sa.String(length=40), nullable=False),
        sa.Column("hash_hex", sa.String(length=32), nullable=False),
        sa.Column("width", sa.Integer(), nullable=False),
        sa.Column("height", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["chapter_release_id"], ["chapter_release.id"]),
        sa.ForeignKeyConstraint(["source_series_id"], ["source_series.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "chapter_release_id",
            "page_index",
            "segment_index",
            "algorithm",
            name="uq_chapter_fingerprint_segment",
        ),
    )
    op.create_index("ix_chapter_fingerprint_chapter_number", "chapter_fingerprint", ["chapter_number"])
    op.create_index("ix_chapter_fingerprint_chapter_release_id", "chapter_fingerprint", ["chapter_release_id"])
    op.create_index("ix_chapter_fingerprint_hash_hex", "chapter_fingerprint", ["hash_hex"])
    op.create_index(
        "ix_chapter_fingerprint_lookup",
        "chapter_fingerprint",
        ["source", "chapter_number", "algorithm"],
    )
    op.create_index("ix_chapter_fingerprint_source", "chapter_fingerprint", ["source"])
    op.create_index("ix_chapter_fingerprint_source_series_id", "chapter_fingerprint", ["source_series_id"])
    op.create_table(
        "cover_fingerprint",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("source_series_id", sa.Integer(), nullable=False),
        sa.Column("source", sa.String(length=50), nullable=False),
        sa.Column("algorithm", sa.String(length=40), nullable=False),
        sa.Column("hash_hex", sa.String(length=32), nullable=False),
        sa.Column("width", sa.Integer(), nullable=False),
        sa.Column("height", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["source_series_id"], ["source_series.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "source_series_id",
            "algorithm",
            name="uq_cover_fingerprint_source_algorithm",
        ),
    )
    op.create_index("ix_cover_fingerprint_hash_hex", "cover_fingerprint", ["hash_hex"])
    op.create_index("ix_cover_fingerprint_lookup", "cover_fingerprint", ["source", "algorithm"])
    op.create_index("ix_cover_fingerprint_source", "cover_fingerprint", ["source"])
    op.create_index("ix_cover_fingerprint_source_series_id", "cover_fingerprint", ["source_series_id"])


def downgrade() -> None:
    op.drop_index("ix_cover_fingerprint_source_series_id", table_name="cover_fingerprint")
    op.drop_index("ix_cover_fingerprint_source", table_name="cover_fingerprint")
    op.drop_index("ix_cover_fingerprint_lookup", table_name="cover_fingerprint")
    op.drop_index("ix_cover_fingerprint_hash_hex", table_name="cover_fingerprint")
    op.drop_table("cover_fingerprint")
    op.drop_index("ix_chapter_fingerprint_source_series_id", table_name="chapter_fingerprint")
    op.drop_index("ix_chapter_fingerprint_source", table_name="chapter_fingerprint")
    op.drop_index("ix_chapter_fingerprint_lookup", table_name="chapter_fingerprint")
    op.drop_index("ix_chapter_fingerprint_hash_hex", table_name="chapter_fingerprint")
    op.drop_index("ix_chapter_fingerprint_chapter_release_id", table_name="chapter_fingerprint")
    op.drop_index("ix_chapter_fingerprint_chapter_number", table_name="chapter_fingerprint")
    op.drop_table("chapter_fingerprint")
