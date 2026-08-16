"""matching evidence and alternate provider listings

Revision ID: 0014_matching_evidence
Revises: 0013_provider_learning
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0014_matching_evidence"
down_revision: Union[str, None] = "0013_provider_learning"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "alternate_source_listing_v2",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("primary_source_series_id", sa.Integer(), nullable=False),
        sa.Column("source", sa.String(50), nullable=False),
        sa.Column("source_id", sa.String(500), nullable=False),
        sa.Column("title", sa.String(500), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("evidence_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["primary_source_series_id"], ["source_series_v2.id"], ondelete="CASCADE"
        ),
        sa.UniqueConstraint("source", "source_id", name="uq_alternate_source_listing_identity"),
    )
    op.create_index(
        "ix_alternate_source_listing_v2_primary_source_series_id",
        "alternate_source_listing_v2",
        ["primary_source_series_id"],
    )
    op.create_index(
        "ix_alternate_source_listing_v2_source",
        "alternate_source_listing_v2",
        ["source"],
    )
    op.create_table(
        "cover_fingerprint_v2",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("source_series_id", sa.Integer(), nullable=False),
        sa.Column("algorithm", sa.String(40), nullable=False),
        sa.Column("hash_hex", sa.String(128), nullable=False),
        sa.Column("content_sha256", sa.String(64), nullable=False),
        sa.Column("width", sa.Integer(), nullable=False),
        sa.Column("height", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["source_series_id"], ["source_series_v2.id"], ondelete="CASCADE"),
        sa.UniqueConstraint(
            "source_series_id", "algorithm", name="uq_cover_fingerprint_v2_source_algorithm"
        ),
    )
    op.create_index(
        "ix_cover_fingerprint_v2_source_series_id",
        "cover_fingerprint_v2",
        ["source_series_id"],
    )
    op.create_index(
        "ix_cover_fingerprint_v2_hash",
        "cover_fingerprint_v2",
        ["algorithm", "hash_hex"],
    )


def downgrade() -> None:
    op.drop_table("cover_fingerprint_v2")
    op.drop_table("alternate_source_listing_v2")
