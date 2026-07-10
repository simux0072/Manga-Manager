"""v2 catalog

Revision ID: 0003_catalog
Revises: 0002_job_events_workers
Create Date: 2026-07-10 14:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "0003_catalog"
down_revision: Union[str, None] = "0002_job_events_workers"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def json_type():
    return sa.JSON().with_variant(postgresql.JSONB(none_as_null=True), "postgresql")


def upgrade() -> None:
    op.create_table(
        "series_v2",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("storage_key", sa.String(length=64), nullable=False),
        sa.Column("title", sa.String(length=500), nullable=False),
        sa.Column("normalized_title", sa.String(length=500), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("cover_url", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=30), nullable=False),
        sa.Column("metadata_json", json_type(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "status IN ('untracked', 'interested', 'reading', 'caught_up', 'paused')",
            name="ck_series_v2_status",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_series_v2_storage_key", "series_v2", ["storage_key"], unique=True)
    op.create_index("ix_series_v2_title", "series_v2", ["title"])
    op.create_index("ix_series_v2_normalized_title", "series_v2", ["normalized_title"])
    op.create_index("ix_series_v2_status", "series_v2", ["status"])

    op.create_table(
        "source_series_v2",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("series_id", sa.Integer(), nullable=False),
        sa.Column("source", sa.String(length=50), nullable=False),
        sa.Column("source_id", sa.String(length=500), nullable=False),
        sa.Column("title", sa.String(length=500), nullable=False),
        sa.Column("normalized_title", sa.String(length=500), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("cover_url", sa.Text(), nullable=False),
        sa.Column("popularity", sa.Float(), nullable=False),
        sa.Column("metadata_json", json_type(), nullable=False),
        sa.Column("last_checked_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("detail_fetched_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["series_id"], ["series_v2.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source", "source_id", name="uq_source_series_v2_identity"),
    )
    op.create_index("ix_source_series_v2_series_id", "source_series_v2", ["series_id"])
    op.create_index("ix_source_series_v2_source", "source_series_v2", ["source"])
    op.create_index("ix_source_series_v2_normalized_title", "source_series_v2", ["normalized_title"])
    op.create_index(
        "ix_source_series_v2_source_checked",
        "source_series_v2",
        ["source", "last_checked_at"],
    )

    op.create_table(
        "series_alias_v2",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("series_id", sa.Integer(), nullable=False),
        sa.Column("source_series_id", sa.Integer(), nullable=True),
        sa.Column("display_value", sa.String(length=500), nullable=False),
        sa.Column("normalized_value", sa.String(length=500), nullable=False),
        sa.ForeignKeyConstraint(["series_id"], ["series_v2.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["source_series_id"], ["source_series_v2.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("series_id", "normalized_value", name="uq_series_alias_v2_value"),
    )
    op.create_index("ix_series_alias_v2_series_id", "series_alias_v2", ["series_id"])
    op.create_index("ix_series_alias_v2_source_series_id", "series_alias_v2", ["source_series_id"])
    op.create_index("ix_series_alias_v2_normalized_value", "series_alias_v2", ["normalized_value"])

    op.create_table(
        "external_identifier_v2",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("series_id", sa.Integer(), nullable=False),
        sa.Column("source_series_id", sa.Integer(), nullable=False),
        sa.Column("provider", sa.String(length=50), nullable=False),
        sa.Column("value", sa.String(length=200), nullable=False),
        sa.ForeignKeyConstraint(["series_id"], ["series_v2.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["source_series_id"], ["source_series_v2.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("provider", "value", name="uq_external_identifier_v2_provider_value"),
        sa.UniqueConstraint(
            "source_series_id", "provider", name="uq_external_identifier_v2_source_provider"
        ),
    )
    op.create_index("ix_external_identifier_v2_series_id", "external_identifier_v2", ["series_id"])
    op.create_index(
        "ix_external_identifier_v2_source_series_id", "external_identifier_v2", ["source_series_id"]
    )
    op.create_index("ix_external_identifier_v2_provider", "external_identifier_v2", ["provider"])
    op.create_index("ix_external_identifier_v2_value", "external_identifier_v2", ["value"])

    op.create_table(
        "chapter_v2",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("series_id", sa.Integer(), nullable=False),
        sa.Column("canonical_number", sa.String(length=100), nullable=False),
        sa.Column("display_number", sa.String(length=100), nullable=False),
        sa.Column("sort_number", sa.Numeric(precision=20, scale=6), nullable=True),
        sa.Column("title", sa.String(length=500), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["series_id"], ["series_v2.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("series_id", "canonical_number", name="uq_chapter_v2_series_number"),
    )
    op.create_index("ix_chapter_v2_series_id", "chapter_v2", ["series_id"])
    op.create_index("ix_chapter_v2_series_sort", "chapter_v2", ["series_id", "sort_number"])

    op.create_table(
        "chapter_release_v2",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("chapter_id", sa.Integer(), nullable=False),
        sa.Column("source_series_id", sa.Integer(), nullable=False),
        sa.Column("source", sa.String(length=50), nullable=False),
        sa.Column("source_release_id", sa.String(length=500), nullable=False),
        sa.Column("title", sa.String(length=500), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("downloadable_after", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["chapter_id"], ["chapter_v2.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["source_series_id"], ["source_series_v2.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "source_series_id", "source_release_id", name="uq_chapter_release_v2_identity"
        ),
    )
    op.create_index("ix_chapter_release_v2_chapter_id", "chapter_release_v2", ["chapter_id"])
    op.create_index(
        "ix_chapter_release_v2_source_series_id", "chapter_release_v2", ["source_series_id"]
    )
    op.create_index("ix_chapter_release_v2_source", "chapter_release_v2", ["source"])
    op.create_index(
        "ix_chapter_release_v2_source_published",
        "chapter_release_v2",
        ["source", "published_at"],
    )

    op.create_table(
        "source_state_v2",
        sa.Column("source", sa.String(length=50), nullable=False),
        sa.Column("manual_enabled", sa.Boolean(), nullable=False),
        sa.Column("health_status", sa.String(length=20), nullable=False),
        sa.Column("consecutive_failures", sa.Integer(), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=False),
        sa.Column("cursor_json", json_type(), nullable=False),
        sa.Column("frontier_json", json_type(), nullable=False),
        sa.Column("cooldown_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_poll_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "health_status IN ('healthy', 'degraded', 'cooldown')",
            name="ck_source_state_v2_health",
        ),
        sa.PrimaryKeyConstraint("source"),
    )
    op.create_index("ix_source_state_v2_health_status", "source_state_v2", ["health_status"])


def downgrade() -> None:
    op.drop_index("ix_source_state_v2_health_status", table_name="source_state_v2")
    op.drop_table("source_state_v2")
    op.drop_index("ix_chapter_release_v2_source_published", table_name="chapter_release_v2")
    op.drop_index("ix_chapter_release_v2_source", table_name="chapter_release_v2")
    op.drop_index("ix_chapter_release_v2_source_series_id", table_name="chapter_release_v2")
    op.drop_index("ix_chapter_release_v2_chapter_id", table_name="chapter_release_v2")
    op.drop_table("chapter_release_v2")
    op.drop_index("ix_chapter_v2_series_sort", table_name="chapter_v2")
    op.drop_index("ix_chapter_v2_series_id", table_name="chapter_v2")
    op.drop_table("chapter_v2")
    op.drop_index("ix_external_identifier_v2_value", table_name="external_identifier_v2")
    op.drop_index("ix_external_identifier_v2_provider", table_name="external_identifier_v2")
    op.drop_index("ix_external_identifier_v2_source_series_id", table_name="external_identifier_v2")
    op.drop_index("ix_external_identifier_v2_series_id", table_name="external_identifier_v2")
    op.drop_table("external_identifier_v2")
    op.drop_index("ix_series_alias_v2_normalized_value", table_name="series_alias_v2")
    op.drop_index("ix_series_alias_v2_source_series_id", table_name="series_alias_v2")
    op.drop_index("ix_series_alias_v2_series_id", table_name="series_alias_v2")
    op.drop_table("series_alias_v2")
    op.drop_index("ix_source_series_v2_source_checked", table_name="source_series_v2")
    op.drop_index("ix_source_series_v2_normalized_title", table_name="source_series_v2")
    op.drop_index("ix_source_series_v2_source", table_name="source_series_v2")
    op.drop_index("ix_source_series_v2_series_id", table_name="source_series_v2")
    op.drop_table("source_series_v2")
    op.drop_index("ix_series_v2_status", table_name="series_v2")
    op.drop_index("ix_series_v2_normalized_title", table_name="series_v2")
    op.drop_index("ix_series_v2_title", table_name="series_v2")
    op.drop_index("ix_series_v2_storage_key", table_name="series_v2")
    op.drop_table("series_v2")
