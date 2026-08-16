"""catalog integrity, decisions, observations, and search

Revision ID: 0007_catalog_integrity_search
Revises: 0006_job_routing_permits
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "0007_catalog_integrity_search"
down_revision: Union[str, None] = "0006_job_routing_permits"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "series_v2",
        sa.Column("integrity_state", sa.String(20), nullable=False, server_default="unknown"),
    )
    op.add_column(
        "series_v2", sa.Column("latest_release_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.add_column(
        "series_v2",
        sa.Column("latest_release_number", sa.String(100), nullable=False, server_default=""),
    )
    op.add_column(
        "series_v2",
        sa.Column("latest_release_source", sa.String(50), nullable=False, server_default=""),
    )
    if op.get_bind().dialect.name == "postgresql":
        op.create_check_constraint(
            "ck_series_v2_integrity_state",
            "series_v2",
            "integrity_state IN ('unknown', 'healthy', 'attention', 'quarantined')",
        )
    op.create_index("ix_series_v2_integrity_state", "series_v2", ["integrity_state"])
    op.create_index("ix_series_v2_latest_release_at", "series_v2", ["latest_release_at"])
    op.create_index("ix_series_v2_latest_cursor", "series_v2", ["latest_release_at", "id"])
    json_type = sa.JSON().with_variant(postgresql.JSONB(none_as_null=True), "postgresql")
    op.create_table(
        "match_decision_v2",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "left_source_series_id",
            sa.Integer(),
            sa.ForeignKey("source_series_v2.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "right_source_series_id",
            sa.Integer(),
            sa.ForeignKey("source_series_v2.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("decision", sa.String(20), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("evidence_json", json_type, nullable=False),
        sa.Column("decided_by", sa.String(100), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "decision IN ('pending', 'accepted', 'rejected')", name="ck_match_decision_v2_decision"
        ),
        sa.UniqueConstraint(
            "left_source_series_id", "right_source_series_id", name="uq_match_decision_v2_pair"
        ),
    )
    op.create_index(
        "ix_match_decision_v2_left_source_series_id", "match_decision_v2", ["left_source_series_id"]
    )
    op.create_index(
        "ix_match_decision_v2_right_source_series_id",
        "match_decision_v2",
        ["right_source_series_id"],
    )
    op.create_index("ix_match_decision_v2_decision", "match_decision_v2", ["decision"])
    op.create_index(
        "ix_match_decision_v2_status_confidence", "match_decision_v2", ["decision", "confidence"]
    )
    op.create_table(
        "catalog_observation_v2",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("source", sa.String(50), nullable=False),
        sa.Column("observation_type", sa.String(50), nullable=False),
        sa.Column("source_key", sa.String(500), nullable=False),
        sa.Column("state", sa.String(20), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("payload_json", json_type, nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "state IN ('observed', 'accepted', 'quarantined', 'rejected')",
            name="ck_catalog_observation_v2_state",
        ),
    )
    op.create_index("ix_catalog_observation_v2_source", "catalog_observation_v2", ["source"])
    op.create_index(
        "ix_catalog_observation_v2_observation_type", "catalog_observation_v2", ["observation_type"]
    )
    op.create_index("ix_catalog_observation_v2_state", "catalog_observation_v2", ["state"])
    op.create_index(
        "ix_catalog_observation_v2_source_state",
        "catalog_observation_v2",
        ["source", "state", "observed_at"],
    )
    if op.get_bind().dialect.name == "postgresql":
        op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
        op.execute(
            "CREATE INDEX ix_series_v2_title_trgm ON series_v2 USING gin (title gin_trgm_ops)"
        )
        op.execute(
            "CREATE INDEX ix_series_v2_normalized_title_trgm ON series_v2 USING gin (normalized_title gin_trgm_ops)"
        )


def downgrade() -> None:
    if op.get_bind().dialect.name == "postgresql":
        op.execute("DROP INDEX IF EXISTS ix_series_v2_normalized_title_trgm")
        op.execute("DROP INDEX IF EXISTS ix_series_v2_title_trgm")
    op.drop_table("catalog_observation_v2")
    op.drop_table("match_decision_v2")
    op.drop_index("ix_series_v2_latest_cursor", table_name="series_v2")
    op.drop_index("ix_series_v2_latest_release_at", table_name="series_v2")
    op.drop_index("ix_series_v2_integrity_state", table_name="series_v2")
    if op.get_bind().dialect.name == "postgresql":
        op.drop_constraint("ck_series_v2_integrity_state", "series_v2", type_="check")
    op.drop_column("series_v2", "latest_release_source")
    op.drop_column("series_v2", "latest_release_number")
    op.drop_column("series_v2", "latest_release_at")
    op.drop_column("series_v2", "integrity_state")
