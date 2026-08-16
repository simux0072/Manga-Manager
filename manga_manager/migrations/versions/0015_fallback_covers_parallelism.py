"""fallback attempts, canonical covers, and Kavita cover state

Revision ID: 0015_fallback_covers_parallelism
Revises: 0014_matching_evidence
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0015_fallback_covers_parallelism"
down_revision: Union[str, None] = "0014_matching_evidence"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        "UPDATE job SET pool = 'pull:' || source WHERE kind = 'source_pull' AND source <> ''"
    )
    with op.batch_alter_table("provider_request_sample") as batch:
        batch.alter_column("run_id", existing_type=sa.Integer(), nullable=True)
    op.add_column(
        "series_v2", sa.Column("cover_checksum", sa.String(64), nullable=False, server_default="")
    )
    op.add_column(
        "series_v2", sa.Column("cover_relative_path", sa.Text(), nullable=False, server_default="")
    )
    op.add_column(
        "series_v2",
        sa.Column("kavita_cover_checksum", sa.String(64), nullable=False, server_default=""),
    )
    op.add_column(
        "chapter_v2",
        sa.Column("kavita_cover_checksum", sa.String(64), nullable=False, server_default=""),
    )
    op.create_table(
        "chapter_release_attempt",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("chapter_id", sa.Integer(), nullable=False),
        sa.Column("chapter_release_id", sa.Integer(), nullable=False),
        sa.Column("job_id", sa.Integer(), nullable=True),
        sa.Column("source", sa.String(50), nullable=False),
        sa.Column("outcome", sa.String(30), nullable=False),
        sa.Column("error_code", sa.String(100), nullable=False, server_default=""),
        sa.Column("error_message", sa.Text(), nullable=False, server_default=""),
        sa.Column("retry_after", sa.DateTime(timezone=True), nullable=True),
        sa.Column("details_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["chapter_id"], ["chapter_v2.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["chapter_release_id"], ["chapter_release_v2.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["job_id"], ["job.id"], ondelete="SET NULL"),
        sa.CheckConstraint(
            "outcome IN ('failed', 'fallback_queued', 'fallback_succeeded', 'succeeded', 'upgraded')",
            name="ck_chapter_release_attempt_outcome",
        ),
    )
    op.create_index(
        "ix_chapter_release_attempt_chapter_id", "chapter_release_attempt", ["chapter_id"]
    )
    op.create_index(
        "ix_chapter_release_attempt_chapter_release_id",
        "chapter_release_attempt",
        ["chapter_release_id"],
    )
    op.create_index("ix_chapter_release_attempt_job_id", "chapter_release_attempt", ["job_id"])
    op.create_index("ix_chapter_release_attempt_source", "chapter_release_attempt", ["source"])
    op.create_index("ix_chapter_release_attempt_outcome", "chapter_release_attempt", ["outcome"])
    op.create_index(
        "ix_chapter_release_attempt_chapter_created",
        "chapter_release_attempt",
        ["chapter_id", "created_at"],
    )
    op.create_index(
        "ix_chapter_release_attempt_release_outcome",
        "chapter_release_attempt",
        ["chapter_release_id", "outcome"],
    )
    op.create_table(
        "provider_endpoint_state",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("source", sa.String(50), nullable=False),
        sa.Column("traffic_class", sa.String(20), nullable=False),
        sa.Column("request_interval_seconds", sa.Float(), nullable=False, server_default="0"),
        sa.Column("next_request_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cooldown_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("consecutive_failures", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text(), nullable=False, server_default=""),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("source", "traffic_class", name="uq_provider_endpoint_source_class"),
    )
    op.create_index("ix_provider_endpoint_state_source", "provider_endpoint_state", ["source"])
    op.create_index(
        "ix_provider_endpoint_state_traffic_class", "provider_endpoint_state", ["traffic_class"]
    )
    op.create_index("ix_provider_endpoint_cooldown", "provider_endpoint_state", ["cooldown_until"])


def downgrade() -> None:
    op.execute("UPDATE job SET pool = 'source_pull' WHERE kind = 'source_pull'")
    op.drop_table("provider_endpoint_state")
    op.drop_table("chapter_release_attempt")
    op.drop_column("chapter_v2", "kavita_cover_checksum")
    op.drop_column("series_v2", "kavita_cover_checksum")
    op.drop_column("series_v2", "cover_relative_path")
    op.drop_column("series_v2", "cover_checksum")
    op.execute("DELETE FROM provider_request_sample WHERE run_id IS NULL")
    with op.batch_alter_table("provider_request_sample") as batch:
        batch.alter_column("run_id", existing_type=sa.Integer(), nullable=False)
