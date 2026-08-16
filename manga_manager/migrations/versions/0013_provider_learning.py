"""provider telemetry and learned policies

Revision ID: 0013_provider_learning
Revises: 0012_download_plans_progress
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "0013_provider_learning"
down_revision: Union[str, None] = "0012_download_plans_progress"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    json_type = sa.JSON().with_variant(postgresql.JSONB(none_as_null=True), "postgresql")
    op.create_table(
        "provider_policy",
        sa.Column("source", sa.String(50), primary_key=True),
        sa.Column("learned_job_limit", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("learned_page_limit", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("request_interval_seconds", sa.Float(), nullable=False, server_default="0"),
        sa.Column("cooldown_seconds", sa.Integer(), nullable=False, server_default="300"),
        sa.Column("clean_since", sa.DateTime(timezone=True)),
        sa.Column("last_limited_at", sa.DateTime(timezone=True)),
        sa.Column("next_exploration_at", sa.DateTime(timezone=True)),
        sa.Column("expires_at", sa.DateTime(timezone=True)),
        sa.Column("successful_tier_runs", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("metadata_json", json_type, nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "provider_benchmark_run",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("source", sa.String(50), nullable=False),
        sa.Column("state", sa.String(20), nullable=False, server_default="running"),
        sa.Column("requested_tier", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("stable_tier", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("request_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("success_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("failure_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("limiting_signal", sa.Text(), nullable=False, server_default=""),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("report_json", json_type, nullable=False),
    )
    op.create_index("ix_provider_benchmark_run_source", "provider_benchmark_run", ["source"])
    op.create_index("ix_provider_benchmark_run_state", "provider_benchmark_run", ["state"])
    op.create_index(
        "ix_provider_benchmark_source_started", "provider_benchmark_run", ["source", "started_at"]
    )
    op.create_table(
        "provider_request_sample",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("run_id", sa.Integer(), nullable=False),
        sa.Column("source", sa.String(50), nullable=False),
        sa.Column("host", sa.String(255), nullable=False, server_default=""),
        sa.Column("status_code", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("latency_ms", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("byte_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_code", sa.String(100), nullable=False, server_default=""),
        sa.Column("retry_after_seconds", sa.Integer()),
        sa.Column("headers_json", json_type, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["provider_benchmark_run.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_provider_request_sample_run_id", "provider_request_sample", ["run_id"])
    op.create_index("ix_provider_request_sample_source", "provider_request_sample", ["source"])
    op.create_index(
        "ix_provider_sample_run_created", "provider_request_sample", ["run_id", "created_at"]
    )


def downgrade() -> None:
    op.drop_table("provider_request_sample")
    op.drop_table("provider_benchmark_run")
    op.drop_table("provider_policy")
