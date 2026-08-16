"""job events and worker heartbeats

Revision ID: 0002_job_events_workers
Revises: 0001_durable_job_queue
Create Date: 2026-07-10 13:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "0002_job_events_workers"
down_revision: Union[str, None] = "0001_durable_job_queue"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    details_type = sa.JSON().with_variant(postgresql.JSONB(none_as_null=True), "postgresql")
    op.create_table(
        "job_event",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("job_id", sa.Integer(), nullable=False),
        sa.Column("event_type", sa.String(length=30), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("owner", sa.String(length=200), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("details", details_type, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "event_type IN ('enqueued', 'leased', 'progress', 'retry_scheduled', 'succeeded', "
            "'failed', 'cancelled', 'released', 'lease_expired')",
            name="ck_job_event_type",
        ),
        sa.ForeignKeyConstraint(["job_id"], ["job.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_job_event_job_id", "job_event", ["job_id"])
    op.create_index("ix_job_event_job_created", "job_event", ["job_id", "created_at"])
    op.create_index("ix_job_event_created", "job_event", ["created_at"])

    op.create_table(
        "worker_heartbeat",
        sa.Column("worker_id", sa.String(length=200), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("active_job_id", sa.Integer(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("stopped_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("metadata_json", details_type, nullable=False),
        sa.CheckConstraint(
            "status IN ('starting', 'running', 'draining', 'stopped')",
            name="ck_worker_heartbeat_status",
        ),
        sa.ForeignKeyConstraint(["active_job_id"], ["job.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("worker_id"),
    )
    op.create_index("ix_worker_heartbeat_active_job_id", "worker_heartbeat", ["active_job_id"])
    op.create_index("ix_worker_heartbeat_seen", "worker_heartbeat", ["heartbeat_at"])


def downgrade() -> None:
    op.drop_index("ix_worker_heartbeat_seen", table_name="worker_heartbeat")
    op.drop_index("ix_worker_heartbeat_active_job_id", table_name="worker_heartbeat")
    op.drop_table("worker_heartbeat")
    op.drop_index("ix_job_event_created", table_name="job_event")
    op.drop_index("ix_job_event_job_created", table_name="job_event")
    op.drop_index("ix_job_event_job_id", table_name="job_event")
    op.drop_table("job_event")
