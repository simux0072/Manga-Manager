"""durable job queue

Revision ID: 0001_durable_job_queue
Revises:
Create Date: 2026-07-10 12:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "0001_durable_job_queue"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    payload_type = sa.JSON().with_variant(postgresql.JSONB(none_as_null=True), "postgresql")
    op.create_table(
        "job",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("kind", sa.String(length=30), nullable=False),
        sa.Column("dedupe_key", sa.String(length=500), nullable=False),
        sa.Column("payload", payload_type, nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("priority", sa.Integer(), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("max_attempts", sa.Integer(), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("lease_owner", sa.String(length=200), nullable=False),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_code", sa.String(length=100), nullable=False),
        sa.Column("error_message", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("attempts >= 0", name="ck_job_attempts_nonnegative"),
        sa.CheckConstraint("dedupe_key <> ''", name="ck_job_dedupe_key_not_empty"),
        sa.CheckConstraint(
            "kind IN ('source_pull', 'chapter_download', 'kavita_sync', "
            "'maintenance', 'notification')",
            name="ck_job_kind",
        ),
        sa.CheckConstraint(
            "status <> 'leased' OR (lease_owner <> '' AND lease_expires_at IS NOT NULL)",
            name="ck_job_lease_fields",
        ),
        sa.CheckConstraint("max_attempts >= 1", name="ck_job_max_attempts_positive"),
        sa.CheckConstraint(
            "status IN ('queued', 'leased', 'retry_wait', 'succeeded', 'failed', 'cancelled')",
            name="ck_job_status",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_job_kind", "job", ["kind"])
    op.create_index("ix_job_status", "job", ["status"])
    op.create_index(
        "ix_job_claim",
        "job",
        ["status", "available_at", "priority", "created_at"],
    )
    op.create_index("ix_job_lease_expiry", "job", ["status", "lease_expires_at"])
    active = sa.text("status IN ('queued', 'leased', 'retry_wait')")
    op.create_index(
        "uq_job_active_dedupe",
        "job",
        ["kind", "dedupe_key"],
        unique=True,
        sqlite_where=active,
        postgresql_where=active,
    )


def downgrade() -> None:
    op.drop_index("uq_job_active_dedupe", table_name="job")
    op.drop_index("ix_job_lease_expiry", table_name="job")
    op.drop_index("ix_job_claim", table_name="job")
    op.drop_index("ix_job_status", table_name="job")
    op.drop_index("ix_job_kind", table_name="job")
    op.drop_table("job")

