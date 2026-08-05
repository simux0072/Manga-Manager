"""durable asynchronous match operations

Revision ID: 0026_async_match_operations
Revises: 0025_shared_worker_fairness
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "0026_async_match_operations"
down_revision: Union[str, None] = "0025_shared_worker_fairness"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("job") as batch:
        batch.drop_constraint("ck_job_kind", type_="check")
        batch.create_check_constraint(
            "ck_job_kind",
            "kind IN ('source_pull', 'source_refresh', 'chapter_download', 'kavita_sync', "
            "'library_repair', 'cover_backfill', 'maintenance', 'notification', "
            "'match_operation')",
        )
    json_type = sa.JSON().with_variant(postgresql.JSONB(none_as_null=True), "postgresql")
    op.create_table(
        "match_operation",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("action", sa.String(20), nullable=False),
        sa.Column("status", sa.String(20), nullable=False, server_default="queued"),
        sa.Column("representative_id", sa.Integer(), nullable=False),
        sa.Column("decision_ids", json_type, nullable=False, server_default=sa.text("'[]'")),
        sa.Column("series_ids", json_type, nullable=False, server_default=sa.text("'[]'")),
        sa.Column("proposal_ids", json_type, nullable=False, server_default=sa.text("'[]'")),
        sa.Column("job_id", sa.Integer(), sa.ForeignKey("job.id", ondelete="SET NULL")),
        sa.Column("error_code", sa.String(100), nullable=False, server_default=""),
        sa.Column("error_message", sa.Text(), nullable=False, server_default=""),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint("action IN ('accepted', 'rejected')", name="ck_match_operation_action"),
        sa.CheckConstraint(
            "status IN ('queued', 'running', 'succeeded', 'failed')",
            name="ck_match_operation_status",
        ),
    )
    op.create_index("ix_match_operation_status", "match_operation", ["status"])
    op.create_index(
        "ix_match_operation_status_created", "match_operation", ["status", "created_at"]
    )
    op.create_index(
        "ix_match_operation_representative",
        "match_operation",
        ["representative_id", "created_at"],
    )
    op.create_index("ix_match_operation_job_id", "match_operation", ["job_id"])
    op.create_index(
        "ix_match_operation_representative_id", "match_operation", ["representative_id"]
    )
    op.create_table(
        "match_operation_series",
        sa.Column(
            "operation_id",
            sa.Integer(),
            sa.ForeignKey("match_operation.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column(
            "series_id",
            sa.Integer(),
            sa.ForeignKey("series_v2.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("series_id", name="uq_match_operation_series_active"),
    )
    op.create_index(
        "ix_match_operation_series_operation", "match_operation_series", ["operation_id"]
    )


def downgrade() -> None:
    op.drop_table("match_operation_series")
    op.drop_table("match_operation")
    with op.batch_alter_table("job") as batch:
        batch.drop_constraint("ck_job_kind", type_="check")
        batch.create_check_constraint(
            "ck_job_kind",
            "kind IN ('source_pull', 'source_refresh', 'chapter_download', 'kavita_sync', "
            "'library_repair', 'cover_backfill', 'maintenance', 'notification')",
        )
