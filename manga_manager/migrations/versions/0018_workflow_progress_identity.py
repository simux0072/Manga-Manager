"""durable workflow progress, retention aggregates, and provider identity normalization

Revision ID: 0018_workflow_progress_identity
Revises: 0017_catalog_recovery_matching
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "0018_workflow_progress_identity"
down_revision: Union[str, None] = "0017_catalog_recovery_matching"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "workload_cycle",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("status", sa.String(20), nullable=False, server_default="active"),
        sa.Column("total_units", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("successful_units", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("failed_units", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("cancelled_units", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("added_units", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("settled_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("status IN ('active', 'settled')", name="ck_workload_cycle_status"),
    )
    op.create_index(
        "uq_workload_cycle_active",
        "workload_cycle",
        ["status"],
        unique=True,
        postgresql_where=sa.text("status = 'active'"),
        sqlite_where=sa.text("status = 'active'"),
    )
    op.create_table(
        "job_daily_aggregate",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("day", sa.DateTime(timezone=True), nullable=False),
        sa.Column("kind", sa.String(30), nullable=False),
        sa.Column("source", sa.String(50), nullable=False, server_default=""),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("error_code", sa.String(100), nullable=False, server_default=""),
        sa.Column("job_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("duration_seconds", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "day", "kind", "source", "status", "error_code",
            name="uq_job_daily_aggregate_bucket",
        ),
    )
    op.create_index("ix_job_daily_aggregate_day", "job_daily_aggregate", ["day"])

    with op.batch_alter_table("job") as batch:
        batch.add_column(sa.Column("cycle_id", sa.Integer(), nullable=True))
        batch.add_column(sa.Column("workflow_key", sa.String(100), nullable=False, server_default=""))
        batch.add_column(sa.Column("group_key", sa.String(500), nullable=False, server_default=""))
        batch.add_column(sa.Column("logical_units", sa.Integer(), nullable=False, server_default="1"))
        batch.add_column(
            sa.Column(
                "pending_payload",
                sa.JSON().with_variant(postgresql.JSONB(none_as_null=True), "postgresql"),
                nullable=False,
                server_default=sa.text("'{}'"),
            )
        )
        batch.create_foreign_key(
            "fk_job_cycle_id_workload_cycle", "workload_cycle", ["cycle_id"], ["id"],
            ondelete="SET NULL",
        )
        batch.drop_constraint("ck_job_kind", type_="check")
        batch.create_check_constraint(
            "ck_job_kind",
            "kind IN ('source_pull', 'source_refresh', 'chapter_download', 'kavita_sync', "
            "'library_repair', 'cover_backfill', 'maintenance', 'notification')",
        )
    op.create_index("ix_job_cycle_id", "job", ["cycle_id"])
    op.create_index("ix_job_workflow_key", "job", ["workflow_key"])
    op.create_index("ix_job_group_key", "job", ["group_key"])
    op.create_index("ix_job_cycle_group_status", "job", ["cycle_id", "group_key", "status"])
    op.create_index("ix_job_workflow_status", "job", ["workflow_key", "status"])

    with op.batch_alter_table("job_event") as batch:
        batch.drop_constraint("ck_job_event_type", type_="check")
        batch.create_check_constraint(
            "ck_job_event_type",
            "event_type IN ('enqueued', 'leased', 'progress', 'retry_scheduled', "
            "'succeeded', 'failed', 'cancelled', 'released', 'lease_expired', 'rerouted')",
        )

    with op.batch_alter_table("source_series_v2") as batch:
        batch.add_column(
            sa.Column("normalized_source_id", sa.String(500), nullable=False, server_default="")
        )
        batch.add_column(
            sa.Column("revision_override", sa.String(20), nullable=False, server_default="")
        )
    op.create_index(
        "ix_source_series_v2_normalized_identity",
        "source_series_v2",
        ["source", "normalized_source_id"],
    )

    op.execute(
        """
        INSERT INTO workload_cycle
            (status, total_units, successful_units, failed_units, cancelled_units,
             added_units, started_at, updated_at)
        SELECT 'active', count(*), 0, 0, 0, count(*), CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        FROM job WHERE status IN ('queued', 'leased', 'retry_wait')
        HAVING count(*) > 0
        """
    )
    op.execute(
        """
        UPDATE job SET cycle_id = (SELECT max(id) FROM workload_cycle),
            workflow_key = CASE
                WHEN kind IN ('source_pull', 'source_refresh') THEN 'pull-' || source || '-legacy'
                ELSE '' END,
            group_key = CASE
                WHEN kind = 'chapter_download' AND series_key <> '' THEN 'download:' || series_key
                WHEN kind IN ('source_pull', 'source_refresh') THEN 'pull-' || source || '-legacy'
                WHEN kind = 'library_repair' AND series_key <> '' THEN 'repair:' || series_key
                ELSE kind || ':' || dedupe_key END
        WHERE status IN ('queued', 'leased', 'retry_wait')
        """
    )
    op.execute("UPDATE source_series_v2 SET normalized_source_id = source_id")


def downgrade() -> None:
    op.drop_index("ix_source_series_v2_normalized_identity", table_name="source_series_v2")
    with op.batch_alter_table("source_series_v2") as batch:
        batch.drop_column("revision_override")
        batch.drop_column("normalized_source_id")
    with op.batch_alter_table("job_event") as batch:
        batch.drop_constraint("ck_job_event_type", type_="check")
        batch.create_check_constraint(
            "ck_job_event_type",
            "event_type IN ('enqueued', 'leased', 'progress', 'retry_scheduled', "
            "'succeeded', 'failed', 'cancelled', 'released', 'lease_expired')",
        )
    op.drop_index("ix_job_workflow_status", table_name="job")
    op.drop_index("ix_job_cycle_group_status", table_name="job")
    op.drop_index("ix_job_group_key", table_name="job")
    op.drop_index("ix_job_workflow_key", table_name="job")
    op.drop_index("ix_job_cycle_id", table_name="job")
    with op.batch_alter_table("job") as batch:
        batch.drop_constraint("fk_job_cycle_id_workload_cycle", type_="foreignkey")
        batch.drop_column("pending_payload")
        batch.drop_column("logical_units")
        batch.drop_column("group_key")
        batch.drop_column("workflow_key")
        batch.drop_column("cycle_id")
        batch.drop_constraint("ck_job_kind", type_="check")
        batch.create_check_constraint(
            "ck_job_kind",
            "kind IN ('source_pull', 'source_refresh', 'chapter_download', 'kavita_sync', "
            "'library_repair', 'maintenance', 'notification')",
        )
    op.drop_table("job_daily_aggregate")
    op.drop_table("workload_cycle")
