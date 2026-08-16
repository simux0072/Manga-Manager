"""fan-out refresh jobs, provider hygiene, and shared storage capacity

Revision ID: 0016_refresh_storage_hardening
Revises: 0015_fallback_covers_parallelism
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0016_refresh_storage_hardening"
down_revision: Union[str, None] = "0015_fallback_covers_parallelism"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

KNOWN = "'asura', 'kingofshojo', 'mangafire'"


def upgrade() -> None:
    # CDN hosts were briefly recorded as providers by cover fingerprint traffic.
    op.execute(f"DELETE FROM provider_request_sample WHERE source NOT IN ({KNOWN})")
    op.execute(f"DELETE FROM provider_endpoint_state WHERE source NOT IN ({KNOWN})")
    op.execute(f"DELETE FROM provider_policy WHERE source NOT IN ({KNOWN})")
    op.execute(f"DELETE FROM source_state_v2 WHERE source NOT IN ({KNOWN})")

    with op.batch_alter_table("job") as batch:
        batch.drop_constraint("ck_job_kind", type_="check")
        batch.create_check_constraint(
            "ck_job_kind",
            "kind IN ('source_pull', 'source_refresh', 'chapter_download', "
            "'kavita_sync', 'maintenance', 'notification')",
        )
    for table, name in (
        ("source_state_v2", "ck_source_state_known_provider"),
        ("provider_policy", "ck_provider_policy_known_provider"),
        ("provider_endpoint_state", "ck_provider_endpoint_known_provider"),
        ("provider_request_sample", "ck_provider_sample_known_provider"),
    ):
        with op.batch_alter_table(table) as batch:
            batch.create_check_constraint(name, f"source IN ({KNOWN})")

    op.create_table(
        "storage_state",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("paused", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("free_bytes", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("min_free_bytes", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("reserved_bytes", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("reason", sa.Text(), nullable=False, server_default=""),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("id = 1", name="ck_storage_state_singleton"),
    )
    op.create_index("ix_storage_state_paused", "storage_state", ["paused"])
    op.create_table(
        "storage_reservation",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("job_id", sa.Integer(), nullable=False, unique=True),
        sa.Column("owner", sa.String(200), nullable=False),
        sa.Column("reserved_bytes", sa.BigInteger(), nullable=False),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["job_id"], ["job.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_storage_reservation_job_id", "storage_reservation", ["job_id"])
    op.create_index("ix_storage_reservation_expiry", "storage_reservation", ["lease_expires_at"])

    # Preserve events but resolve jobs produced by the old untyped watermark loop.
    op.execute(
        "UPDATE job SET status='cancelled', error_code='superseded_storage_capacity', "
        "completed_at=COALESCE(completed_at, CURRENT_TIMESTAMP), updated_at=CURRENT_TIMESTAMP "
        "WHERE status='failed' AND kind='chapter_download' AND "
        "(error_message LIKE '%storage free-space watermark%' OR "
        "error_message LIKE '%storage capacity unavailable%')"
    )
    op.execute(
        "UPDATE chapter_download_intent SET job_id=NULL, "
        "state=CASE WHEN tier='backfill' THEN 'blocked' ELSE 'pending' END, "
        "updated_at=CURRENT_TIMESTAMP WHERE job_id IN "
        "(SELECT id FROM job WHERE error_code='superseded_storage_capacity')"
    )
    op.execute(
        "UPDATE job SET status='cancelled', error_code='superseded_source_pull', "
        "completed_at=COALESCE(completed_at, CURRENT_TIMESTAMP), updated_at=CURRENT_TIMESTAMP "
        "WHERE status='failed' AND kind='source_pull'"
    )


def downgrade() -> None:
    op.drop_table("storage_reservation")
    op.drop_table("storage_state")
    for table, name in (
        ("provider_request_sample", "ck_provider_sample_known_provider"),
        ("provider_endpoint_state", "ck_provider_endpoint_known_provider"),
        ("provider_policy", "ck_provider_policy_known_provider"),
        ("source_state_v2", "ck_source_state_known_provider"),
    ):
        with op.batch_alter_table(table) as batch:
            batch.drop_constraint(name, type_="check")
    op.execute("DELETE FROM job WHERE kind = 'source_refresh'")
    with op.batch_alter_table("job") as batch:
        batch.drop_constraint("ck_job_kind", type_="check")
        batch.create_check_constraint(
            "ck_job_kind",
            "kind IN ('source_pull', 'chapter_download', 'kavita_sync', "
            "'maintenance', 'notification')",
        )
