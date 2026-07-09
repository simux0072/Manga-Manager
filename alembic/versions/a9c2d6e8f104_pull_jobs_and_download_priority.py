"""pull jobs and download priority

Revision ID: a9c2d6e8f104
Revises: f2a7c9d4e1b3
Create Date: 2026-07-08 22:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "a9c2d6e8f104"
down_revision = "f2a7c9d4e1b3"
branch_labels = None
depends_on = None


def deduplicate_active_pull_jobs_sql() -> str:
    return (
        "UPDATE source_pull_job AS job "
        "SET status = 'failed', "
        "error = CASE "
        "WHEN COALESCE(error, '') = '' THEN 'deduplicated before active pull job constraint' "
        "ELSE error END, "
        "updated_at = CURRENT_TIMESTAMP, "
        "completed_at = COALESCE(completed_at, CURRENT_TIMESTAMP) "
        "WHERE status IN ('queued', 'running') "
        "AND EXISTS ("
        "SELECT 1 FROM source_pull_job AS newer "
        "WHERE newer.source = job.source "
        "AND newer.status IN ('queued', 'running') "
        "AND ("
        "newer.created_at > job.created_at "
        "OR (newer.created_at = job.created_at AND newer.id > job.id)"
        ")"
        ")"
    )


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    tables = set(inspector.get_table_names())
    download_columns = (
        {column["name"] for column in inspector.get_columns("download_job")}
        if "download_job" in tables
        else set()
    )
    if "priority" not in download_columns:
        op.add_column("download_job", sa.Column("priority", sa.Integer(), nullable=False, server_default="100"))
        op.create_index(op.f("ix_download_job_priority"), "download_job", ["priority"])
    if "job_type" not in download_columns:
        op.add_column("download_job", sa.Column("job_type", sa.String(length=30), nullable=False, server_default="normal"))
        op.create_index(op.f("ix_download_job_job_type"), "download_job", ["job_type"])
    if "source_pull_job" not in tables:
        op.create_table(
            "source_pull_job",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("source", sa.String(length=50), nullable=False),
            sa.Column("status", sa.String(length=30), nullable=False),
            sa.Column("total_items", sa.Integer(), nullable=False),
            sa.Column("processed_items", sa.Integer(), nullable=False),
            sa.Column("error", sa.Text(), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
            sa.PrimaryKeyConstraint("id"),
        )
        op.create_index(op.f("ix_source_pull_job_source"), "source_pull_job", ["source"])
        op.create_index(op.f("ix_source_pull_job_status"), "source_pull_job", ["status"])
        op.create_index(op.f("ix_source_pull_job_created_at"), "source_pull_job", ["created_at"])
        op.create_index(
            "uq_source_pull_job_active_source",
            "source_pull_job",
            ["source"],
            unique=True,
            sqlite_where=sa.text("status IN ('queued', 'running')"),
            postgresql_where=sa.text("status IN ('queued', 'running')"),
        )
    else:
        indexes = {index["name"] for index in inspector.get_indexes("source_pull_job") if index.get("name")}
        if "uq_source_pull_job_active_source" not in indexes:
            op.execute(sa.text(deduplicate_active_pull_jobs_sql()))
            op.create_index(
                "uq_source_pull_job_active_source",
                "source_pull_job",
                ["source"],
                unique=True,
                sqlite_where=sa.text("status IN ('queued', 'running')"),
                postgresql_where=sa.text("status IN ('queued', 'running')"),
            )


def downgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    tables = set(inspector.get_table_names())
    if "source_pull_job" in tables:
        indexes = {index["name"] for index in inspector.get_indexes("source_pull_job") if index.get("name")}
        if "uq_source_pull_job_active_source" in indexes:
            op.drop_index("uq_source_pull_job_active_source", table_name="source_pull_job")
        op.drop_index(op.f("ix_source_pull_job_created_at"), table_name="source_pull_job")
        op.drop_index(op.f("ix_source_pull_job_status"), table_name="source_pull_job")
        op.drop_index(op.f("ix_source_pull_job_source"), table_name="source_pull_job")
        op.drop_table("source_pull_job")
    download_columns = (
        {column["name"] for column in inspector.get_columns("download_job")}
        if "download_job" in tables
        else set()
    )
    if "job_type" in download_columns:
        op.drop_index(op.f("ix_download_job_job_type"), table_name="download_job")
        op.drop_column("download_job", "job_type")
    if "priority" in download_columns:
        op.drop_index(op.f("ix_download_job_priority"), table_name="download_job")
        op.drop_column("download_job", "priority")
