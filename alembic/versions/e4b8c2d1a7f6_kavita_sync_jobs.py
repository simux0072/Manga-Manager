"""kavita sync jobs

Revision ID: e4b8c2d1a7f6
Revises: d1f9c0a4e8b2
Create Date: 2026-07-08 19:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "e4b8c2d1a7f6"
down_revision = "d1f9c0a4e8b2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    if "kavita_sync_job" in set(inspector.get_table_names()):
        return
    op.create_table(
        "kavita_sync_job",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("series_id", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=30), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("error", sa.Text(), nullable=False),
        sa.Column("retry_after", sa.DateTime(timezone=True), nullable=True),
        sa.Column("folder_path", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["series_id"], ["series.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("series_id", name="uq_kavita_sync_job_series"),
    )
    op.create_index(op.f("ix_kavita_sync_job_series_id"), "kavita_sync_job", ["series_id"])
    op.create_index(op.f("ix_kavita_sync_job_status"), "kavita_sync_job", ["status"])


def downgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    if "kavita_sync_job" not in set(inspector.get_table_names()):
        return
    op.drop_index(op.f("ix_kavita_sync_job_status"), table_name="kavita_sync_job")
    op.drop_index(op.f("ix_kavita_sync_job_series_id"), table_name="kavita_sync_job")
    op.drop_table("kavita_sync_job")
