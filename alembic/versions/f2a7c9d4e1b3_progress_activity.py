"""progress and activity tables

Revision ID: f2a7c9d4e1b3
Revises: e4b8c2d1a7f6
Create Date: 2026-07-08 21:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "f2a7c9d4e1b3"
down_revision = "e4b8c2d1a7f6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    tables = set(inspector.get_table_names())
    if "series_progress" not in tables:
        op.create_table(
            "series_progress",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("series_id", sa.Integer(), nullable=False),
            sa.Column("status", sa.String(length=30), nullable=False),
            sa.Column("note", sa.Text(), nullable=False),
            sa.Column("rating", sa.Integer(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
            sa.ForeignKeyConstraint(["series_id"], ["series.id"]),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("series_id", name="uq_series_progress_series"),
        )
        op.create_index(op.f("ix_series_progress_series_id"), "series_progress", ["series_id"])
        op.create_index(op.f("ix_series_progress_status"), "series_progress", ["status"])
    if "chapter_progress" not in tables:
        op.create_table(
            "chapter_progress",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("chapter_id", sa.Integer(), nullable=False),
            sa.Column("status", sa.String(length=30), nullable=False),
            sa.Column("read_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
            sa.ForeignKeyConstraint(["chapter_id"], ["chapter.id"]),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("chapter_id", name="uq_chapter_progress_chapter"),
        )
        op.create_index(op.f("ix_chapter_progress_chapter_id"), "chapter_progress", ["chapter_id"])
        op.create_index(op.f("ix_chapter_progress_status"), "chapter_progress", ["status"])
    if "activity_event" not in tables:
        op.create_table(
            "activity_event",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("kind", sa.String(length=50), nullable=False),
            sa.Column("status", sa.String(length=30), nullable=False),
            sa.Column("message", sa.Text(), nullable=False),
            sa.Column("source", sa.String(length=50), nullable=False),
            sa.Column("series_id", sa.Integer(), nullable=True),
            sa.Column("chapter_id", sa.Integer(), nullable=True),
            sa.Column("download_job_id", sa.Integer(), nullable=True),
            sa.Column("kavita_sync_job_id", sa.Integer(), nullable=True),
            sa.Column("metadata_json", sa.Text(), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.ForeignKeyConstraint(["chapter_id"], ["chapter.id"]),
            sa.ForeignKeyConstraint(["download_job_id"], ["download_job.id"]),
            sa.ForeignKeyConstraint(["kavita_sync_job_id"], ["kavita_sync_job.id"]),
            sa.ForeignKeyConstraint(["series_id"], ["series.id"]),
            sa.PrimaryKeyConstraint("id"),
        )
        op.create_index(op.f("ix_activity_event_kind"), "activity_event", ["kind"])
        op.create_index(op.f("ix_activity_event_status"), "activity_event", ["status"])
        op.create_index(op.f("ix_activity_event_source"), "activity_event", ["source"])
        op.create_index(op.f("ix_activity_event_series_id"), "activity_event", ["series_id"])
        op.create_index(op.f("ix_activity_event_chapter_id"), "activity_event", ["chapter_id"])
        op.create_index(op.f("ix_activity_event_download_job_id"), "activity_event", ["download_job_id"])
        op.create_index(
            op.f("ix_activity_event_kavita_sync_job_id"),
            "activity_event",
            ["kavita_sync_job_id"],
        )
        op.create_index(op.f("ix_activity_event_created_at"), "activity_event", ["created_at"])


def downgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    tables = set(inspector.get_table_names())
    if "activity_event" in tables:
        op.drop_index(op.f("ix_activity_event_created_at"), table_name="activity_event")
        op.drop_index(op.f("ix_activity_event_kavita_sync_job_id"), table_name="activity_event")
        op.drop_index(op.f("ix_activity_event_download_job_id"), table_name="activity_event")
        op.drop_index(op.f("ix_activity_event_chapter_id"), table_name="activity_event")
        op.drop_index(op.f("ix_activity_event_series_id"), table_name="activity_event")
        op.drop_index(op.f("ix_activity_event_source"), table_name="activity_event")
        op.drop_index(op.f("ix_activity_event_status"), table_name="activity_event")
        op.drop_index(op.f("ix_activity_event_kind"), table_name="activity_event")
        op.drop_table("activity_event")
    if "chapter_progress" in tables:
        op.drop_index(op.f("ix_chapter_progress_status"), table_name="chapter_progress")
        op.drop_index(op.f("ix_chapter_progress_chapter_id"), table_name="chapter_progress")
        op.drop_table("chapter_progress")
    if "series_progress" in tables:
        op.drop_index(op.f("ix_series_progress_status"), table_name="series_progress")
        op.drop_index(op.f("ix_series_progress_series_id"), table_name="series_progress")
        op.drop_table("series_progress")
