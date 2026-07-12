"""download plans and durable job progress

Revision ID: 0012_download_plans_progress
Revises: 0011_classify_legacy_failures
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0012_download_plans_progress"
down_revision: Union[str, None] = "0011_classify_legacy_failures"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("job", sa.Column("progress_phase", sa.String(50), nullable=False, server_default=""))
    op.add_column("job", sa.Column("progress_current", sa.Integer(), nullable=False, server_default="0"))
    op.add_column("job", sa.Column("progress_total", sa.Integer(), nullable=False, server_default="0"))
    op.add_column("job", sa.Column("progress_unit", sa.String(30), nullable=False, server_default=""))
    op.add_column("job", sa.Column("progress_bytes", sa.Integer(), nullable=False, server_default="0"))
    op.add_column("job", sa.Column("progress_message", sa.Text(), nullable=False, server_default=""))
    op.add_column("job", sa.Column("progress_updated_at", sa.DateTime(timezone=True), nullable=True))
    op.create_table(
        "series_download_plan",
        sa.Column("series_id", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(20), nullable=False, server_default="active"),
        sa.Column("phase", sa.String(20), nullable=False, server_default="priority"),
        sa.Column("total_chapters", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("satisfied_chapters", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("attention_chapters", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("status IN ('active', 'complete', 'cancelled')", name="ck_series_download_plan_status"),
        sa.CheckConstraint("phase IN ('priority', 'backfill', 'complete', 'cancelled')", name="ck_series_download_plan_phase"),
        sa.ForeignKeyConstraint(["series_id"], ["series_v2.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("series_id"),
    )
    op.create_index("ix_series_download_plan_status", "series_download_plan", ["status"])
    op.create_index("ix_series_download_plan_phase", "series_download_plan", ["phase"])
    op.create_index("ix_series_download_plan_status_phase", "series_download_plan", ["status", "phase"])
    op.create_table(
        "chapter_download_intent",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("series_id", sa.Integer(), nullable=False),
        sa.Column("chapter_id", sa.Integer(), nullable=False),
        sa.Column("tier", sa.String(20), nullable=False, server_default="backfill"),
        sa.Column("state", sa.String(20), nullable=False, server_default="blocked"),
        sa.Column("job_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("tier IN ('current', 'priority', 'backfill')", name="ck_chapter_download_intent_tier"),
        sa.CheckConstraint("state IN ('blocked', 'pending', 'queued', 'satisfied', 'attention', 'cancelled')", name="ck_chapter_download_intent_state"),
        sa.ForeignKeyConstraint(["series_id"], ["series_v2.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["chapter_id"], ["chapter_v2.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["job_id"], ["job.id"], ondelete="SET NULL"),
        sa.UniqueConstraint("series_id", "chapter_id", name="uq_chapter_download_intent_chapter"),
    )
    op.create_index("ix_chapter_download_intent_series_id", "chapter_download_intent", ["series_id"])
    op.create_index("ix_chapter_download_intent_chapter_id", "chapter_download_intent", ["chapter_id"])
    op.create_index("ix_chapter_download_intent_job_id", "chapter_download_intent", ["job_id"])
    op.create_index("ix_chapter_download_intent_tier", "chapter_download_intent", ["tier"])
    op.create_index("ix_chapter_download_intent_state", "chapter_download_intent", ["state"])
    op.create_index("ix_chapter_download_intent_plan_state", "chapter_download_intent", ["series_id", "tier", "state"])


def downgrade() -> None:
    op.drop_table("chapter_download_intent")
    op.drop_table("series_download_plan")
    for column in (
        "progress_updated_at", "progress_message", "progress_bytes", "progress_unit",
        "progress_total", "progress_current", "progress_phase",
    ):
        op.drop_column("job", column)
