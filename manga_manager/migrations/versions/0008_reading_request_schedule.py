"""chapter reading state and provider request scheduling

Revision ID: 0008_reading_request_schedule
Revises: 0007_catalog_integrity_search
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0008_reading_request_schedule"
down_revision: Union[str, None] = "0007_catalog_integrity_search"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "source_state_v2", sa.Column("next_request_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.create_table(
        "chapter_reading_state_v2",
        sa.Column(
            "chapter_id",
            sa.Integer(),
            sa.ForeignKey("chapter_v2.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("status", sa.String(20), nullable=False, server_default="unread"),
        sa.Column("read_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "status IN ('unread', 'reading', 'read')",
            name="ck_chapter_reading_state_v2_status",
        ),
    )
    op.create_index(
        "ix_chapter_reading_state_v2_status", "chapter_reading_state_v2", ["status"]
    )
    op.create_index(
        "ix_chapter_reading_state_v2_status_updated",
        "chapter_reading_state_v2",
        ["status", "updated_at"],
    )


def downgrade() -> None:
    op.drop_table("chapter_reading_state_v2")
    op.drop_column("source_state_v2", "next_request_at")
