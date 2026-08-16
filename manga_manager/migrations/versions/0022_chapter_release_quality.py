"""store selected chapter release quality

Revision ID: 0022_chapter_release_quality
Revises: 0021_job_logical_attempt_index
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "0022_chapter_release_quality"
down_revision: Union[str, None] = "0021_job_logical_attempt_index"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "chapter_release_v2",
        sa.Column("quality_rank", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "chapter_artifact",
        sa.Column("quality_rank", sa.Integer(), nullable=False, server_default="0"),
    )


def downgrade() -> None:
    op.drop_column("chapter_artifact", "quality_rank")
    op.drop_column("chapter_release_v2", "quality_rank")
