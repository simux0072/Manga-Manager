"""index logical job attempts

Revision ID: 0021_job_logical_attempt_index
Revises: 0020_kavita_cover_reconciliation
"""

from typing import Sequence, Union

from alembic import op


revision: str = "0021_job_logical_attempt_index"
down_revision: Union[str, None] = "0020_kavita_cover_reconciliation"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Failed-job views ask whether a later attempt with the same logical key
    # exists.  Without this index that anti-join becomes quadratic at scale.
    op.create_index(
        "ix_job_logical_attempt",
        "job",
        ["kind", "dedupe_key", "id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_job_logical_attempt", table_name="job")
