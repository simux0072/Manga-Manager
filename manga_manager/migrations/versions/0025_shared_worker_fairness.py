"""allow fair parallel chapter work within a manga

Revision ID: 0025_shared_worker_fairness
Revises: 0024_elastic_network_workers
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "0025_shared_worker_fairness"
down_revision: Union[str, None] = "0024_elastic_network_workers"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_index("uq_job_leased_chapter_series", table_name="job")
    op.create_index(
        "uq_job_leased_library_repair_series",
        "job",
        ["series_key"],
        unique=True,
        sqlite_where=sa.text(
            "kind = 'library_repair' AND status = 'leased' AND series_key <> ''"
        ),
        postgresql_where=sa.text(
            "kind = 'library_repair' AND status = 'leased' AND series_key <> ''"
        ),
    )


def downgrade() -> None:
    op.drop_index("uq_job_leased_library_repair_series", table_name="job")
    op.create_index(
        "uq_job_leased_chapter_series",
        "job",
        ["series_key"],
        unique=True,
        sqlite_where=sa.text(
            "kind IN ('chapter_download', 'library_repair') "
            "AND status = 'leased' AND series_key <> ''"
        ),
        postgresql_where=sa.text(
            "kind IN ('chapter_download', 'library_repair') "
            "AND status = 'leased' AND series_key <> ''"
        ),
    )
