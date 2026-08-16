"""job routing and provider permits

Revision ID: 0006_job_routing_permits
Revises: 0005_kavita_mapping
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "0006_job_routing_permits"
down_revision: Union[str, None] = "0005_kavita_mapping"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("job", sa.Column("source", sa.String(50), nullable=False, server_default=""))
    op.add_column("job", sa.Column("series_key", sa.String(100), nullable=False, server_default=""))
    op.add_column(
        "job", sa.Column("pool", sa.String(50), nullable=False, server_default="maintenance")
    )
    op.create_index("ix_job_source", "job", ["source"])
    op.create_index("ix_job_series_key", "job", ["series_key"])
    op.create_index("ix_job_pool", "job", ["pool"])
    op.create_index("ix_job_pool_claim", "job", ["pool", "status", "available_at", "priority"])
    op.create_index("ix_job_source_status", "job", ["source", "status"])
    op.create_index(
        "uq_job_leased_chapter_series",
        "job",
        ["series_key"],
        unique=True,
        postgresql_where=sa.text(
            "kind = 'chapter_download' AND status = 'leased' AND series_key <> ''"
        ),
    )
    op.create_table(
        "job_permit",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "job_id", sa.Integer(), sa.ForeignKey("job.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column("pool", sa.String(50), nullable=False),
        sa.Column("owner", sa.String(200), nullable=False),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("job_id", "pool", name="uq_job_permit_job_pool"),
    )
    op.create_index("ix_job_permit_job_id", "job_permit", ["job_id"])
    op.create_index("ix_job_permit_pool_expiry", "job_permit", ["pool", "lease_expires_at"])


def downgrade() -> None:
    op.drop_table("job_permit")
    op.drop_index("uq_job_leased_chapter_series", table_name="job")
    op.drop_index("ix_job_source_status", table_name="job")
    op.drop_index("ix_job_pool_claim", table_name="job")
    op.drop_index("ix_job_pool", table_name="job")
    op.drop_index("ix_job_series_key", table_name="job")
    op.drop_index("ix_job_source", table_name="job")
    op.drop_column("job", "pool")
    op.drop_column("job", "series_key")
    op.drop_column("job", "source")
