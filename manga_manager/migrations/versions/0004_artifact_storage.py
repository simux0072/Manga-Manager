"""artifact storage and library projection

Revision ID: 0004_artifact_storage
Revises: 0003_catalog
Create Date: 2026-07-10 15:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "0004_artifact_storage"
down_revision: Union[str, None] = "0003_catalog"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "artifact_blob",
        sa.Column("checksum", sa.String(length=64), nullable=False),
        sa.Column("relative_path", sa.Text(), nullable=False),
        sa.Column("byte_count", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("checksum"),
        sa.UniqueConstraint("relative_path"),
    )
    op.create_table(
        "chapter_artifact",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("chapter_id", sa.Integer(), nullable=False),
        sa.Column("chapter_release_id", sa.Integer(), nullable=True),
        sa.Column("blob_checksum", sa.String(length=64), nullable=False),
        sa.Column("state", sa.String(length=20), nullable=False),
        sa.Column("provenance", sa.String(length=50), nullable=False),
        sa.Column("source", sa.String(length=50), nullable=False),
        sa.Column("image_count", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("deactivated_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "state IN ('active', 'inactive', 'quarantined')",
            name="ck_chapter_artifact_state",
        ),
        sa.ForeignKeyConstraint(["blob_checksum"], ["artifact_blob.checksum"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["chapter_id"], ["chapter_v2.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["chapter_release_id"], ["chapter_release_v2.id"], ondelete="SET NULL"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_chapter_artifact_chapter_id", "chapter_artifact", ["chapter_id"])
    op.create_index(
        "ix_chapter_artifact_chapter_release_id", "chapter_artifact", ["chapter_release_id"]
    )
    op.create_index("ix_chapter_artifact_blob_checksum", "chapter_artifact", ["blob_checksum"])
    op.create_index("ix_chapter_artifact_state", "chapter_artifact", ["state"])
    op.create_index(
        "ix_chapter_artifact_state_created", "chapter_artifact", ["state", "created_at"]
    )
    active = sa.text("state = 'active'")
    op.create_index(
        "uq_chapter_artifact_active",
        "chapter_artifact",
        ["chapter_id"],
        unique=True,
        sqlite_where=active,
        postgresql_where=active,
    )
    op.create_table(
        "library_projection",
        sa.Column("chapter_id", sa.Integer(), nullable=False),
        sa.Column("artifact_id", sa.Integer(), nullable=False),
        sa.Column("relative_path", sa.Text(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["artifact_id"], ["chapter_artifact.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["chapter_id"], ["chapter_v2.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("chapter_id"),
        sa.UniqueConstraint("artifact_id", name="uq_library_projection_artifact"),
        sa.UniqueConstraint("relative_path", name="uq_library_projection_path"),
    )
    op.create_index("ix_library_projection_artifact_id", "library_projection", ["artifact_id"])


def downgrade() -> None:
    op.drop_index("ix_library_projection_artifact_id", table_name="library_projection")
    op.drop_table("library_projection")
    op.drop_index("uq_chapter_artifact_active", table_name="chapter_artifact")
    op.drop_index("ix_chapter_artifact_state_created", table_name="chapter_artifact")
    op.drop_index("ix_chapter_artifact_state", table_name="chapter_artifact")
    op.drop_index("ix_chapter_artifact_blob_checksum", table_name="chapter_artifact")
    op.drop_index("ix_chapter_artifact_chapter_release_id", table_name="chapter_artifact")
    op.drop_index("ix_chapter_artifact_chapter_id", table_name="chapter_artifact")
    op.drop_table("chapter_artifact")
    op.drop_table("artifact_blob")
