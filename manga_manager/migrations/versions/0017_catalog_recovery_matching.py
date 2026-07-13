"""catalog recovery, visual matching, and Kavita projections

Revision ID: 0017_catalog_recovery_matching
Revises: 0016_refresh_storage_hardening
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "0017_catalog_recovery_matching"
down_revision: Union[str, None] = "0016_refresh_storage_hardening"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("job") as batch:
        batch.drop_constraint("ck_job_kind", type_="check")
        batch.create_check_constraint(
            "ck_job_kind",
            "kind IN ('source_pull', 'source_refresh', 'chapter_download', 'kavita_sync', "
            "'library_repair', 'maintenance', 'notification')",
        )
    op.drop_index("uq_job_leased_chapter_series", table_name="job")
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
    op.add_column(
        "match_decision_v2",
        sa.Column("scorer_version", sa.String(50), nullable=False, server_default="legacy"),
    )
    op.add_column(
        "match_decision_v2",
        sa.Column(
            "feature_vector_json",
            sa.JSON().with_variant(postgresql.JSONB(none_as_null=True), "postgresql"),
            nullable=False,
            server_default=sa.text("'{}'"),
        ),
    )
    op.create_table(
        "cover_asset_v2",
        sa.Column("source_series_id", sa.Integer(), primary_key=True),
        sa.Column("content_checksum", sa.String(64), nullable=False),
        sa.Column("relative_path", sa.Text(), nullable=False),
        sa.Column("content_type", sa.String(100), nullable=False, server_default=""),
        sa.Column("source_url", sa.Text(), nullable=False, server_default=""),
        sa.Column("width", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("height", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["source_series_id"], ["source_series_v2.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_cover_asset_v2_content_checksum", "cover_asset_v2", ["content_checksum"])
    op.create_table(
        "cover_signature_v2",
        sa.Column("source_series_id", sa.Integer(), primary_key=True),
        sa.Column("algorithm_version", sa.String(50), nullable=False),
        sa.Column(
            "feature_json",
            sa.JSON().with_variant(postgresql.JSONB(none_as_null=True), "postgresql"),
            nullable=False,
            server_default=sa.text("'{}'"),
        ),
        sa.Column("keypoints_blob", sa.LargeBinary(), nullable=False),
        sa.Column("descriptors_blob", sa.LargeBinary(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["source_series_id"], ["source_series_v2.id"], ondelete="CASCADE"),
    )
    op.create_index(
        "ix_cover_signature_algorithm", "cover_signature_v2", ["algorithm_version"]
    )
    op.create_table(
        "match_training_label",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("original_decision_id", sa.Integer(), nullable=True),
        sa.Column("label", sa.Integer(), nullable=False),
        sa.Column("origin", sa.String(50), nullable=False, server_default="review"),
        sa.Column("scorer_version", sa.String(50), nullable=False, server_default="legacy"),
        sa.Column(
            "feature_vector_json",
            sa.JSON().with_variant(postgresql.JSONB(none_as_null=True), "postgresql"),
            nullable=False,
            server_default=sa.text("'{}'"),
        ),
        sa.Column(
            "evidence_json",
            sa.JSON().with_variant(postgresql.JSONB(none_as_null=True), "postgresql"),
            nullable=False,
            server_default=sa.text("'{}'"),
        ),
        sa.Column(
            "left_identity_json",
            sa.JSON().with_variant(postgresql.JSONB(none_as_null=True), "postgresql"),
            nullable=False,
            server_default=sa.text("'{}'"),
        ),
        sa.Column(
            "right_identity_json",
            sa.JSON().with_variant(postgresql.JSONB(none_as_null=True), "postgresql"),
            nullable=False,
            server_default=sa.text("'{}'"),
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("label IN (0, 1)", name="ck_match_training_label_value"),
    )
    op.create_index(
        "ix_match_training_label_original_decision_id",
        "match_training_label",
        ["original_decision_id"],
    )
    op.create_index(
        "ix_match_training_label_created", "match_training_label", ["created_at"]
    )
    op.create_table(
        "kavita_projection",
        sa.Column("chapter_id", sa.Integer(), primary_key=True),
        sa.Column("artifact_id", sa.Integer(), nullable=False),
        sa.Column("relative_path", sa.Text(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["chapter_id"], ["chapter_v2.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["artifact_id"], ["chapter_artifact.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("relative_path", name="uq_kavita_projection_path"),
    )
    op.create_index("ix_kavita_projection_artifact_id", "kavita_projection", ["artifact_id"])
    op.create_table(
        "artifact_metadata_rewrite",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("chapter_id", sa.Integer(), nullable=False),
        sa.Column("old_blob_checksum", sa.String(64), nullable=False),
        sa.Column("new_blob_checksum", sa.String(64), nullable=False),
        sa.Column("old_comic_info", sa.LargeBinary(), nullable=False),
        sa.Column("new_comic_info", sa.LargeBinary(), nullable=False),
        sa.Column("reason", sa.String(100), nullable=False, server_default="metadata"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["chapter_id"], ["chapter_v2.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("new_blob_checksum", name="uq_metadata_rewrite_new_blob"),
    )
    op.create_index(
        "ix_metadata_rewrite_chapter_created",
        "artifact_metadata_rewrite",
        ["chapter_id", "created_at"],
    )
    op.create_index(
        "ix_artifact_metadata_rewrite_old_blob_checksum",
        "artifact_metadata_rewrite",
        ["old_blob_checksum"],
    )
    op.create_index(
        "ix_artifact_metadata_rewrite_new_blob_checksum",
        "artifact_metadata_rewrite",
        ["new_blob_checksum"],
    )


def downgrade() -> None:
    op.drop_table("artifact_metadata_rewrite")
    op.drop_table("kavita_projection")
    op.drop_table("match_training_label")
    op.drop_table("cover_signature_v2")
    op.drop_table("cover_asset_v2")
    op.drop_column("match_decision_v2", "feature_vector_json")
    op.drop_column("match_decision_v2", "scorer_version")
    op.drop_index("uq_job_leased_chapter_series", table_name="job")
    op.create_index(
        "uq_job_leased_chapter_series",
        "job",
        ["series_key"],
        unique=True,
        sqlite_where=sa.text(
            "kind = 'chapter_download' AND status = 'leased' AND series_key <> ''"
        ),
        postgresql_where=sa.text(
            "kind = 'chapter_download' AND status = 'leased' AND series_key <> ''"
        ),
    )
    op.execute("DELETE FROM job WHERE kind='library_repair'")
    with op.batch_alter_table("job") as batch:
        batch.drop_constraint("ck_job_kind", type_="check")
        batch.create_check_constraint(
            "ck_job_kind",
            "kind IN ('source_pull', 'source_refresh', 'chapter_download', "
            "'kavita_sync', 'maintenance', 'notification')",
        )
