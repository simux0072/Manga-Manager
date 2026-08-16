"""repair latest-release aggregates and index telemetry retention

Revision ID: 0019_latest_release_integrity
Revises: 0018_workflow_progress_identity
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0019_latest_release_integrity"
down_revision: Union[str, None] = "0018_workflow_progress_identity"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("source_series_v2") as batch:
        batch.add_column(
            sa.Column("observation_version", sa.String(100), nullable=False, server_default="")
        )
        batch.add_column(sa.Column("observation_seen_at", sa.DateTime(timezone=True)))
    with op.batch_alter_table("cover_signature_v2") as batch:
        for index in range(4):
            batch.add_column(
                sa.Column(f"hash_band_{index}", sa.String(16), nullable=False, server_default="")
            )
    for index in range(4):
        op.create_index(
            f"ix_cover_signature_v2_hash_band_{index}",
            "cover_signature_v2",
            [f"hash_band_{index}"],
        )
    op.create_index(
        "ix_provider_request_sample_created_at",
        "provider_request_sample",
        ["created_at"],
    )
    # Numeric chapters always win; publication time is only a fallback for nonnumeric rows.
    # Correlated subqueries keep this migration portable to the SQLite round-trip suite.
    order = (
        "(c.sort_number IS NULL), c.sort_number DESC, "
        "CASE WHEN c.sort_number IS NULL THEN r.published_at END DESC NULLS LAST, r.id DESC"
    )
    op.execute(
        f"""
        UPDATE series_v2 SET
          latest_release_number = COALESCE((
            SELECT c.display_number FROM chapter_v2 c
            JOIN chapter_release_v2 r ON r.chapter_id=c.id
            WHERE c.series_id=series_v2.id ORDER BY {order} LIMIT 1
          ), ''),
          latest_release_source = COALESCE((
            SELECT r.source FROM chapter_v2 c
            JOIN chapter_release_v2 r ON r.chapter_id=c.id
            WHERE c.series_id=series_v2.id ORDER BY {order} LIMIT 1
          ), ''),
          latest_release_at = (
            SELECT COALESCE(r.published_at, r.first_seen_at) FROM chapter_v2 c
            JOIN chapter_release_v2 r ON r.chapter_id=c.id
            WHERE c.series_id=series_v2.id ORDER BY {order} LIMIT 1
          ),
          integrity_state = CASE WHEN EXISTS (
            SELECT 1 FROM chapter_v2 c JOIN chapter_release_v2 r ON r.chapter_id=c.id
            WHERE c.series_id=series_v2.id
          ) THEN 'healthy' ELSE integrity_state END
        """
    )
    op.execute(
        """
        UPDATE source_series_v2 SET observation_version = COALESCE((
          SELECT c.display_number FROM chapter_release_v2 r
          JOIN chapter_v2 c ON c.id=r.chapter_id
          WHERE r.source_series_id=source_series_v2.id
          ORDER BY (c.sort_number IS NULL), c.sort_number DESC,
                   CASE WHEN c.sort_number IS NULL THEN r.published_at END DESC NULLS LAST,
                   r.id DESC LIMIT 1
        ), ''), observation_seen_at=detail_fetched_at
        """
    )
    op.execute(
        """
        UPDATE match_decision_v2 SET decision='accepted', decided_by='canonicalized',
          decided_at=CURRENT_TIMESTAMP
        WHERE decision='pending' AND EXISTS (
          SELECT 1 FROM source_series_v2 l, source_series_v2 r
          WHERE l.id=match_decision_v2.left_source_series_id
            AND r.id=match_decision_v2.right_source_series_id
            AND l.series_id=r.series_id
        )
        """
    )


def downgrade() -> None:
    op.drop_index(
        "ix_provider_request_sample_created_at",
        table_name="provider_request_sample",
    )
    with op.batch_alter_table("source_series_v2") as batch:
        batch.drop_column("observation_seen_at")
        batch.drop_column("observation_version")
    for index in range(4):
        op.drop_index(
            f"ix_cover_signature_v2_hash_band_{index}",
            table_name="cover_signature_v2",
        )
    with op.batch_alter_table("cover_signature_v2") as batch:
        for index in range(4):
            batch.drop_column(f"hash_band_{index}")
