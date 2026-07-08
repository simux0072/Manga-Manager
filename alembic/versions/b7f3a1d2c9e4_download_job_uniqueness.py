"""download job uniqueness

Revision ID: b7f3a1d2c9e4
Revises: 9d37ef72a2c1
Create Date: 2026-07-08 12:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "b7f3a1d2c9e4"
down_revision = "9d37ef72a2c1"
branch_labels = None
depends_on = None


INDEX_NAME = "uq_download_job_chapter_release"


def upgrade() -> None:
    connection = op.get_bind()
    inspector = sa.inspect(connection)
    table_names = set(inspector.get_table_names())
    if "download_job" not in table_names:
        return

    connection.execute(sa.text(deduplicate_download_jobs_sql()))
    indexes = {index["name"] for index in inspector.get_indexes("download_job") if index.get("name")}
    constraints = {
        constraint["name"]
        for constraint in inspector.get_unique_constraints("download_job")
        if constraint.get("name")
    }
    if INDEX_NAME not in indexes and INDEX_NAME not in constraints:
        op.create_index(INDEX_NAME, "download_job", ["chapter_release_id"], unique=True)


def downgrade() -> None:
    connection = op.get_bind()
    inspector = sa.inspect(connection)
    if "download_job" not in set(inspector.get_table_names()):
        return
    indexes = {index["name"] for index in inspector.get_indexes("download_job") if index.get("name")}
    if INDEX_NAME in indexes:
        op.drop_index(INDEX_NAME, table_name="download_job")


def deduplicate_download_jobs_sql() -> str:
    return """
DELETE FROM download_job
WHERE id IN (
    SELECT id
    FROM (
        SELECT
            id,
            ROW_NUMBER() OVER (
                PARTITION BY chapter_release_id
                ORDER BY
                    CASE status
                        WHEN 'complete' THEN 1
                        WHEN 'delayed' THEN 2
                        WHEN 'running' THEN 3
                        WHEN 'queued' THEN 4
                        WHEN 'failed' THEN 5
                        ELSE 6
                    END,
                    COALESCE(updated_at, created_at, '1970-01-01 00:00:00') DESC,
                    id ASC
            ) AS row_number
        FROM download_job
    ) ranked
    WHERE row_number > 1
)
"""
