"""register MangaDex as a catalog provider

Revision ID: 0023_mangadex_provider
Revises: 0022_chapter_release_quality
"""

from typing import Sequence, Union

from alembic import op


revision: str = "0023_mangadex_provider"
down_revision: Union[str, None] = "0022_chapter_release_quality"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

KNOWN = "'asura', 'mangadex', 'mangafire', 'kingofshojo'"
PREVIOUS = "'asura', 'mangafire', 'kingofshojo'"


def upgrade() -> None:
    for table, name in (
        ("source_state_v2", "ck_source_state_known_provider"),
        ("provider_policy", "ck_provider_policy_known_provider"),
        ("provider_endpoint_state", "ck_provider_endpoint_known_provider"),
        ("provider_request_sample", "ck_provider_sample_known_provider"),
    ):
        with op.batch_alter_table(table) as batch:
            batch.drop_constraint(name, type_="check")
            batch.create_check_constraint(name, f"source IN ({KNOWN})")


def downgrade() -> None:
    op.execute("DELETE FROM provider_request_sample WHERE source='mangadex'")
    op.execute("DELETE FROM provider_endpoint_state WHERE source='mangadex'")
    op.execute("DELETE FROM provider_policy WHERE source='mangadex'")
    op.execute("DELETE FROM source_state_v2 WHERE source='mangadex'")
    for table, name in (
        ("source_state_v2", "ck_source_state_known_provider"),
        ("provider_policy", "ck_provider_policy_known_provider"),
        ("provider_endpoint_state", "ck_provider_endpoint_known_provider"),
        ("provider_request_sample", "ck_provider_sample_known_provider"),
    ):
        with op.batch_alter_table(table) as batch:
            batch.drop_constraint(name, type_="check")
            batch.create_check_constraint(name, f"source IN ({PREVIOUS})")
