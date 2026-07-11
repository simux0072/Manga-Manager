"""enforce one provider identity per canonical series

Revision ID: 0009_canonical_provider_unique
Revises: 0008_reading_request_schedule
"""

from typing import Sequence, Union

from alembic import op

revision: str = "0009_canonical_provider_unique"
down_revision: Union[str, None] = "0008_reading_request_schedule"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("source_series_v2") as batch:
        batch.create_unique_constraint("uq_source_series_v2_series_source", ["series_id", "source"])


def downgrade() -> None:
    with op.batch_alter_table("source_series_v2") as batch:
        batch.drop_constraint("uq_source_series_v2_series_source", type_="unique")
