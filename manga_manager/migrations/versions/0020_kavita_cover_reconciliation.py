"""reconcile Kavita covers after chapter remapping

Revision ID: 0020_kavita_cover_reconciliation
Revises: 0019_latest_release_integrity
"""

from typing import Sequence, Union

from alembic import op


revision: str = "0020_kavita_cover_reconciliation"
down_revision: Union[str, None] = "0019_latest_release_integrity"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Older synchronization state remembered the source checksum but not whether a Kavita scan
    # had replaced the remote chapter entity. Force one bounded refresh; subsequent syncs compare
    # both checksum and Kavita chapter ID.
    op.execute("UPDATE chapter_v2 SET kavita_cover_checksum = ''")
    op.execute(
        """
        UPDATE series_v2
        SET kavita_cover_checksum = '', kavita_synced_at = NULL
        WHERE kavita_series_id IS NOT NULL
        """
    )


def downgrade() -> None:
    # Remote cover state cannot be reconstructed safely. A following sync repopulates it.
    pass
