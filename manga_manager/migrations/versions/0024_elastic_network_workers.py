"""route catalog refreshes into the elastic network worker pool

Revision ID: 0024_elastic_network_workers
Revises: 0023_mangadex_provider
"""

from typing import Sequence, Union

from alembic import op


revision: str = "0024_elastic_network_workers"
down_revision: Union[str, None] = "0023_mangadex_provider"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Do not move a live lease out from under an old worker during a rolling
    # deployment. Queued/retry work moves atomically; an expired old lease is
    # normalized by the recovery path after the new code takes over.
    op.execute(
        """
        UPDATE job
        SET pool = 'refresh:' || source
        WHERE kind = 'source_refresh'
          AND status IN ('queued', 'retry_wait')
          AND source <> ''
          AND pool <> 'refresh:' || source
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE job
        SET pool = 'pull:' || source
        WHERE kind = 'source_refresh'
          AND status IN ('queued', 'retry_wait')
          AND source <> ''
          AND pool <> 'pull:' || source
        """
    )
