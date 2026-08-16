"""classify known legacy job failures

Revision ID: 0011_classify_legacy_failures
Revises: 0010_web_reliability_search
"""

from typing import Sequence, Union

from alembic import op

revision: str = "0011_classify_legacy_failures"
down_revision: Union[str, None] = "0010_web_reliability_search"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        "UPDATE job SET error_code = 'legacy_counter_null' "
        "WHERE error_message LIKE '%unsupported operand type(s) for +=:%NoneType%'"
    )


def downgrade() -> None:
    op.execute("UPDATE job SET error_code = '' WHERE error_code = 'legacy_counter_null'")
