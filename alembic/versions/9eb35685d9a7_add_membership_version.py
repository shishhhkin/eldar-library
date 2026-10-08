"""add membership version

Revision ID: 9eb35685d9a7
Revises: 1178fea485dd
Create Date: 2026-10-03 19:41:35.851167

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '9eb35685d9a7'
down_revision: Union[str, Sequence[str], None] = '1178fea485dd'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        'memberships',
        sa.Column('version', sa.Integer(), server_default=sa.text('1'), nullable=False),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('memberships', 'version')
