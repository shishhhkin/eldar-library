"""create memberships table

Revision ID: 1178fea485dd
Revises: 
Create Date: 2026-09-28 19:28:55.627872

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '1178fea485dd'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute(sa.schema.CreateSequence(sa.Sequence('membership_number_seq')))
    op.create_table(
        'memberships',
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('user_id', sa.Uuid(), nullable=False),
        sa.Column('number', sa.String(length=12), nullable=False),
        sa.Column('issued_at', sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('id', name=op.f('pk_memberships')),
        sa.UniqueConstraint('number', name=op.f('uq_memberships_number')),
        sa.UniqueConstraint('user_id', name=op.f('uq_memberships_user_id')),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table('memberships')
    op.execute(sa.schema.DropSequence(sa.Sequence('membership_number_seq')))
