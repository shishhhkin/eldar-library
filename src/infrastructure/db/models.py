from datetime import datetime
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column

from src.infrastructure.db.base import Base

membership_number_seq = sa.Sequence('membership_number_seq', metadata=Base.metadata)


class MembershipModel(Base):
    __tablename__ = 'memberships'

    id: Mapped[UUID] = mapped_column(sa.Uuid, primary_key=True)
    user_id: Mapped[UUID] = mapped_column(sa.Uuid, nullable=False, unique=True)
    number: Mapped[str] = mapped_column(sa.String(12), nullable=False, unique=True)
    issued_at: Mapped[datetime] = mapped_column(sa.DateTime(timezone=True), nullable=False)
