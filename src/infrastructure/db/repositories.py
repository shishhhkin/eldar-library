from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.domain.membership import CardNumber, Membership
from src.infrastructure.db.models import MembershipModel, membership_number_seq


def to_domain(model: MembershipModel) -> Membership:
    return Membership(
        id=model.id,
        user_id=model.user_id,
        number=CardNumber(model.number),
        issued_at=model.issued_at,
    )


class SqlAlchemyMembershipRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get(self, membership_id: UUID) -> Membership | None:
        model = await self.session.get(MembershipModel, membership_id)
        return None if model is None else to_domain(model)

    async def get_by_user_id(self, user_id: UUID) -> Membership | None:
        stmt = select(MembershipModel).where(MembershipModel.user_id == user_id)
        model = (await self.session.execute(stmt)).scalar_one_or_none()
        return None if model is None else to_domain(model)

    async def add(self, membership: Membership) -> bool:
        stmt = (
            pg_insert(MembershipModel)
            .values(
                id=membership.id,
                user_id=membership.user_id,
                number=membership.number.value,
                issued_at=membership.issued_at,
            )
            .on_conflict_do_nothing(index_elements=[MembershipModel.user_id])
            .returning(MembershipModel.id)
        )
        return (await self.session.execute(stmt)).scalar_one_or_none() is not None

    async def next_card_number(self) -> CardNumber:
        value = (
            await self.session.execute(select(membership_number_seq.next_value()))
        ).scalar_one()
        return CardNumber.from_sequence(value)
