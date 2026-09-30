import logging
from datetime import UTC, datetime
from uuid import UUID

from src.application.unit_of_work import UnitOfWork
from src.domain.exceptions import MembershipAlreadyExistsError, MembershipNotFoundError
from src.domain.membership import Membership

logger = logging.getLogger(__name__)


class IssueMembership:
    def __init__(self, uow: UnitOfWork) -> None:
        self.uow = uow

    async def __call__(self, user_id: UUID) -> Membership:
        async with self.uow:
            number = await self.uow.memberships.next_card_number()
            membership = Membership.issue(user_id, number, datetime.now(UTC))
            if not await self.uow.memberships.add(membership):
                logger.info('membership already exists: user_id=%s', user_id)
                raise MembershipAlreadyExistsError(user_id)
            await self.uow.commit()
        return membership


class GetMembership:
    def __init__(self, uow: UnitOfWork) -> None:
        self.uow = uow

    async def __call__(self, membership_id: UUID) -> Membership:
        async with self.uow:
            membership = await self.uow.memberships.get(membership_id)
        if membership is None:
            logger.info('membership not found: %s', membership_id)
            raise MembershipNotFoundError(membership_id)
        return membership


class FindMembershipByUser:
    def __init__(self, uow: UnitOfWork) -> None:
        self.uow = uow

    async def __call__(self, user_id: UUID) -> Membership | None:
        async with self.uow:
            return await self.uow.memberships.get_by_user_id(user_id)
