from datetime import UTC, datetime
from uuid import uuid4

from src.domain.membership import CardNumber, Membership
from src.infrastructure.db.unit_of_work import SqlAlchemyUnitOfWork


def _membership(number: int = 1) -> Membership:
    return Membership.issue(uuid4(), CardNumber.from_sequence(number), datetime.now(UTC))


async def test_saved_membership_reads_back_equal(uow: SqlAlchemyUnitOfWork) -> None:
    membership = _membership()
    async with uow:
        assert await uow.memberships.add(membership)
        await uow.commit()

    async with uow:
        assert await uow.memberships.get(membership.id) == membership


async def test_get_missing_returns_none(uow: SqlAlchemyUnitOfWork) -> None:
    async with uow:
        assert await uow.memberships.get(uuid4()) is None


async def test_add_returns_false_for_same_user(uow: SqlAlchemyUnitOfWork) -> None:
    first = _membership(1)
    second = Membership.issue(first.user_id, CardNumber.from_sequence(2), datetime.now(UTC))
    async with uow:
        assert await uow.memberships.add(first)
        await uow.commit()

    async with uow:
        assert not await uow.memberships.add(second)
        await uow.commit()

    async with uow:
        assert await uow.memberships.get(first.id) == first
        assert await uow.memberships.get(second.id) is None


async def test_next_card_number_increments(uow: SqlAlchemyUnitOfWork) -> None:
    async with uow:
        first = await uow.memberships.next_card_number()
        second = await uow.memberships.next_card_number()

    assert first == CardNumber('LIB-00000001')
    assert second == CardNumber('LIB-00000002')


async def test_exit_without_commit_rolls_back(uow: SqlAlchemyUnitOfWork) -> None:
    membership = _membership()
    async with uow:
        assert await uow.memberships.add(membership)

    async with uow:
        assert await uow.memberships.get(membership.id) is None
