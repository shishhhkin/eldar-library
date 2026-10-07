import logging
from uuid import uuid4

import pytest

from src.application.use_cases import FindMembershipByUser, GetMembership, IssueMembership
from src.domain.exceptions import MembershipNotFoundError
from src.infrastructure.db.unit_of_work import SqlAlchemyUnitOfWork

LOGGER = 'src.application.use_cases'


async def test_issue_membership_persists(uow: SqlAlchemyUnitOfWork) -> None:
    user_id = uuid4()

    issued, created = await IssueMembership(uow)(user_id)

    assert created
    assert issued.user_id == user_id
    assert await GetMembership(uow)(issued.id) == issued


async def test_issue_membership_twice_returns_existing_and_logs(
    uow: SqlAlchemyUnitOfWork, caplog: pytest.LogCaptureFixture
) -> None:
    user_id = uuid4()
    first, _ = await IssueMembership(uow)(user_id)
    caplog.set_level(logging.INFO, logger=LOGGER)

    again, created = await IssueMembership(uow)(user_id)

    assert not created
    assert again == first
    assert f'membership already exists: user_id={user_id}' in caplog.messages


async def test_get_missing_membership_raises_and_logs(
    uow: SqlAlchemyUnitOfWork, caplog: pytest.LogCaptureFixture
) -> None:
    missing_id = uuid4()
    caplog.set_level(logging.INFO, logger=LOGGER)

    with pytest.raises(MembershipNotFoundError, match=str(missing_id)):
        await GetMembership(uow)(missing_id)

    assert f'membership not found: {missing_id}' in caplog.messages


async def test_find_membership_by_user(uow: SqlAlchemyUnitOfWork) -> None:
    issued, _ = await IssueMembership(uow)(uuid4())

    assert await FindMembershipByUser(uow)(issued.user_id) == issued
    assert await FindMembershipByUser(uow)(uuid4()) is None
