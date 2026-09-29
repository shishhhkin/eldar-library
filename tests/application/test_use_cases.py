import logging
from uuid import uuid4

import pytest

from src.application.use_cases import GetMembership, IssueMembership
from src.domain.exceptions import MembershipAlreadyExistsError, MembershipNotFoundError
from src.infrastructure.db.unit_of_work import SqlAlchemyUnitOfWork

LOGGER = 'src.application.use_cases'


async def test_issue_membership_persists(uow: SqlAlchemyUnitOfWork) -> None:
    user_id = uuid4()

    issued = await IssueMembership(uow)(user_id)

    assert issued.user_id == user_id
    assert await GetMembership(uow)(issued.id) == issued


async def test_issue_membership_twice_raises_and_logs(
    uow: SqlAlchemyUnitOfWork, caplog: pytest.LogCaptureFixture
) -> None:
    user_id = uuid4()
    first = await IssueMembership(uow)(user_id)
    caplog.set_level(logging.INFO, logger=LOGGER)

    with pytest.raises(MembershipAlreadyExistsError, match=str(user_id)):
        await IssueMembership(uow)(user_id)

    assert f'membership already exists: user_id={user_id}' in caplog.messages
    assert await GetMembership(uow)(first.id) == first


async def test_get_missing_membership_raises_and_logs(
    uow: SqlAlchemyUnitOfWork, caplog: pytest.LogCaptureFixture
) -> None:
    missing_id = uuid4()
    caplog.set_level(logging.INFO, logger=LOGGER)

    with pytest.raises(MembershipNotFoundError, match=str(missing_id)):
        await GetMembership(uow)(missing_id)

    assert f'membership not found: {missing_id}' in caplog.messages
