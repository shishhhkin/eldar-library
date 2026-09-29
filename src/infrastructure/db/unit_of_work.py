from types import TracebackType
from typing import Self

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.domain.repositories import MembershipRepository
from src.infrastructure.db.repositories import SqlAlchemyMembershipRepository


class SqlAlchemyUnitOfWork:
    memberships: MembershipRepository
    _session: AsyncSession

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def __aenter__(self) -> Self:
        self._session = self._session_factory()
        self.memberships = SqlAlchemyMembershipRepository(self._session)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self._session.close()

    async def commit(self) -> None:
        await self._session.commit()
