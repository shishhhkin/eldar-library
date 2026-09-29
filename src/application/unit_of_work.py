from types import TracebackType
from typing import Protocol, Self

from src.domain.repositories import MembershipRepository


class UnitOfWork(Protocol):
    memberships: MembershipRepository

    async def __aenter__(self) -> Self: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...

    async def commit(self) -> None: ...
