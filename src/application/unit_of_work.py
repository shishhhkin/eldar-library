from types import TracebackType
from typing import Protocol, Self

from src.domain.repositories import BookReservationRepository, MembershipRepository


class UnitOfWork(Protocol):
    memberships: MembershipRepository
    book_reservations: BookReservationRepository

    async def __aenter__(self) -> Self: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...

    async def commit(self) -> None: ...
