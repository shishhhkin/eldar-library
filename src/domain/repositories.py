from typing import Protocol
from uuid import UUID

from src.domain.book_reservation import BookReservation
from src.domain.membership import CardNumber, Membership


class MembershipRepository(Protocol):
    async def get(self, membership_id: UUID) -> Membership | None: ...

    async def get_by_user_id(self, user_id: UUID) -> Membership | None: ...

    async def add(self, membership: Membership) -> bool: ...

    async def next_card_number(self) -> CardNumber: ...


class BookReservationRepository(Protocol):
    async def add(self, reservation: BookReservation) -> bool: ...
