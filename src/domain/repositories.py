from typing import Protocol
from uuid import UUID

from src.domain.membership import CardNumber, Membership


class MembershipRepository(Protocol):
    async def get(self, membership_id: UUID) -> Membership | None: ...

    async def add(self, membership: Membership) -> bool: ...

    async def next_card_number(self) -> CardNumber: ...
