from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Final
from uuid import UUID, uuid7

CARD_NUMBER_PREFIX: Final = 'LIB-'
CARD_NUMBER_DIGITS: Final = 8
_CARD_NUMBER_PATTERN: Final = re.compile(rf'{CARD_NUMBER_PREFIX}[0-9]{{{CARD_NUMBER_DIGITS}}}')


@dataclass(frozen=True, slots=True)
class CardNumber:
    value: str

    def __post_init__(self) -> None:
        if not _CARD_NUMBER_PATTERN.fullmatch(self.value):
            raise ValueError(f'Invalid card number: {self.value!r}')

    @classmethod
    def from_sequence(cls, n: int) -> CardNumber:
        return cls(f'{CARD_NUMBER_PREFIX}{n:0{CARD_NUMBER_DIGITS}d}')


@dataclass(slots=True)
class Membership:
    id: UUID
    user_id: UUID
    number: CardNumber
    issued_at: datetime

    @classmethod
    def issue(cls, user_id: UUID, number: CardNumber, now: datetime) -> Membership:
        return cls(id=uuid7(), user_id=user_id, number=number, issued_at=now)
