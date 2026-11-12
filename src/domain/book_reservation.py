from dataclasses import dataclass
from datetime import datetime
from uuid import UUID


@dataclass(slots=True)
class BookReservation:
    id: UUID
    title: str
    author: str
    reserved_at: datetime
