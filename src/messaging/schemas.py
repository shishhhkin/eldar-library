from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field


class BookReservationEvent(BaseModel):
    id: UUID
    title: str = Field(min_length=1, max_length=255)
    author: str = Field(min_length=1, max_length=128)
    reserved_at: datetime
