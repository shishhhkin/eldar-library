from src.domain.book_reservation import BookReservation
from src.messaging.schemas import BookReservationEvent


def to_book_reservation(event: BookReservationEvent) -> BookReservation:
    return BookReservation(
        id=event.id,
        title=event.title,
        author=event.author,
        reserved_at=event.reserved_at,
    )
