from dataclasses import replace
from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.domain.book_reservation import BookReservation
from src.infrastructure.db.models import BookReservationModel
from src.infrastructure.db.unit_of_work import SqlAlchemyUnitOfWork


def _reservation() -> BookReservation:
    return BookReservation(uuid4(), 'Мастер и Маргарита', 'Михаил Булгаков', datetime.now(UTC))


async def _stored(session_factory: async_sessionmaker[AsyncSession]) -> list[BookReservation]:
    async with session_factory() as session:
        models = (await session.execute(select(BookReservationModel))).scalars()
        return [
            BookReservation(model.id, model.title, model.author, model.reserved_at)
            for model in models
        ]


async def test_added_reservation_is_stored(
    uow: SqlAlchemyUnitOfWork, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    reservation = _reservation()
    async with uow:
        assert await uow.book_reservations.add(reservation)
        await uow.commit()

    assert await _stored(session_factory) == [reservation]


async def test_add_returns_false_for_same_id_and_keeps_first(
    uow: SqlAlchemyUnitOfWork, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    first = _reservation()
    async with uow:
        assert await uow.book_reservations.add(first)
        await uow.commit()

    async with uow:
        assert not await uow.book_reservations.add(replace(first, title='Белая гвардия'))
        await uow.commit()

    assert await _stored(session_factory) == [first]
