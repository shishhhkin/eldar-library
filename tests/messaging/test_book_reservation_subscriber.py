import asyncio
import json
import logging
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from src.application.use_cases import RecordBookReservation
from src.domain.book_reservation import BookReservation
from src.infrastructure.db.models import BookReservationModel
from src.infrastructure.db.unit_of_work import SqlAlchemyUnitOfWork
from src.messaging.book_reservation_subscriber import BookReservationSubscriber
from src.messaging.dlq import dlq_topic
from tests.conftest import read_messages, started_subscriber

SUBSCRIBER_LOGGER = 'src.messaging.book_reservation_subscriber'
WAIT_TIMEOUT_SECONDS = 15.0
RESERVED_AT = datetime(2026, 10, 23, 10, 0, tzinfo=UTC)


def _message(
    reservation_id: UUID | None = None, title: str = 'Мастер и Маргарита'
) -> dict[str, Any]:
    return {
        'id': str(reservation_id or uuid4()),
        'title': title,
        'author': 'Михаил Булгаков',
        'reserved_at': RESERVED_AT.isoformat(),
    }


def _encoded(message: dict[str, Any]) -> bytes:
    return json.dumps(message, ensure_ascii=False).encode()


async def _reservations(
    session_factory: async_sessionmaker[AsyncSession],
) -> dict[UUID, tuple[str, str, datetime]]:
    async with session_factory() as session:
        stmt = select(
            BookReservationModel.id,
            BookReservationModel.title,
            BookReservationModel.author,
            BookReservationModel.reserved_at,
        )
        return {
            reservation_id: (title, author, reserved_at)
            for reservation_id, title, author, reserved_at in await session.execute(stmt)
        }


async def _committed(subscriber: BookReservationSubscriber, topic: str) -> int | None:
    return await subscriber.consumer.committed(TopicPartition(topic, 0))


async def _poll_until_committed(
    subscriber: BookReservationSubscriber, topic: str, offset: int
) -> None:
    async with asyncio.timeout(WAIT_TIMEOUT_SECONDS):
        while await _committed(subscriber, topic) != offset:
            await subscriber.poll_once()


async def _poll_until_deferred(
    subscriber: BookReservationSubscriber, caplog: pytest.LogCaptureFixture, times: int
) -> None:
    async with asyncio.timeout(WAIT_TIMEOUT_SECONDS):
        while sum('deferred' in text for text in caplog.messages) < times:
            await subscriber.poll_once()


async def _topic_size(bootstrap: str, topic: str) -> int:
    consumer = AIOKafkaConsumer(bootstrap_servers=bootstrap)
    await consumer.start()
    try:
        partition = TopicPartition(topic, 0)
        return int((await consumer.end_offsets([partition]))[partition])
    finally:
        await consumer.stop()


async def test_message_is_recorded_and_offset_committed(
    session_factory: async_sessionmaker[AsyncSession],
    kafka_bootstrap: str,
    kafka_topic: str,
    kafka_group_id: str,
    kafka_producer: AIOKafkaProducer,
) -> None:
    message = _message()
    await kafka_producer.send_and_wait(kafka_topic, _encoded(message), key=message['id'].encode())

    async with started_subscriber(
        session_factory, kafka_bootstrap, kafka_topic, kafka_group_id
    ) as subscriber:
        await _poll_until_committed(subscriber, kafka_topic, 1)

    assert await _reservations(session_factory) == {
        UUID(message['id']): ('Мастер и Маргарита', 'Михаил Булгаков', RESERVED_AT)
    }


async def test_duplicate_message_is_recorded_once(
    session_factory: async_sessionmaker[AsyncSession],
    kafka_bootstrap: str,
    kafka_topic: str,
    kafka_group_id: str,
    kafka_producer: AIOKafkaProducer,
    caplog: pytest.LogCaptureFixture,
) -> None:
    message = _message()
    for _ in range(2):
        await kafka_producer.send_and_wait(kafka_topic, _encoded(message))
    caplog.set_level(logging.INFO, logger='src.application.use_cases')

    async with started_subscriber(
        session_factory, kafka_bootstrap, kafka_topic, kafka_group_id
    ) as subscriber:
        await _poll_until_committed(subscriber, kafka_topic, 2)

    assert list(await _reservations(session_factory)) == [UUID(message['id'])]
    assert f'book reservation already recorded: {message["id"]}' in caplog.messages


async def test_redelivered_reservation_is_committed_without_overwriting(
    session_factory: async_sessionmaker[AsyncSession],
    kafka_bootstrap: str,
    kafka_topic: str,
    kafka_group_id: str,
    kafka_producer: AIOKafkaProducer,
) -> None:
    reservation_id = uuid4()
    recorded = BookReservation(reservation_id, 'Белая гвардия', 'Михаил Булгаков', RESERVED_AT)
    assert await RecordBookReservation(SqlAlchemyUnitOfWork(session_factory))(recorded)
    await kafka_producer.send_and_wait(kafka_topic, _encoded(_message(reservation_id)))

    async with started_subscriber(
        session_factory, kafka_bootstrap, kafka_topic, kafka_group_id
    ) as subscriber:
        await _poll_until_committed(subscriber, kafka_topic, 1)

    assert await _reservations(session_factory) == {
        reservation_id: ('Белая гвардия', 'Михаил Булгаков', RESERVED_AT)
    }


@pytest.mark.parametrize(
    'value',
    [
        b'not json',
        _encoded({**_message(), 'title': ''}),
        _encoded({**_message(), 'title': 'М' * 256}),
        _encoded({**_message(), 'id': 'not-a-uuid'}),
        _encoded({key: value for key, value in _message().items() if key != 'author'}),
        _encoded({key: value for key, value in _message().items() if key != 'reserved_at'}),
        None,
    ],
    ids=[
        'not_json',
        'empty_title',
        'long_title',
        'bad_id',
        'no_author',
        'no_reserved_at',
        'tombstone',
    ],
)
async def test_poison_message_goes_to_dlq_and_next_message_is_recorded(
    session_factory: async_sessionmaker[AsyncSession],
    kafka_bootstrap: str,
    kafka_topic: str,
    kafka_group_id: str,
    kafka_producer: AIOKafkaProducer,
    value: bytes | None,
) -> None:
    valid = _message()
    await kafka_producer.send_and_wait(kafka_topic, value, key=b'poison')
    await kafka_producer.send_and_wait(kafka_topic, _encoded(valid))

    async with started_subscriber(
        session_factory, kafka_bootstrap, kafka_topic, kafka_group_id
    ) as subscriber:
        await _poll_until_committed(subscriber, kafka_topic, 2)

    assert list(await _reservations(session_factory)) == [UUID(valid['id'])]
    [dead_letter] = await read_messages(kafka_bootstrap, dlq_topic(kafka_topic), 1)
    assert (dead_letter.key, dead_letter.value) == (b'poison', value)
    headers = dict(dead_letter.headers)
    assert headers['error.class'] == b'ValidationError'
    assert headers['error.message']
    assert headers['original.topic'] == kafka_topic.encode()
    assert (headers['original.partition'], headers['original.offset']) == (b'0', b'0')
    assert await _topic_size(kafka_bootstrap, dlq_topic(kafka_topic)) == 1


async def test_message_rejected_by_database_goes_to_dlq_and_next_message_is_recorded(
    session_factory: async_sessionmaker[AsyncSession],
    kafka_bootstrap: str,
    kafka_topic: str,
    kafka_group_id: str,
    kafka_producer: AIOKafkaProducer,
) -> None:
    rejected, valid = _encoded(_message(title='Мастер\x00и Маргарита')), _message()
    await kafka_producer.send_and_wait(kafka_topic, rejected)
    await kafka_producer.send_and_wait(kafka_topic, _encoded(valid))

    async with started_subscriber(
        session_factory, kafka_bootstrap, kafka_topic, kafka_group_id
    ) as subscriber:
        await _poll_until_committed(subscriber, kafka_topic, 2)

    assert list(await _reservations(session_factory)) == [UUID(valid['id'])]
    [dead_letter] = await read_messages(kafka_bootstrap, dlq_topic(kafka_topic), 1)
    assert dead_letter.value == rejected
    assert dict(dead_letter.headers)['error.class'] == b'DBAPIError'


async def test_database_failure_leaves_offset_uncommitted_until_message_is_recorded(
    session_factory: async_sessionmaker[AsyncSession],
    unreachable_session_factory: async_sessionmaker[AsyncSession],
    kafka_bootstrap: str,
    kafka_topic: str,
    kafka_group_id: str,
    kafka_producer: AIOKafkaProducer,
    caplog: pytest.LogCaptureFixture,
) -> None:
    message = _message()
    await kafka_producer.send_and_wait(kafka_topic, _encoded(message))

    with caplog.at_level(logging.WARNING, logger=SUBSCRIBER_LOGGER):
        async with started_subscriber(
            unreachable_session_factory, kafka_bootstrap, kafka_topic, kafka_group_id
        ) as subscriber:
            await _poll_until_deferred(subscriber, caplog, 2)
            assert await _committed(subscriber, kafka_topic) is None

    assert await _topic_size(kafka_bootstrap, dlq_topic(kafka_topic)) == 0
    async with started_subscriber(
        session_factory, kafka_bootstrap, kafka_topic, kafka_group_id
    ) as subscriber:
        await _poll_until_committed(subscriber, kafka_topic, 1)
    assert list(await _reservations(session_factory)) == [UUID(message['id'])]


async def test_sql_error_goes_to_dlq(
    missing_schema_session_factory: async_sessionmaker[AsyncSession],
    kafka_bootstrap: str,
    kafka_topic: str,
    kafka_group_id: str,
    kafka_producer: AIOKafkaProducer,
) -> None:
    message = _encoded(_message())
    await kafka_producer.send_and_wait(kafka_topic, message)

    async with started_subscriber(
        missing_schema_session_factory, kafka_bootstrap, kafka_topic, kafka_group_id
    ) as subscriber:
        await _poll_until_committed(subscriber, kafka_topic, 1)

    [dead_letter] = await read_messages(kafka_bootstrap, dlq_topic(kafka_topic), 1)
    assert dead_letter.value == message
    assert dict(dead_letter.headers)['error.class'] == b'ProgrammingError'


async def test_lock_timeout_leaves_offset_uncommitted_until_lock_is_released(
    engine: AsyncEngine,
    session_factory: async_sessionmaker[AsyncSession],
    lock_timeout_session_factory: async_sessionmaker[AsyncSession],
    kafka_bootstrap: str,
    kafka_topic: str,
    kafka_group_id: str,
    kafka_producer: AIOKafkaProducer,
    caplog: pytest.LogCaptureFixture,
) -> None:
    message = _message()
    await kafka_producer.send_and_wait(kafka_topic, _encoded(message))

    with caplog.at_level(logging.WARNING, logger=SUBSCRIBER_LOGGER):
        async with started_subscriber(
            lock_timeout_session_factory, kafka_bootstrap, kafka_topic, kafka_group_id
        ) as subscriber:
            async with engine.connect() as locker, locker.begin():
                await locker.execute(
                    text(
                        f'LOCK TABLE {BookReservationModel.__tablename__} IN ACCESS EXCLUSIVE MODE'
                    )
                )
                await _poll_until_deferred(subscriber, caplog, 2)
                assert await _committed(subscriber, kafka_topic) is None
            await _poll_until_committed(subscriber, kafka_topic, 1)

    assert await _topic_size(kafka_bootstrap, dlq_topic(kafka_topic)) == 0
    assert list(await _reservations(session_factory)) == [UUID(message['id'])]


async def test_run_records_messages_until_stopped(
    session_factory: async_sessionmaker[AsyncSession],
    kafka_bootstrap: str,
    kafka_topic: str,
    kafka_group_id: str,
    kafka_producer: AIOKafkaProducer,
) -> None:
    message = _message()

    async with started_subscriber(
        session_factory, kafka_bootstrap, kafka_topic, kafka_group_id
    ) as subscriber:
        worker = asyncio.create_task(subscriber.run())
        try:
            await kafka_producer.send_and_wait(kafka_topic, _encoded(message))
            async with asyncio.timeout(WAIT_TIMEOUT_SECONDS):
                while not await _reservations(session_factory):
                    await asyncio.sleep(0.05)
        finally:
            subscriber.stop()
            async with asyncio.timeout(WAIT_TIMEOUT_SECONDS):
                await worker

    assert list(await _reservations(session_factory)) == [UUID(message['id'])]
