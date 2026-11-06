import asyncio
import socket
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.structs import ConsumerRecord
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from testcontainers.community.kafka import KafkaContainer
from testcontainers.community.postgres import PostgresContainer

from src.app import get_app
from src.application.use_cases import RecordBookReservation
from src.infrastructure.db import models  # noqa: F401
from src.infrastructure.db.base import Base
from src.infrastructure.db.unit_of_work import SqlAlchemyUnitOfWork
from src.messaging.book_reservation_subscriber import BookReservationSubscriber
from src.messaging.dlq import DLQPublisher, dlq_topic
from src.messaging.kafka import build_consumer, build_dlq_producer

KAFKA_IMAGE = 'confluentinc/cp-kafka:7.9.10'
KAFKA_POLL_TIMEOUT_SECONDS = 0.1
RETRY_DELAY_SECONDS = 0.05
KAFKA_SEND_TIMEOUT_SECONDS = 5.0
KAFKA_READ_TIMEOUT_SECONDS = 10.0
KAFKA_POLL_MS = 100


def closed_port() -> int:
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return int(sock.getsockname()[1])


@pytest.fixture(scope='session')
def postgres_container() -> Iterator[PostgresContainer]:
    with PostgresContainer('postgres:17-alpine', driver='asyncpg') as container:
        yield container


@pytest.fixture(scope='session')
async def engine(postgres_container: PostgresContainer) -> AsyncIterator[AsyncEngine]:
    engine = create_async_engine(postgres_container.get_connection_url(), pool_pre_ping=True)
    yield engine
    await engine.dispose()


@pytest.fixture
async def session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    return async_sessionmaker(bind=engine, expire_on_commit=False)


@pytest.fixture
def uow(session_factory: async_sessionmaker[AsyncSession]) -> SqlAlchemyUnitOfWork:
    return SqlAlchemyUnitOfWork(session_factory)


@pytest.fixture
async def client(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncClient]:
    app = get_app()
    app.state.session_factory = session_factory
    async with AsyncClient(transport=ASGITransport(app=app), base_url='http://test') as ac:
        yield ac


@pytest.fixture
async def unreachable_client() -> AsyncIterator[AsyncClient]:
    engine = create_async_engine(
        f'postgresql+asyncpg://postgres:postgres@127.0.0.1:{closed_port()}/postgres'
    )
    app = get_app()
    app.state.session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        yield ac
    await engine.dispose()


@pytest.fixture
async def unreachable_session_factory() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_async_engine(
        f'postgresql+asyncpg://postgres:postgres@127.0.0.1:{closed_port()}/postgres'
    )
    yield async_sessionmaker(bind=engine, expire_on_commit=False)
    await engine.dispose()


@pytest.fixture(scope='session')
def kafka_container() -> Iterator[KafkaContainer]:
    with KafkaContainer(KAFKA_IMAGE).with_kraft() as container:
        yield container


@pytest.fixture(scope='session')
def kafka_bootstrap(kafka_container: KafkaContainer) -> str:
    return kafka_container.get_bootstrap_server()


@pytest.fixture
async def kafka_topic(kafka_bootstrap: str) -> str:
    topic = f'book-reservations-{uuid4().hex}'
    admin = AIOKafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    await admin.start()
    try:
        await admin.create_topics(
            [
                NewTopic(name, num_partitions=1, replication_factor=1)
                for name in (topic, dlq_topic(topic))
            ]
        )
    finally:
        await admin.close()
    return topic


@pytest.fixture
def kafka_group_id() -> str:
    return f'eldar-library-{uuid4().hex}'


@pytest.fixture
async def kafka_producer(kafka_bootstrap: str) -> AsyncIterator[AIOKafkaProducer]:
    producer = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap)
    await producer.start()
    yield producer
    await producer.stop()


@asynccontextmanager
async def started_subscriber(
    session_factory: async_sessionmaker[AsyncSession], bootstrap: str, topic: str, group_id: str
) -> AsyncIterator[BookReservationSubscriber]:
    consumer = build_consumer(bootstrap, topic, group_id)
    dlq_producer = build_dlq_producer(bootstrap)
    await dlq_producer.start()
    await consumer.start()
    try:
        yield BookReservationSubscriber(
            consumer,
            DLQPublisher(dlq_producer, KAFKA_SEND_TIMEOUT_SECONDS),
            RecordBookReservation(SqlAlchemyUnitOfWork(session_factory)),
            KAFKA_POLL_TIMEOUT_SECONDS,
            RETRY_DELAY_SECONDS,
        )
    finally:
        await consumer.stop()
        await dlq_producer.stop()


async def read_messages(bootstrap: str, topic: str, count: int) -> list[ConsumerRecord]:
    consumer = AIOKafkaConsumer(topic, bootstrap_servers=bootstrap, auto_offset_reset='earliest')
    await consumer.start()
    try:
        records: list[ConsumerRecord] = []
        async with asyncio.timeout(KAFKA_READ_TIMEOUT_SECONDS):
            while len(records) < count:
                for batch in (await consumer.getmany(timeout_ms=KAFKA_POLL_MS)).values():
                    records.extend(batch)
        return records
    finally:
        await consumer.stop()
