import asyncio
import logging
from collections.abc import Callable
from contextlib import suppress

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.errors import CommitFailedError
from aiokafka.structs import ConsumerRecord
from asyncpg.exceptions import (
    AdminShutdownError,
    CannotConnectNowError,
    CrashShutdownError,
    DeadlockDetectedError,
    LockNotAvailableError,
    PostgresConnectionError,
    SerializationError,
    TooManyConnectionsError,
)
from sqlalchemy.exc import DBAPIError

from src.application.use_cases import RecordBookReservation
from src.messaging.dlq import DLQPublisher
from src.messaging.mappers import to_book_reservation
from src.messaging.schemas import BookReservationEvent

logger = logging.getLogger(__name__)

RETRYABLE_POSTGRES_ERRORS = (
    PostgresConnectionError,
    SerializationError,
    DeadlockDetectedError,
    TooManyConnectionsError,
    LockNotAvailableError,
    AdminShutdownError,
    CrashShutdownError,
    CannotConnectNowError,
)


def is_retryable(exc: Exception) -> bool:
    if isinstance(exc, OSError):
        return True
    if not isinstance(exc, DBAPIError):
        return False
    postgres_error = exc.orig.__cause__ if exc.orig is not None else None
    return exc.connection_invalidated or isinstance(postgres_error, RETRYABLE_POSTGRES_ERRORS)


class BookReservationSubscriber:
    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        dlq: DLQPublisher,
        record_reservation: RecordBookReservation,
        poll_timeout_seconds: float,
        retry_delay_seconds: float,
        heartbeat: Callable[[], None] = lambda: None,
    ) -> None:
        self.consumer = consumer
        self.dlq = dlq
        self.record_reservation = record_reservation
        self.poll_timeout_ms = int(poll_timeout_seconds * 1000)
        self.retry_delay_seconds = retry_delay_seconds
        self.heartbeat = heartbeat
        self.stopping = asyncio.Event()

    def stop(self) -> None:
        self.stopping.set()

    async def run(self) -> None:
        while not self.stopping.is_set():
            try:
                await self.poll_once()
            except Exception:
                logger.exception('book reservation subscriber failed')
                await self._wait_before_retry()

    async def poll_once(self) -> None:
        batches = await self.consumer.getmany(timeout_ms=self.poll_timeout_ms)
        self.heartbeat()
        deferred = False
        for partition, records in batches.items():
            for record in records:
                if self.stopping.is_set():
                    return
                if not await self._process(partition, record):
                    self.consumer.seek(partition, record.offset)
                    deferred = True
                    break
                self.heartbeat()
        if deferred:
            await self._wait_before_retry()

    async def _process(self, partition: TopicPartition, record: ConsumerRecord) -> bool:
        try:
            event = BookReservationEvent.model_validate_json(record.value)
            await self.record_reservation(to_book_reservation(event))
        except Exception as exc:
            if is_retryable(exc):
                logger.warning(
                    'book reservation deferred: %s offset=%s (%r)', partition, record.offset, exc
                )
                return False
            logger.exception('book reservation rejected: %s offset=%s', partition, record.offset)
            if not await self.dlq.publish(record, exc):
                return False
        await self._commit(partition, record.offset + 1)
        return True

    async def _commit(self, partition: TopicPartition, offset: int) -> None:
        try:
            await self.consumer.commit({partition: offset})
        except CommitFailedError as exc:
            logger.warning('offset not committed: %s offset=%s (%s)', partition, offset, exc)

    async def _wait_before_retry(self) -> None:
        with suppress(TimeoutError):
            await asyncio.wait_for(self.stopping.wait(), self.retry_delay_seconds)
