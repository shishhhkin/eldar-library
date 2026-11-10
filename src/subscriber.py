import asyncio
import logging
import signal
import sys
import tempfile
import time
from pathlib import Path

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.application.use_cases import RecordBookReservation
from src.infrastructure.config import Settings
from src.infrastructure.db.unit_of_work import SqlAlchemyUnitOfWork
from src.infrastructure.logging_config import setup_logging
from src.messaging.book_reservation_subscriber import BookReservationSubscriber
from src.messaging.dlq import DLQPublisher
from src.messaging.kafka import build_consumer, build_dlq_producer

logger = logging.getLogger(__name__)

HEARTBEAT_NAME = 'book-reservation-subscriber.heartbeat'


def heartbeat_path() -> Path:
    return Path(tempfile.gettempdir()) / HEARTBEAT_NAME


async def serve() -> None:
    settings = Settings()  # type: ignore[call-arg]
    engine = create_async_engine(str(settings.postgres_url), pool_pre_ping=True)
    session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)
    consumer = build_consumer(
        settings.kafka_bootstrap_servers,
        settings.kafka_book_reservations_topic,
        settings.kafka_group_id,
    )
    dlq_producer = build_dlq_producer(settings.kafka_bootstrap_servers)
    subscriber = BookReservationSubscriber(
        consumer,
        DLQPublisher(dlq_producer, settings.kafka_send_timeout_seconds),
        RecordBookReservation(SqlAlchemyUnitOfWork(session_factory)),
        settings.kafka_poll_timeout_seconds,
        settings.book_reservation_subscriber_retry_delay_seconds,
        heartbeat_path().touch,
    )
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, subscriber.stop)
    try:
        await dlq_producer.start()
        await consumer.start()
        logger.info('book reservation subscriber started')
        await subscriber.run()
    finally:
        try:
            async with asyncio.timeout(settings.kafka_send_timeout_seconds):
                await asyncio.gather(consumer.stop(), dlq_producer.stop())
        except TimeoutError:
            logger.warning('kafka clients stop timed out')
        await engine.dispose()
    logger.info('book reservation subscriber stopped')


def is_alive(settings: Settings) -> bool:
    max_age = (
        settings.kafka_poll_timeout_seconds
        + settings.book_reservation_subscriber_retry_delay_seconds
        + settings.kafka_send_timeout_seconds
    )
    try:
        age = time.time() - heartbeat_path().stat().st_mtime
    except FileNotFoundError:
        return False
    return age <= max_age


def main() -> None:
    if sys.argv[1:] == ['healthcheck']:
        sys.exit(0 if is_alive(Settings()) else 1)  # type: ignore[call-arg]
    setup_logging()
    asyncio.run(serve())


if __name__ == '__main__':
    main()
