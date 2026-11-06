import asyncio
import json
import os
import signal
import sys
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaProducer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from testcontainers.community.postgres import PostgresContainer

from src.infrastructure.db.models import BookReservationModel
from src.subscriber import HEARTBEAT_NAME

ROOT = Path(__file__).resolve().parent.parent.parent
MODULE = 'src.subscriber'
STARTED = 'book reservation subscriber started'
STOPPED = 'book reservation subscriber stopped'
POLL_TIMEOUT_SECONDS = 0.1
RETRY_DELAY_SECONDS = 0.05
SEND_TIMEOUT_SECONDS = 5
PROCESS_TIMEOUT_SECONDS = 20.0
POLL_SECONDS = 0.05


@pytest.fixture
def subscriber_env(
    postgres_container: PostgresContainer,
    kafka_bootstrap: str,
    kafka_topic: str,
    kafka_group_id: str,
    tmp_path: Path,
) -> dict[str, str]:
    return {
        **os.environ,
        'TMPDIR': str(tmp_path),
        'postgres_user': postgres_container.username,
        'postgres_password': postgres_container.password,
        'postgres_host': postgres_container.get_container_host_ip(),
        'postgres_port': str(postgres_container.get_exposed_port(5432)),
        'postgres_db': postgres_container.dbname,
        'kafka_bootstrap_servers': kafka_bootstrap,
        'kafka_book_reservations_topic': kafka_topic,
        'kafka_group_id': kafka_group_id,
        'kafka_poll_timeout_seconds': str(POLL_TIMEOUT_SECONDS),
        'kafka_send_timeout_seconds': str(SEND_TIMEOUT_SECONDS),
        'book_reservation_subscriber_retry_delay_seconds': str(RETRY_DELAY_SECONDS),
    }


class SubscriberProcess:
    def __init__(self, process: asyncio.subprocess.Process) -> None:
        self.process = process
        self.log: list[str] = []
        self._reader = asyncio.create_task(self._read())

    async def _read(self) -> None:
        assert self.process.stderr is not None
        async for line in self.process.stderr:
            self.log.append(line.decode())

    def logged(self, text: str) -> bool:
        return any(text in line for line in self.log)

    async def wait_for(self, text: str) -> None:
        async with asyncio.timeout(PROCESS_TIMEOUT_SECONDS):
            while not self.logged(text):
                if self._reader.done():
                    raise AssertionError(f'subscriber exited before {text!r}:\n{"".join(self.log)}')
                await asyncio.sleep(POLL_SECONDS)

    async def stop(self) -> int:
        self.process.send_signal(signal.SIGTERM)
        async with asyncio.timeout(PROCESS_TIMEOUT_SECONDS):
            await self._reader
            return await self.process.wait()


@asynccontextmanager
async def running_subscriber(env: dict[str, str]) -> AsyncIterator[SubscriberProcess]:
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        '-m',
        MODULE,
        cwd=ROOT,
        env=env,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    subscriber = SubscriberProcess(process)
    try:
        await subscriber.wait_for(STARTED)
        yield subscriber
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()


async def _healthcheck(env: dict[str, str]) -> int:
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        '-m',
        MODULE,
        'healthcheck',
        cwd=ROOT,
        env=env,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    async with asyncio.timeout(PROCESS_TIMEOUT_SECONDS):
        return await process.wait()


async def test_subscriber_process_records_reservation_and_stops_on_sigterm(
    subscriber_env: dict[str, str],
    session_factory: async_sessionmaker[AsyncSession],
    kafka_producer: AIOKafkaProducer,
    kafka_topic: str,
) -> None:
    reservation_id = uuid4()
    message = {
        'id': str(reservation_id),
        'title': 'Мастер и Маргарита',
        'author': 'Михаил Булгаков',
        'reserved_at': '2026-10-23T10:00:00+00:00',
    }
    await kafka_producer.send_and_wait(kafka_topic, json.dumps(message).encode())

    async with running_subscriber(subscriber_env) as subscriber:
        async with asyncio.timeout(PROCESS_TIMEOUT_SECONDS):
            while True:
                async with session_factory() as session:
                    if await session.get(BookReservationModel, reservation_id) is not None:
                        break
                await asyncio.sleep(POLL_SECONDS)
        assert await _healthcheck(subscriber_env) == 0
        assert await subscriber.stop() == 0

    assert subscriber.logged(STOPPED)
    async with session_factory() as session:
        assert list((await session.execute(select(BookReservationModel.id))).scalars()) == [
            reservation_id
        ]


async def test_subscriber_healthcheck_fails_on_stale_heartbeat(
    subscriber_env: dict[str, str], tmp_path: Path
) -> None:
    heartbeat = tmp_path / HEARTBEAT_NAME
    heartbeat.touch()
    max_age = POLL_TIMEOUT_SECONDS + RETRY_DELAY_SECONDS + SEND_TIMEOUT_SECONDS
    beat_at = time.time() - max_age - 1
    os.utime(heartbeat, (beat_at, beat_at))

    assert await _healthcheck(subscriber_env) == 1
