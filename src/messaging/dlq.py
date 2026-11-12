import asyncio
import logging

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from aiokafka.structs import ConsumerRecord

logger = logging.getLogger(__name__)

DLQ_SUFFIX = '.dlq'


def dlq_topic(topic: str) -> str:
    return f'{topic}{DLQ_SUFFIX}'


class DLQPublisher:
    def __init__(self, producer: AIOKafkaProducer, send_timeout_seconds: float) -> None:
        self.producer = producer
        self.send_timeout_seconds = send_timeout_seconds

    async def publish(self, record: ConsumerRecord, error: Exception) -> bool:
        headers = [
            *record.headers,
            ('error.class', type(error).__name__.encode()),
            ('error.message', str(error).encode()),
            ('original.topic', record.topic.encode()),
            ('original.partition', str(record.partition).encode()),
            ('original.offset', str(record.offset).encode()),
        ]
        try:
            async with asyncio.timeout(self.send_timeout_seconds):
                await self.producer.send_and_wait(
                    dlq_topic(record.topic), value=record.value, key=record.key, headers=headers
                )
        except (KafkaError, TimeoutError) as exc:
            logger.warning(
                'dead letter deferred: %s-%s offset=%s (%r)',
                record.topic,
                record.partition,
                record.offset,
                exc,
            )
            return False
        return True
