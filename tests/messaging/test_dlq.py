import logging

import pytest
from aiokafka import AIOKafkaProducer

from src.messaging.dlq import DLQPublisher, dlq_topic
from src.messaging.kafka import build_dlq_producer
from tests.conftest import read_messages

DLQ_LOGGER = 'src.messaging.dlq'
SEND_TIMEOUT_SECONDS = 5.0


async def test_publish_sends_record_with_error_headers(
    kafka_bootstrap: str, kafka_topic: str, kafka_producer: AIOKafkaProducer
) -> None:
    await kafka_producer.send_and_wait(
        kafka_topic, b'payload', key=b'key', headers=[('trace', b'abc')]
    )
    [record] = await read_messages(kafka_bootstrap, kafka_topic, 1)

    assert await DLQPublisher(kafka_producer, SEND_TIMEOUT_SECONDS).publish(
        record, ValueError('broken')
    )

    [dead_letter] = await read_messages(kafka_bootstrap, dlq_topic(kafka_topic), 1)
    assert (dead_letter.key, dead_letter.value) == (b'key', b'payload')
    assert list(dead_letter.headers) == [
        ('trace', b'abc'),
        ('error.class', b'ValueError'),
        ('error.message', b'broken'),
        ('original.topic', kafka_topic.encode()),
        ('original.partition', b'0'),
        ('original.offset', b'0'),
    ]


async def test_publish_returns_false_when_producer_cannot_send(
    kafka_bootstrap: str,
    kafka_topic: str,
    kafka_producer: AIOKafkaProducer,
    caplog: pytest.LogCaptureFixture,
) -> None:
    await kafka_producer.send_and_wait(kafka_topic, b'payload')
    [record] = await read_messages(kafka_bootstrap, kafka_topic, 1)
    closed = build_dlq_producer(kafka_bootstrap)
    await closed.start()
    await closed.stop()

    with caplog.at_level(logging.WARNING, logger=DLQ_LOGGER):
        published = await DLQPublisher(closed, SEND_TIMEOUT_SECONDS).publish(
            record, ValueError('broken')
        )

    assert not published
    assert any(message.startswith('dead letter deferred:') for message in caplog.messages)
