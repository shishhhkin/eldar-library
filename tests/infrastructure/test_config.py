from collections.abc import Callable

import pytest
from pydantic import ValidationError

from src.infrastructure.config import Settings
from src.messaging.dlq import dlq_topic

type LoadSettings = Callable[..., Settings]

KAFKA_TOPIC_LIMIT = 249

BASE_ENV = {
    'postgres_user': 'postgres',
    'postgres_password': 'postgres',
    'postgres_host': 'localhost',
    'postgres_port': '5432',
    'postgres_db': 'library',
    'kafka_bootstrap_servers': 'localhost:9092',
    'kafka_book_reservations_topic': 'book-reservations',
    'kafka_group_id': 'eldar-library',
    'kafka_poll_timeout_seconds': '1',
    'kafka_send_timeout_seconds': '3',
    'book_reservation_subscriber_retry_delay_seconds': '5',
}

SECONDS_UP_TO_MINUTE = (
    'kafka_poll_timeout_seconds',
    'kafka_send_timeout_seconds',
    'book_reservation_subscriber_retry_delay_seconds',
)
INVALID_BOOTSTRAP_SERVERS = (
    '',
    'localhost',
    'localhost:',
    ':9092',
    'localhost:x',
    'localhost:0',
    'localhost:65536',
    'localhost:9092,',
    'localhost:9092, other:9092',
)
INVALID_TOPICS = ('', 'book reservations', 'бронь', 'book/reservations', 'a' * 246)

INVALID_VALUES = [
    *[('kafka_bootstrap_servers', value) for value in INVALID_BOOTSTRAP_SERVERS],
    *[('kafka_book_reservations_topic', value) for value in INVALID_TOPICS],
    ('kafka_group_id', ''),
    *[
        (name, value)
        for name in SECONDS_UP_TO_MINUTE
        for value in ('0', '-1', '60.5', 'inf', 'nan', 'x')
    ],
]

LOWER_BOUNDS: dict[str, object] = {
    'kafka_bootstrap_servers': 'h:1',
    'kafka_book_reservations_topic': 'a',
    'kafka_group_id': 'g',
    'kafka_poll_timeout_seconds': 0.001,
    'kafka_send_timeout_seconds': 0.001,
    'book_reservation_subscriber_retry_delay_seconds': 0.001,
}

UPPER_BOUNDS: dict[str, object] = {
    'kafka_bootstrap_servers': 'kafka-1.internal:65535,kafka-2.internal:9092',
    'kafka_book_reservations_topic': 'a' * 245,
    'kafka_poll_timeout_seconds': 60,
    'kafka_send_timeout_seconds': 60,
    'book_reservation_subscriber_retry_delay_seconds': 60,
}


@pytest.fixture
def load_settings(monkeypatch: pytest.MonkeyPatch) -> LoadSettings:
    def load(**overrides: str) -> Settings:
        for name, value in {**BASE_ENV, **overrides}.items():
            monkeypatch.setenv(name, value)
        return Settings(_env_file=None)  # type: ignore[call-arg]

    return load


def test_base_env_covers_every_setting() -> None:
    assert set(BASE_ENV) == set(Settings.model_fields)


@pytest.mark.parametrize(('name', 'value'), INVALID_VALUES)
def test_invalid_value_is_rejected(load_settings: LoadSettings, name: str, value: str) -> None:
    with pytest.raises(ValidationError) as exc_info:
        load_settings(**{name: value})

    assert {error['loc'] for error in exc_info.value.errors()} == {(name,)}


@pytest.mark.parametrize('bounds', [LOWER_BOUNDS, UPPER_BOUNDS], ids=['lower', 'upper'])
def test_boundary_values_are_accepted(
    load_settings: LoadSettings, bounds: dict[str, object]
) -> None:
    settings = load_settings(**{name: str(value) for name, value in bounds.items()})

    assert settings.model_dump(include=set(bounds)) == bounds


def test_dlq_of_longest_allowed_topic_fits_kafka_limit() -> None:
    assert len(dlq_topic('a' * 245)) == KAFKA_TOPIC_LIMIT
