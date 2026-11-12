import re
from pathlib import Path
from typing import Annotated

from pydantic import Field, PostgresDsn, computed_field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

ENV_FILE = Path(__file__).resolve().parent.parent.parent / '.env'

SecondsUpToMinute = Annotated[float, Field(gt=0, le=60)]
KafkaTopic = Annotated[str, Field(pattern=r'^[A-Za-z0-9._-]{1,245}$')]

_BOOTSTRAP_SERVER_RE = re.compile(r'[^\s,:]+:(\d{1,5})')


class Settings(BaseSettings):
    postgres_user: str
    postgres_password: str
    postgres_host: str
    postgres_port: int
    postgres_db: str

    kafka_bootstrap_servers: str
    kafka_book_reservations_topic: KafkaTopic
    kafka_group_id: str = Field(min_length=1)
    kafka_poll_timeout_seconds: SecondsUpToMinute
    kafka_send_timeout_seconds: SecondsUpToMinute

    book_reservation_subscriber_retry_delay_seconds: SecondsUpToMinute

    model_config = SettingsConfigDict(
        env_file=ENV_FILE,
        env_file_encoding='utf-8',
        case_sensitive=False,
        extra='ignore',
    )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def postgres_url(self) -> PostgresDsn:
        return PostgresDsn.build(  # type: ignore[return-value]
            scheme='postgresql+asyncpg',
            username=self.postgres_user,
            password=self.postgres_password,
            host=self.postgres_host,
            port=self.postgres_port,
            path=self.postgres_db,
        )

    @field_validator('kafka_bootstrap_servers')
    @classmethod
    def _check_bootstrap_servers(cls, value: str) -> str:
        for server in value.split(','):
            match = _BOOTSTRAP_SERVER_RE.fullmatch(server)
            if match is None or not 1 <= int(match.group(1)) <= 65535:
                raise ValueError(
                    'kafka_bootstrap_servers must be a comma-separated list of host:port'
                )
        return value
