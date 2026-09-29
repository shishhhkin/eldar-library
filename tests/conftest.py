import socket
from collections.abc import AsyncIterator, Iterator

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from testcontainers.community.postgres import PostgresContainer

from src.app import get_app
from src.infrastructure.db import models  # noqa: F401
from src.infrastructure.db.base import Base
from src.infrastructure.db.unit_of_work import SqlAlchemyUnitOfWork


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
