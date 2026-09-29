from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import APIRouter, FastAPI
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.api.errors import register_exception_handlers
from src.api.middleware import LoggingMiddleware, RequestIDMiddleware
from src.api.routers.healthcheck import router as healthcheck_router
from src.api.routers.memberships import router as memberships_router
from src.infrastructure.config import Settings
from src.infrastructure.logging_config import setup_logging


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    settings = Settings()  # type: ignore[call-arg]
    engine = create_async_engine(str(settings.postgres_url), pool_pre_ping=True)
    app.state.session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)

    yield

    await engine.dispose()


def get_app() -> FastAPI:
    setup_logging()

    app = FastAPI(
        title='Library',
        docs_url='/docs',
        openapi_url='/openapi.json',
        lifespan=lifespan,
    )

    app.add_middleware(LoggingMiddleware)
    app.add_middleware(RequestIDMiddleware)

    register_exception_handlers(app)

    app.include_router(healthcheck_router)

    v1_router = APIRouter(prefix='/v1')
    v1_router.include_router(memberships_router)
    app.include_router(v1_router)

    return app
