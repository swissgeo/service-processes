import logging
from collections.abc import AsyncGenerator
from typing import Annotated, cast

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from fastapi import Depends, FastAPI, Request

from app.settings import Settings

logger = logging.getLogger(__name__)


def create_database_engine(settings: Settings) -> AsyncEngine:
    """Create async PostgreSQL engine."""
    return create_async_engine(
        settings.async_database_url,
        pool_pre_ping=True,
    )


async def initialize_database(app: FastAPI, settings: Settings) -> None:
    """Initialize the engine and session maker for the application."""
    engine = create_database_engine(settings)
    app.state.database_engine = engine
    app.state.database_session_maker = async_sessionmaker(engine, expire_on_commit=False)
    logger.info("Initialized PostgreSQL connection pool")


async def close_database(app: FastAPI) -> None:
    """Dispose of connection pool."""
    engine = cast("AsyncEngine", app.state.database_engine)
    await engine.dispose()
    app.state.database_engine = None
    app.state.database_session_maker = None
    logger.info("Closed PostgreSQL connection pool")


async def check_database_connection(engine: AsyncEngine) -> None:
    """Verify that PostgreSQL accepts a simple query using the configured engine."""
    async with engine.connect() as connection:
        await connection.execute(text("SELECT 1"))


async def get_database_session(request: Request) -> AsyncGenerator[AsyncSession]:
    """Provide a request-scoped async database session.

    The dependency intentionally does not commit transactions. Read endpoints can use the session
    directly; write endpoints should explicitly use `async with session.begin():`.
    """
    async with request.app.state.database_session_maker() as session:
        yield session


DatabaseSessionDep = Annotated[AsyncSession, Depends(get_database_session)]
