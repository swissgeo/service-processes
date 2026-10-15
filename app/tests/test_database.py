from unittest.mock import AsyncMock, MagicMock

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from fastapi import FastAPI
from fastapi.testclient import TestClient

import pytest

from app.core.database import (
    DatabaseSessionDep,
    check_database_connection,
    close_database,
    create_database_engine,
    initialize_database,
)
from app.settings import Settings


@pytest.mark.asyncio
async def test_create_database_engine_uses_database_url_override() -> None:
    database_url = "postgresql+asyncpg://readonly:secret@db.example.test:5432/processes"
    engine = create_database_engine(Settings(_env_file=None, database_url=database_url))

    try:
        assert engine.url.render_as_string(hide_password=False) == database_url
        assert engine.pool._pre_ping is True  # noqa: SLF001
    finally:
        await engine.dispose()


def test_create_database_engine_sets_connect_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    create_async_engine = MagicMock()
    monkeypatch.setattr("app.core.database.create_async_engine", create_async_engine)

    create_database_engine(Settings(_env_file=None, db_connect_timeout=2.5))

    assert create_async_engine.call_args.kwargs["connect_args"] == {"timeout": 2.5}


@pytest.mark.asyncio
async def test_database_lifecycle_initializes_and_clears_application_state() -> None:
    app = FastAPI()
    settings = Settings(_env_file=None)

    await initialize_database(app, settings)
    try:
        assert app.state.database_engine.url.render_as_string(hide_password=False) == (
            "postgresql+asyncpg://service_processes:service_processes@localhost:15432/service_processes"
        )
        assert app.state.database_session_maker.kw["expire_on_commit"] is False
    finally:
        await close_database(app)

    assert app.state.database_engine is None
    assert app.state.database_session_maker is None


@pytest.mark.asyncio
async def test_database_session_dependency_provides_async_session_without_connecting() -> None:
    app = FastAPI()
    await initialize_database(app, Settings(_env_file=None))

    @app.get("/session")
    async def get_session(session: DatabaseSessionDep) -> dict[str, bool]:
        return {"is_async_session": isinstance(session, AsyncSession)}

    try:
        with TestClient(app) as client:
            response = client.get("/session")
    finally:
        await close_database(app)

    assert response.status_code == 200
    assert response.json() == {"is_async_session": True}


@pytest.mark.asyncio
async def test_check_database_connection_executes_ping_query() -> None:
    connection = AsyncMock()
    connection_context = AsyncMock()
    connection_context.__aenter__.return_value = connection
    engine = MagicMock(spec=AsyncEngine)
    engine.connect.return_value = connection_context

    await check_database_connection(engine)

    statement = connection.execute.await_args.args[0]
    assert str(statement) == "SELECT 1"
    connection_context.__aexit__.assert_awaited_once()
