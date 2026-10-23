import uuid
from unittest.mock import AsyncMock, MagicMock

from sqlalchemy import ClauseElement
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession

import pytest

from app.core.process_service import ProcessService, get_process_service
from app.models.process import Process


def _mock_session(processes: list[Process], total: int | None) -> AsyncMock:
    session = AsyncMock(spec=AsyncSession)
    session.scalars.return_value = MagicMock(all=MagicMock(return_value=processes))
    session.scalar.return_value = total
    return session


def _compile(statement: ClauseElement) -> str:
    return str(
        statement.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True})
    )


@pytest.mark.asyncio
async def test_list_processes_returns_processes_and_total():
    processes = [Process(name="A"), Process(name="B")]
    session = _mock_session(processes, total=12)

    result = await ProcessService(session).list_processes(limit=2, offset=0)

    assert result == (processes, 12)


@pytest.mark.asyncio
async def test_list_processes_orders_and_paginates_query():
    session = _mock_session([], total=0)

    await ProcessService(session).list_processes(limit=20, offset=40)

    statement = _compile(session.scalars.await_args.args[0])
    assert "ORDER BY process.name" in statement
    assert "LIMIT 20 OFFSET 40" in statement


@pytest.mark.asyncio
async def test_list_processes_counts_all_processes():
    session = _mock_session([], total=0)

    await ProcessService(session).list_processes(limit=20, offset=40)

    statement = _compile(session.scalar.await_args.args[0])
    assert statement == "SELECT count(*) AS count_1 \nFROM process"


@pytest.mark.asyncio
async def test_list_processes_total_defaults_to_zero():
    session = _mock_session([], total=None)

    _, total = await ProcessService(session).list_processes(limit=20, offset=0)

    assert total == 0


@pytest.mark.asyncio
async def test_get_process_queries_by_sgid():
    sgid = uuid.UUID("0195f3a2-7c1e-7b3a-9d4e-1f2a3b4c5d6e")
    session = AsyncMock(spec=AsyncSession)

    await ProcessService(session).get_process(sgid)

    statement = _compile(session.scalar.await_args.args[0])
    assert "WHERE process.sgid = '0195f3a2-7c1e-7b3a-9d4e-1f2a3b4c5d6e'" in statement


@pytest.mark.asyncio
async def test_get_process_returns_process():
    process = Process(name="A")
    session = AsyncMock(spec=AsyncSession)
    session.scalar.return_value = process

    result = await ProcessService(session).get_process(uuid.uuid7())

    assert result is process


@pytest.mark.asyncio
async def test_get_process_returns_none_when_not_found():
    session = AsyncMock(spec=AsyncSession)
    session.scalar.return_value = None

    result = await ProcessService(session).get_process(uuid.uuid7())

    assert result is None


def test_get_process_service_uses_session():
    session = AsyncMock(spec=AsyncSession)

    service = get_process_service(session)

    assert isinstance(service, ProcessService)
    assert service.session is session
