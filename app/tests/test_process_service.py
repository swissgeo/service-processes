import uuid
from unittest.mock import AsyncMock, MagicMock

from sqlalchemy import ClauseElement
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession

import pytest

from app.core.process_service import ProcessService, get_process_service
from app.models.process import Process

DATASET = uuid.UUID("0195f3a2-7c1e-7b3a-9d4e-000000000001")


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

    result = await ProcessService(session).list_processes(
        dataset_sgids=[DATASET], limit=2, offset=0
    )

    assert result == (processes, 12)


@pytest.mark.asyncio
async def test_list_processes_orders_and_paginates_query():
    session = _mock_session([], total=0)

    await ProcessService(session).list_processes(dataset_sgids=[DATASET], limit=20, offset=40)

    statement = _compile(session.scalars.await_args.args[0])
    assert "ORDER BY process.name" in statement
    assert "LIMIT 20 OFFSET 40" in statement


@pytest.mark.asyncio
async def test_list_processes_filters_by_datasets():
    other = uuid.UUID("0195f3a2-7c1e-7b3a-9d4e-000000000002")
    session = _mock_session([], total=0)

    await ProcessService(session).list_processes(dataset_sgids=[DATASET, other], limit=20, offset=0)

    statement = _compile(session.scalars.await_args.args[0])
    assert (
        "WHERE process.dataset_sgid IN "
        "('0195f3a2-7c1e-7b3a-9d4e-000000000001', '0195f3a2-7c1e-7b3a-9d4e-000000000002')"
    ) in statement


@pytest.mark.asyncio
async def test_list_processes_counts_processes_of_datasets():
    session = _mock_session([], total=0)

    await ProcessService(session).list_processes(dataset_sgids=[DATASET], limit=20, offset=40)

    statement = _compile(session.scalar.await_args.args[0])
    assert statement == (
        "SELECT count(*) AS count_1 \nFROM process \n"
        "WHERE process.dataset_sgid IN ('0195f3a2-7c1e-7b3a-9d4e-000000000001')"
    )


@pytest.mark.asyncio
async def test_list_processes_total_defaults_to_zero():
    session = _mock_session([], total=None)

    _, total = await ProcessService(session).list_processes(
        dataset_sgids=[DATASET], limit=20, offset=0
    )

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


@pytest.mark.asyncio
async def test_get_datasets_queries_distinct_hierarchy_of_datasets():
    session = AsyncMock(spec=AsyncSession)
    session.execute.return_value = MagicMock()

    await ProcessService(session).get_datasets([DATASET])

    statement = _compile(session.execute.await_args.args[0])
    assert statement == (
        "SELECT DISTINCT process.dataset_sgid, process.organization_unit_sgid, "
        "process.organization_sgid \nFROM process \n"
        "WHERE process.dataset_sgid IN ('0195f3a2-7c1e-7b3a-9d4e-000000000001')"
    )


@pytest.mark.asyncio
async def test_get_datasets_returns_tuples():
    rows = [(DATASET, uuid.uuid7(), uuid.uuid7())]
    session = AsyncMock(spec=AsyncSession)
    session.execute.return_value = MagicMock(
        tuples=MagicMock(return_value=MagicMock(all=MagicMock(return_value=rows)))
    )

    result = await ProcessService(session).get_datasets([DATASET])

    assert result == rows


def test_get_process_service_uses_session():
    session = AsyncMock(spec=AsyncSession)

    service = get_process_service(session)

    assert isinstance(service, ProcessService)
    assert service.session is session
