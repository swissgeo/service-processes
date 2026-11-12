import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import DEFAULT, AsyncMock, MagicMock

from sqlalchemy import ClauseElement
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession

import pytest

from app.core.airflow import AirflowClient
from app.core.process_service import InvalidInputsError, ProcessService, get_process_service
from app.models.process import JobStatus, Process

DATASET = "0195f3a2-7c1e-7b3a-9d4e-000000000001"


def _session() -> AsyncMock:
    """AsyncSession mock failing if a statement runs outside of an `async with session.begin()`.

    `session.on_commit` is called whenever a transaction block exits without error.
    """
    session = AsyncMock(spec=AsyncSession)
    session.add = MagicMock()  # synchronous on AsyncSession
    session.on_commit = MagicMock()
    in_transaction = False

    @asynccontextmanager
    async def begin() -> AsyncGenerator[None]:
        nonlocal in_transaction
        assert not in_transaction, "begin() called while a transaction is open"
        in_transaction = True
        try:
            yield
        finally:
            in_transaction = False
        session.on_commit()

    def require_transaction(*_args: Any, **_kwargs: Any) -> Any:
        assert in_transaction, "statement executed outside of session.begin()"
        return DEFAULT

    session.begin = MagicMock(side_effect=begin)
    for statement in (session.add, session.scalar, session.scalars, session.execute):
        statement.side_effect = require_transaction
    return session


def _mock_session(processes: list[Process], total: int | None) -> AsyncMock:
    session = _session()
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

    result = await ProcessService(session, MagicMock()).list_processes(
        dataset_sgids=[DATASET], limit=2, offset=0
    )

    assert result == (processes, 12)


@pytest.mark.asyncio
async def test_list_processes_orders_and_paginates_query():
    session = _mock_session([], total=0)

    await ProcessService(session, MagicMock()).list_processes(
        dataset_sgids=[DATASET], limit=20, offset=40
    )

    statement = _compile(session.scalars.await_args.args[0])
    assert "ORDER BY process.name" in statement
    assert "LIMIT 20 OFFSET 40" in statement


@pytest.mark.asyncio
async def test_list_processes_filters_by_datasets():
    other = "0195f3a2-7c1e-7b3a-9d4e-000000000002"
    session = _mock_session([], total=0)

    await ProcessService(session, MagicMock()).list_processes(
        dataset_sgids=[DATASET, other], limit=20, offset=0
    )

    statement = _compile(session.scalars.await_args.args[0])
    assert (
        "WHERE process.dataset_sgid IN "
        "('0195f3a2-7c1e-7b3a-9d4e-000000000001', '0195f3a2-7c1e-7b3a-9d4e-000000000002')"
    ) in statement


@pytest.mark.asyncio
async def test_list_processes_counts_processes_of_datasets():
    session = _mock_session([], total=0)

    await ProcessService(session, MagicMock()).list_processes(
        dataset_sgids=[DATASET], limit=20, offset=40
    )

    statement = _compile(session.scalar.await_args.args[0])
    assert statement == (
        "SELECT count(*) AS count_1 \nFROM process \n"
        "WHERE process.dataset_sgid IN ('0195f3a2-7c1e-7b3a-9d4e-000000000001')"
    )


@pytest.mark.asyncio
async def test_list_processes_total_defaults_to_zero():
    session = _mock_session([], total=None)

    _, total = await ProcessService(session, MagicMock()).list_processes(
        dataset_sgids=[DATASET], limit=20, offset=0
    )

    assert total == 0


@pytest.mark.asyncio
async def test_get_process_queries_by_sgid():
    sgid = "0195f3a2-7c1e-7b3a-9d4e-1f2a3b4c5d6e"
    session = _session()

    await ProcessService(session, MagicMock()).get_process(sgid)

    statement = _compile(session.scalar.await_args.args[0])
    assert "WHERE process.sgid = '0195f3a2-7c1e-7b3a-9d4e-1f2a3b4c5d6e'" in statement


@pytest.mark.asyncio
async def test_get_process_returns_process():
    process = Process(name="A")
    session = _session()
    session.scalar.return_value = process

    result = await ProcessService(session, MagicMock()).get_process(str(uuid.uuid7()))

    assert result is process


@pytest.mark.asyncio
async def test_get_process_returns_none_when_not_found():
    session = _session()
    session.scalar.return_value = None

    result = await ProcessService(session, MagicMock()).get_process(str(uuid.uuid7()))

    assert result is None


@pytest.mark.asyncio
async def test_get_datasets_queries_distinct_hierarchy_of_datasets():
    session = _session()
    session.execute.return_value = MagicMock()

    await ProcessService(session, MagicMock()).get_datasets([DATASET])

    statement = _compile(session.execute.await_args.args[0])
    assert statement == (
        "SELECT DISTINCT process.dataset_sgid, process.organization_unit_sgid, "
        "process.organization_sgid \nFROM process \n"
        "WHERE process.dataset_sgid IN ('0195f3a2-7c1e-7b3a-9d4e-000000000001')"
    )


@pytest.mark.asyncio
async def test_get_datasets_returns_tuples():
    rows = [(DATASET, str(uuid.uuid7()), str(uuid.uuid7()))]
    session = _session()
    session.execute.return_value = MagicMock(
        tuples=MagicMock(return_value=MagicMock(all=MagicMock(return_value=rows)))
    )

    result = await ProcessService(session, MagicMock()).get_datasets([DATASET])

    assert result == rows


def test_get_process_service_uses_session():
    session = _session()

    service = get_process_service(session, MagicMock())

    assert isinstance(service, ProcessService)
    assert service.session is session


def _process() -> Process:
    return Process(
        id=1,
        sgid="prc_1",
        dag_id="dummy_dag",
        user_parameter_schema={
            "type": "object",
            "properties": {"startDate": {"type": "string"}},
            "required": ["startDate"],
        },
        internal_config={"output_format": "geojson", "options": {"crs": 2056}},
        parameter_mapping={"startDate": "options.date_range.start"},
    )


def _mock_job_session() -> tuple[AsyncMock, list[str]]:
    """Session mock recording the status of the added job at each commit."""
    session = _session()
    committed: list[str] = []
    session.on_commit.side_effect = lambda: committed.append(session.add.call_args.args[0].status)
    return session, committed


@pytest.mark.asyncio
async def test_execute_process_triggers_dag_and_creates_job():
    session, committed = _mock_job_session()
    airflow = AsyncMock(spec=AirflowClient)
    process = _process()

    async def trigger_dag_run(*_args: Any) -> None:
        # The job must be committed (no transaction open) before Airflow is called
        assert committed == [JobStatus.SUBMITTING]

    airflow.trigger_dag_run.side_effect = trigger_dag_run

    job = await ProcessService(session, airflow).execute_process(
        process, {"startDate": "2026-01-01"}
    )

    session.add.assert_called_once_with(job)
    assert job.process_id == 1
    assert job.status == JobStatus.QUEUED
    assert committed == [JobStatus.SUBMITTING, JobStatus.QUEUED]
    airflow.trigger_dag_run.assert_awaited_once_with(
        "dummy_dag",
        job.dag_run_id,
        {
            "output_format": "geojson",
            "options": {"crs": 2056, "date_range": {"start": "2026-01-01"}},
        },
    )
    # internal config of the process must not be modified
    assert process.internal_config == {"output_format": "geojson", "options": {"crs": 2056}}


@pytest.mark.asyncio
async def test_execute_process_marks_job_failed_if_airflow_fails():
    session, committed = _mock_job_session()
    airflow = AsyncMock(spec=AirflowClient)
    airflow.trigger_dag_run.side_effect = RuntimeError("airflow down")

    with pytest.raises(RuntimeError):
        await ProcessService(session, airflow).execute_process(
            _process(), {"startDate": "2026-01-01"}
        )

    assert committed == [JobStatus.SUBMITTING, JobStatus.FAILED]


@pytest.mark.asyncio
async def test_execute_process_invalid_inputs():
    session = _session()
    airflow = AsyncMock(spec=AirflowClient)

    with pytest.raises(InvalidInputsError) as error:
        await ProcessService(session, airflow).execute_process(_process(), {"startDate": 1})

    assert error.value.errors == [
        {"loc": ["body", "inputs", "startDate"], "msg": "1 is not of type 'string'", "type": "type"}
    ]
    session.add.assert_not_called()
    airflow.trigger_dag_run.assert_not_awaited()


@pytest.mark.asyncio
async def test_execute_process_rejects_unknown_inputs():
    # The stored schema of _process() doesn't forbid additional properties, input_schema does
    session = _session()
    airflow = AsyncMock(spec=AirflowClient)

    with pytest.raises(InvalidInputsError) as error:
        await ProcessService(session, airflow).execute_process(
            _process(), {"startDate": "2026-01-01", "foo": 1}
        )

    assert error.value.errors == [
        {
            "loc": ["body", "inputs"],
            "msg": "Additional properties are not allowed ('foo' was unexpected)",
            "type": "additionalProperties",
        }
    ]
    session.add.assert_not_called()
    airflow.trigger_dag_run.assert_not_awaited()


@pytest.mark.asyncio
async def test_execute_process_input_without_mapping():
    # Misconfigured process: schema property without parameter mapping
    process = _process()
    process.user_parameter_schema["properties"]["endDate"] = {"type": "string"}
    session = _session()
    airflow = AsyncMock(spec=AirflowClient)

    with pytest.raises(ValueError, match="No parameter mapping for input 'endDate'"):
        await ProcessService(session, airflow).execute_process(
            process, {"startDate": "2026-01-01", "endDate": "2026-01-31"}
        )

    session.add.assert_not_called()
    airflow.trigger_dag_run.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path", "error", "match"),
    [
        ("options.crs", ValueError, "options.crs is already set by internal_config"),
        ("options", ValueError, "options is already set by internal_config"),
        ("output_format.x", TypeError, "output_format is not an object"),
    ],
)
async def test_execute_process_input_cannot_overwrite_internal_config(
    path: str, error: type[Exception], match: str
):
    # Misconfigured process: mapping targets a path already set by internal_config
    process = _process()
    process.parameter_mapping = {"startDate": path}
    session = _session()
    airflow = AsyncMock(spec=AirflowClient)

    with pytest.raises(error, match=match):
        await ProcessService(session, airflow).execute_process(process, {"startDate": "2026-01-01"})

    session.add.assert_not_called()
    airflow.trigger_dag_run.assert_not_awaited()
    assert process.internal_config == {"output_format": "geojson", "options": {"crs": 2056}}
