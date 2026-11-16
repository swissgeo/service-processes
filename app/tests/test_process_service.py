import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any
from unittest.mock import DEFAULT, AsyncMock, MagicMock

from sqlalchemy import ClauseElement
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

import pytest

from app.core.airflow import AirflowClient, AirflowDagRunResponse, AirflowError
from app.core.process_service import (
    IdempotencyKeyReusedError,
    InvalidInputsError,
    ProcessService,
    _hash_inputs,
    get_process_service,
)
from app.models.process import Job, JobStatus, Process

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


def _mock_session(rows: list[Any], total: int | None) -> AsyncMock:
    session = _session()
    session.scalars.return_value = MagicMock(all=MagicMock(return_value=rows))
    session.scalar.return_value = total
    return session


def _compile(statement: ClauseElement) -> str:
    return str(
        statement.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True})
    )


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


KEY = "0195f3a2-7c1e-7b3a-9d4e-00000000000a"
INPUTS = {"startDate": "2026-01-01"}


def _mock_job_session() -> tuple[AsyncMock, list[str]]:
    """Session mock without existing job for the key, recording the status of the added job at
    each commit after it was added."""
    session = _session()
    session.scalar.return_value = None
    committed: list[str] = []

    def on_commit() -> None:
        if session.add.called:
            committed.append(session.add.call_args.args[0].status)

    session.on_commit.side_effect = on_commit
    return session, committed


def _existing_job(inputs: dict[str, Any]) -> Job:
    return Job(
        sgid="pjb_existing",
        dag_run_id="run-existing",
        idempotency_key=KEY,
        request_hash=_hash_inputs(inputs),
        status=JobStatus.SUBMITTING,
        process=_process(),
    )


def _insert_race_session(existing: Job | None) -> AsyncMock:
    """Session mock where a concurrent request inserts its job between the lookup and the insert,
    and existing is what the second lookup returns."""
    session = _session()
    lookups = iter([None, existing])
    session.scalar.side_effect = lambda *_args: next(lookups)
    session.add.side_effect = IntegrityError("INSERT", {}, Exception("duplicate key"))
    return session


def _job(status: JobStatus) -> Job:
    return Job(sgid="pjb_123", dag_run_id="run-1", status=status, process=_process())


class TestListProcesses:
    @pytest.mark.asyncio
    async def test_list_processes_returns_processes_and_total(self):
        processes = [Process(name="A"), Process(name="B")]
        session = _mock_session(processes, total=12)

        result = await ProcessService(session, MagicMock()).list_processes(
            dataset_sgids=[DATASET], limit=2, offset=0
        )

        assert result == (processes, 12)

    @pytest.mark.asyncio
    async def test_list_processes_orders_and_paginates_query(self):
        session = _mock_session([], total=0)

        await ProcessService(session, MagicMock()).list_processes(
            dataset_sgids=[DATASET], limit=20, offset=40
        )

        statement = _compile(session.scalars.await_args.args[0])
        assert "ORDER BY process.name" in statement
        assert "LIMIT 20 OFFSET 40" in statement

    @pytest.mark.asyncio
    async def test_list_processes_filters_by_datasets(self):
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
    async def test_list_processes_counts_processes_of_datasets(self):
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
    async def test_list_processes_total_defaults_to_zero(self):
        session = _mock_session([], total=None)

        _, total = await ProcessService(session, MagicMock()).list_processes(
            dataset_sgids=[DATASET], limit=20, offset=0
        )

        assert total == 0

    @pytest.mark.parametrize(
        ("process_sgids", "dataset_sgids", "expected_where"),
        [
            pytest.param(
                ["prc_1"], None, "WHERE process.sgid IN ('prc_1') ORDER BY", id="processes"
            ),
            pytest.param(
                None, ["ds_1"], "WHERE process.dataset_sgid IN ('ds_1') ORDER BY", id="datasets"
            ),
            pytest.param(
                ["prc_1"],
                ["ds_1"],
                "WHERE process.sgid IN ('prc_1') AND process.dataset_sgid IN ('ds_1') ORDER BY",
                id="processes and datasets",
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_get_processes_filters(
        self, process_sgids: list[str] | None, dataset_sgids: list[str] | None, expected_where: str
    ):
        processes = [_process()]
        session = _mock_session(processes, total=None)

        result = await ProcessService(session, MagicMock()).get_processes(
            process_sgids, dataset_sgids
        )

        assert result == processes
        assert expected_where in _compile(session.scalars.await_args.args[0])


class TestGetProcess:
    @pytest.mark.asyncio
    async def test_get_process_queries_by_sgid(self):
        sgid = "0195f3a2-7c1e-7b3a-9d4e-1f2a3b4c5d6e"
        session = _session()

        await ProcessService(session, MagicMock()).get_process(sgid)

        statement = _compile(session.scalar.await_args.args[0])
        assert "WHERE process.sgid = '0195f3a2-7c1e-7b3a-9d4e-1f2a3b4c5d6e'" in statement

    @pytest.mark.asyncio
    async def test_get_process_returns_process(self):
        process = Process(name="A")
        session = _session()
        session.scalar.return_value = process

        result = await ProcessService(session, MagicMock()).get_process(str(uuid.uuid7()))

        assert result is process

    @pytest.mark.asyncio
    async def test_get_process_returns_none_when_not_found(self):
        session = _session()
        session.scalar.return_value = None

        result = await ProcessService(session, MagicMock()).get_process(str(uuid.uuid7()))

        assert result is None


class TestGetDatasets:
    @pytest.mark.asyncio
    async def test_get_datasets_queries_distinct_hierarchy_of_datasets(self):
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
    async def test_get_datasets_returns_tuples(self):
        rows = [(DATASET, str(uuid.uuid7()), str(uuid.uuid7()))]
        session = _session()
        session.execute.return_value = MagicMock(
            tuples=MagicMock(return_value=MagicMock(all=MagicMock(return_value=rows)))
        )

        result = await ProcessService(session, MagicMock()).get_datasets([DATASET])

        assert result == rows


class TestProcessService:
    def test_get_process_service_uses_session(self):
        session = _session()

        service = get_process_service(session, MagicMock())

        assert isinstance(service, ProcessService)
        assert service.session is session


class TestExecuteProcess:
    @pytest.mark.asyncio
    async def test_execute_process_triggers_dag_and_creates_job(self):
        session, committed = _mock_job_session()
        airflow = AsyncMock(spec=AirflowClient)
        process = _process()

        async def trigger_dag_run(*_args: Any) -> None:
            # The job must be committed (no transaction open) before Airflow is called
            assert committed == [JobStatus.SUBMITTING]

        airflow.trigger_dag_run.side_effect = trigger_dag_run

        job, created = await ProcessService(session, airflow).execute_process(process, INPUTS, KEY)

        assert created is True
        session.add.assert_called_once_with(job)
        assert job.process_id == 1
        assert job.status == JobStatus.QUEUED
        assert job.idempotency_key == KEY
        assert job.request_hash == _hash_inputs(INPUTS)
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
    async def test_execute_process_marks_job_failed_if_airflow_fails(self):
        session, committed = _mock_job_session()
        airflow = AsyncMock(spec=AirflowClient)
        airflow.trigger_dag_run.side_effect = RuntimeError("airflow down")

        with pytest.raises(RuntimeError):
            await ProcessService(session, airflow).execute_process(_process(), INPUTS, KEY)

        assert committed == [JobStatus.SUBMITTING, JobStatus.FAILED_SUBMIT]

    @pytest.mark.asyncio
    async def test_execute_process_queries_job_by_process_and_key(self):
        session, _committed = _mock_job_session()

        await ProcessService(session, AsyncMock(spec=AirflowClient)).execute_process(
            _process(), INPUTS, KEY
        )

        statement = _compile(session.scalar.await_args_list[0].args[0])
        assert f"WHERE job.process_id = 1 AND job.idempotency_key = '{KEY}'" in statement
        # The response includes the process sgid
        assert "JOIN process" in statement

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "status", [JobStatus.SUBMITTING, JobStatus.FAILED_SUBMIT, JobStatus.SUCCESS]
    )
    async def test_execute_process_returns_existing_job_for_same_key(self, status: JobStatus):
        existing = _existing_job(INPUTS)
        existing.status = status
        session = _session()
        session.scalar.return_value = existing
        airflow = AsyncMock(spec=AirflowClient)

        # Same inputs with the keys in another order
        job, created = await ProcessService(session, airflow).execute_process(
            _process(), dict(reversed(INPUTS.items())), KEY
        )

        assert job is existing
        assert created is False
        assert job.status == status  # no state is retried, whatever the status
        session.add.assert_not_called()
        airflow.trigger_dag_run.assert_not_awaited()
        airflow.get_dag_run.assert_not_awaited()  # not active, nothing to refresh

    @pytest.mark.asyncio
    @pytest.mark.parametrize("race", [False, True])
    async def test_execute_process_refreshes_existing_active_job(self, race: bool):
        existing = _existing_job(INPUTS)
        existing.status = JobStatus.QUEUED
        if race:
            session = _insert_race_session(existing)
        else:
            session = _session()
            session.scalar.return_value = existing
        airflow = AsyncMock(spec=AirflowClient)
        airflow.get_dag_run.return_value = AirflowDagRunResponse(
            state="running", start_date=datetime(2026, 1, 1, 10, 0, 5, tzinfo=UTC), end_date=None
        )

        job, created = await ProcessService(session, airflow).execute_process(
            _process(), INPUTS, KEY
        )

        assert job is existing
        assert created is False
        assert job.status == JobStatus.RUNNING
        airflow.get_dag_run.assert_awaited_once_with("dummy_dag", "run-existing")
        airflow.trigger_dag_run.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_execute_process_returns_existing_job_if_process_config_changed(self):
        # The mapping was changed after the first request, so the conf can't be built anymore
        process = _process()
        process.parameter_mapping = {"startDate": "options.crs"}
        session = _session()
        session.scalar.return_value = _existing_job(INPUTS)
        airflow = AsyncMock(spec=AirflowClient)

        job, created = await ProcessService(session, airflow).execute_process(process, INPUTS, KEY)

        assert job.sgid == "pjb_existing"
        assert created is False
        airflow.trigger_dag_run.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_execute_process_rejects_same_key_with_other_inputs(self):
        session = _session()
        session.scalar.return_value = _existing_job({"startDate": "2025-12-31"})
        airflow = AsyncMock(spec=AirflowClient)

        with pytest.raises(IdempotencyKeyReusedError):
            await ProcessService(session, airflow).execute_process(_process(), INPUTS, KEY)

        session.add.assert_not_called()
        airflow.trigger_dag_run.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_execute_process_returns_job_of_concurrent_request_with_same_key(self):
        existing = _existing_job(INPUTS)
        session = _insert_race_session(existing)
        airflow = AsyncMock(spec=AirflowClient)

        job, created = await ProcessService(session, airflow).execute_process(
            _process(), INPUTS, KEY
        )

        assert job is existing
        assert created is False
        airflow.trigger_dag_run.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_execute_process_rejects_concurrent_request_with_same_key_and_other_inputs(
        self,
    ):
        session = _insert_race_session(_existing_job({"startDate": "2025-12-31"}))
        airflow = AsyncMock(spec=AirflowClient)

        with pytest.raises(IdempotencyKeyReusedError):
            await ProcessService(session, airflow).execute_process(_process(), INPUTS, KEY)

        airflow.trigger_dag_run.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_execute_process_reraises_other_integrity_errors(self):
        session = _insert_race_session(None)  # no job with the key, another constraint failed
        airflow = AsyncMock(spec=AirflowClient)

        with pytest.raises(IntegrityError):
            await ProcessService(session, airflow).execute_process(_process(), INPUTS, KEY)

        airflow.trigger_dag_run.assert_not_awaited()

    def test_hash_inputs_ignores_key_order(self):
        assert _hash_inputs({"a": 1, "b": {"c": 2, "d": 3}}) == _hash_inputs(
            {"b": {"d": 3, "c": 2}, "a": 1}
        )
        assert _hash_inputs({"a": 1}) != _hash_inputs({"a": 2})

    @pytest.mark.asyncio
    async def test_execute_process_invalid_inputs(self):
        session = _session()
        airflow = AsyncMock(spec=AirflowClient)

        with pytest.raises(InvalidInputsError) as error:
            await ProcessService(session, airflow).execute_process(
                _process(), {"startDate": 1}, KEY
            )

        assert error.value.errors == [
            {
                "loc": ["body", "inputs", "startDate"],
                "msg": "1 is not of type 'string'",
                "type": "type",
            }
        ]
        session.add.assert_not_called()
        airflow.trigger_dag_run.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_execute_process_rejects_unknown_inputs(self):
        # The stored schema of _process() doesn't forbid additional properties, input_schema does
        session = _session()
        airflow = AsyncMock(spec=AirflowClient)

        with pytest.raises(InvalidInputsError) as error:
            await ProcessService(session, airflow).execute_process(
                _process(), {"startDate": "2026-01-01", "foo": 1}, KEY
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
    async def test_execute_process_input_without_mapping(self):
        # Misconfigured process: schema property without parameter mapping
        process = _process()
        process.user_parameter_schema["properties"]["endDate"] = {"type": "string"}
        session, _committed = _mock_job_session()
        airflow = AsyncMock(spec=AirflowClient)

        with pytest.raises(ValueError, match="No parameter mapping for input 'endDate'"):
            await ProcessService(session, airflow).execute_process(
                process, {"startDate": "2026-01-01", "endDate": "2026-01-31"}, KEY
            )

        session.add.assert_not_called()
        airflow.trigger_dag_run.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("path", "error", "match"),
        [
            pytest.param(
                "options.crs",
                ValueError,
                "options.crs is already set by internal_config",
                id="overwrite leaf",
            ),
            pytest.param(
                "options",
                ValueError,
                "options is already set by internal_config",
                id="overwrite object",
            ),
            pytest.param(
                "output_format.x",
                TypeError,
                "output_format is not an object",
                id="write object into string",
            ),
        ],
    )
    async def test_execute_process_input_cannot_overwrite_internal_config(
        self, path: str, error: type[Exception], match: str
    ):
        # Misconfigured process: mapping targets a path already set by internal_config
        process = _process()
        process.parameter_mapping = {"startDate": path}
        session, _committed = _mock_job_session()
        airflow = AsyncMock(spec=AirflowClient)

        with pytest.raises(error, match=match):
            await ProcessService(session, airflow).execute_process(process, INPUTS, KEY)

        session.add.assert_not_called()
        airflow.trigger_dag_run.assert_not_awaited()
        assert process.internal_config == {"output_format": "geojson", "options": {"crs": 2056}}


class TestGetJob:
    @pytest.mark.asyncio
    async def test_get_job_queries_by_sgid_with_process(self):
        session = _session()

        await ProcessService(session, MagicMock()).get_job("pjb_123")

        statement = _compile(session.scalar.await_args.args[0])
        assert "WHERE job.sgid = 'pjb_123'" in statement
        # The process is needed for the authorization check and must be loaded with the job
        assert "JOIN process" in statement

    @pytest.mark.asyncio
    async def test_get_job_returns_job(self):
        job = Job(sgid="pjb_123")
        session = _session()
        session.scalar.return_value = job

        result = await ProcessService(session, MagicMock()).get_job("pjb_123")

        assert result is job

    @pytest.mark.asyncio
    async def test_get_job_returns_none_when_not_found(self):
        session = _session()
        session.scalar.return_value = None

        result = await ProcessService(session, MagicMock()).get_job("pjb_123")

        assert result is None

    @pytest.mark.parametrize(
        "status",
        [
            pytest.param(JobStatus.SUBMITTING, id="submitting"),
            pytest.param(JobStatus.FAILED_SUBMIT, id="failed submit"),
            pytest.param(JobStatus.SUCCESS, id="success"),
            pytest.param(JobStatus.FAILED, id="failed"),
        ],
    )
    @pytest.mark.asyncio
    async def test_get_updated_job_skips_airflow(self, status: JobStatus):
        session = _session()
        airflow = AsyncMock(spec=AirflowClient)
        job = _job(status)

        result = await ProcessService(session, airflow).get_updated_job(job)

        assert result is job
        assert job.status == status
        airflow.get_dag_run.assert_not_awaited()
        session.on_commit.assert_not_called()

    @pytest.mark.parametrize(
        "status",
        [
            pytest.param(JobStatus.QUEUED, id="queued"),
            pytest.param(JobStatus.RUNNING, id="running"),
        ],
    )
    @pytest.mark.asyncio
    async def test_get_updated_job_updates_status_from_dag_run(self, status: JobStatus):
        session = _session()
        committed: list[str] = []
        job = _job(status)
        session.on_commit.side_effect = lambda: committed.append(job.status)
        airflow = AsyncMock(spec=AirflowClient)
        started_at = datetime(2026, 1, 1, 10, 0, 5, tzinfo=UTC)
        finished_at = datetime(2026, 1, 1, 10, 1, 0, tzinfo=UTC)
        airflow.get_dag_run.return_value = AirflowDagRunResponse(
            state="success", start_date=started_at, end_date=finished_at
        )

        result = await ProcessService(session, airflow).get_updated_job(job)

        assert result is job
        assert job.status == JobStatus.SUCCESS
        assert job.started_at == started_at
        assert job.finished_at == finished_at
        assert committed == [JobStatus.SUCCESS]
        airflow.get_dag_run.assert_awaited_once_with("dummy_dag", "run-1")

    @pytest.mark.asyncio
    async def test_get_updated_job_keeps_status_if_airflow_fails(
        self, caplog: pytest.LogCaptureFixture
    ):
        session = _session()
        airflow = AsyncMock(spec=AirflowClient)
        airflow.get_dag_run.side_effect = AirflowError("airflow down")
        job = _job(JobStatus.RUNNING)

        result = await ProcessService(session, airflow).get_updated_job(job)

        assert result is job
        assert job.status == JobStatus.RUNNING
        session.on_commit.assert_not_called()
        assert "Could not refresh job pjb_123 from Airflow" in caplog.text


class TestListJobs:
    @pytest.mark.asyncio
    async def test_list_jobs_without_processes_skips_queries(self):
        session = _session()
        airflow = AsyncMock(spec=AirflowClient)

        result = await ProcessService(session, airflow).list_jobs([], None, limit=20, offset=0)

        assert result == ([], 0)
        session.begin.assert_not_called()
        airflow.get_dag_run.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_list_jobs_refreshes_before_query(self):
        jobs = [_job(JobStatus.SUCCESS)]
        session = _mock_session(jobs, total=3)
        service = ProcessService(session, MagicMock())
        calls: list[str] = []
        service.refresh_active_jobs = AsyncMock(side_effect=lambda _ids: calls.append("refresh"))
        session.scalars.side_effect = lambda *_: calls.append("query") or DEFAULT

        result = await service.list_jobs([1], None, limit=20, offset=0)

        assert result == (jobs, 3)
        assert calls == ["refresh", "query"]
        service.refresh_active_jobs.assert_awaited_once_with([1])

    @pytest.mark.asyncio
    async def test_list_jobs_query(self):
        session = _mock_session([], total=0)
        service = ProcessService(session, MagicMock())
        service.refresh_active_jobs = AsyncMock()

        await service.list_jobs(
            [1, 2], [JobStatus.QUEUED, JobStatus.FAILED_SUBMIT], limit=20, offset=40
        )

        statement = _compile(session.scalars.await_args.args[0])
        assert "JOIN process" in statement
        assert (
            "WHERE job.process_id IN (1, 2) AND job.status IN ('queued', 'failed_submit') "
            "ORDER BY job.created_at DESC, job.id DESC \n LIMIT 20 OFFSET 40"
        ) in statement
        assert _compile(session.scalar.await_args.args[0]) == (
            "SELECT count(*) AS count_1 \nFROM job \n"
            "WHERE job.process_id IN (1, 2) AND job.status IN ('queued', 'failed_submit')"
        )

    @pytest.mark.asyncio
    async def test_list_jobs_without_status_filter(self):
        session = _mock_session([], total=0)
        service = ProcessService(session, MagicMock())
        service.refresh_active_jobs = AsyncMock()

        await service.list_jobs([1], None, limit=20, offset=0)

        assert _compile(session.scalar.await_args.args[0]) == (
            "SELECT count(*) AS count_1 \nFROM job \nWHERE job.process_id IN (1)"
        )

    @pytest.mark.asyncio
    async def test_refresh_active_jobs_queries_active_jobs_of_processes(self):
        session = _mock_session([], total=None)
        airflow = AsyncMock(spec=AirflowClient)

        await ProcessService(session, airflow).refresh_active_jobs([1, 2])

        statement = _compile(session.scalars.await_args.args[0])
        assert "JOIN process" in statement
        assert "WHERE job.process_id IN (1, 2) AND job.status IN ('queued', 'running')" in statement
        airflow.get_dag_run.assert_not_awaited()
        session.on_commit.assert_called_once()  # only the select, nothing to write

    @pytest.mark.asyncio
    async def test_refresh_active_jobs_updates_jobs(self, caplog: pytest.LogCaptureFixture):
        finished = _job(JobStatus.RUNNING)
        unreachable = Job(
            sgid="pjb_456", dag_run_id="run-2", status=JobStatus.QUEUED, process=_process()
        )
        session = _mock_session([finished, unreachable], total=None)
        in_transaction_during_airflow: list[bool] = []
        airflow = AsyncMock(spec=AirflowClient)
        started_at = datetime(2026, 1, 1, 10, 0, 5, tzinfo=UTC)
        finished_at = datetime(2026, 1, 1, 10, 1, 0, tzinfo=UTC)

        async def get_dag_run(_dag_id: str, dag_run_id: str) -> AirflowDagRunResponse:
            # Only the initial select must have been committed, no transaction open
            in_transaction_during_airflow.append(session.on_commit.call_count != 1)
            if dag_run_id == "run-2":
                raise AirflowError("airflow down")
            return AirflowDagRunResponse(
                state="success", start_date=started_at, end_date=finished_at
            )

        airflow.get_dag_run.side_effect = get_dag_run

        await ProcessService(session, airflow).refresh_active_jobs([1])

        assert in_transaction_during_airflow == [False, False]
        assert finished.status == JobStatus.SUCCESS
        assert finished.started_at == started_at
        assert finished.finished_at == finished_at
        assert unreachable.status == JobStatus.QUEUED
        assert session.on_commit.call_count == 2  # select + write of the results
        assert "Could not refresh job pjb_456 from Airflow" in caplog.text
