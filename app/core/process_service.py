import copy
import logging
import uuid
from collections.abc import Sequence
from typing import Annotated, Any

from jsonschema import Draft202012Validator, FormatChecker
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from fastapi import Depends

from app.core.airflow import AirflowClient, AirflowDep, AirflowError
from app.core.database import DatabaseSessionDep
from app.models.process import Job, JobStatus, Process

logger = logging.getLogger(__name__)


class ProcessService:
    """Reads and manages processes.

    Every method runs its statements in an explicit `async with self.session.begin():` block, so
    that transactions are short and none is open during external calls (authorization, Airflow).
    A statement outside such a block would autobegin a transaction and make the next begin() fail.
    """

    def __init__(self, session: AsyncSession, airflow: AirflowClient) -> None:
        self.session = session
        self.airflow = airflow

    async def list_processes(
        self, dataset_sgids: Sequence[str], limit: int, offset: int
    ) -> tuple[Sequence[Process], int]:
        """Return one page of processes of the given datasets and their total number."""
        in_datasets = Process.dataset_sgid.in_(dataset_sgids)
        async with self.session.begin():
            processes = await self.session.scalars(
                select(Process)
                .where(in_datasets)
                .order_by(Process.name)
                .limit(limit)
                .offset(offset)
            )
            total = await self.session.scalar(
                select(func.count()).select_from(Process).where(in_datasets)
            )
            return processes.all(), total or 0

    async def get_process(self, sgid: str) -> Process | None:
        """Return the process with the given sgid, or None if it doesn't exist."""
        async with self.session.begin():
            return await self.session.scalar(select(Process).where(Process.sgid == sgid))

    async def get_datasets(self, dataset_sgids: Sequence[str]) -> Sequence[tuple[str, str, str]]:
        """Get tuple of (dataset, unit, organization) of provided datasets."""
        async with self.session.begin():
            rows = await self.session.execute(
                select(
                    Process.dataset_sgid, Process.organization_unit_sgid, Process.organization_sgid
                )
                .where(Process.dataset_sgid.in_(dataset_sgids))
                .distinct()
            )
            return rows.tuples().all()

    async def execute_process(self, process: Process, inputs: dict[str, Any]) -> Job:
        """Start a process and return the job tracking it.

        The user inputs are validated against the process schema, mapped and merged into the
        internal config and passed as conf to a new run of the process DAG in Airflow.

        Raises InvalidInputsError if the inputs don't match the schema of the process.
        """
        errors = _validate_inputs(process.input_schema, inputs)
        if errors:
            raise InvalidInputsError(errors)
        conf = _build_dag_conf(process.internal_config, process.parameter_mapping, inputs)

        job = Job(process_id=process.id, dag_run_id=str(uuid.uuid7()), status=JobStatus.SUBMITTING)
        async with self.session.begin():
            self.session.add(job)  # eager_defaults: sgid etc. are fetched on insert

        try:
            await self.airflow.trigger_dag_run(process.dag_id, job.dag_run_id, conf)
        except Exception:
            async with self.session.begin():
                job.status = JobStatus.FAILED_SUBMIT
            raise

        async with self.session.begin():
            job.status = JobStatus.QUEUED
        return job

    async def get_job(self, sgid: str) -> Job | None:
        """Return the job with the given sgid, or None if it doesn't exist."""
        async with self.session.begin():
            return await self.session.scalar(
                select(Job).options(joinedload(Job.process)).where(Job.sgid == sgid)
            )

    async def get_updated_job(self, job: Job) -> Job:
        """Return the job after getting the latest information from the airflow dag run.
        Only get the dag run information if the job state shows there might be a change.
        """
        if job.status in (
            JobStatus.SUBMITTING,  # internal state -> dag run does not (yet) exist in airflow
            JobStatus.FAILED_SUBMIT,  # internal state -> dag run does not exist in airflow
            JobStatus.SUCCESS,  # dag run finished, no updates to get
            JobStatus.FAILED,  # dag run finished, no updates to get
        ):
            return job
        try:
            dag_run_details = await self.airflow.get_dag_run(job.process.dag_id, job.dag_run_id)
        except AirflowError:
            logger.warning("Could not refresh job %s from Airflow", job.sgid, exc_info=True)
            return job
        async with self.session.begin():
            job.status = dag_run_details.state
            job.started_at = dag_run_details.start_date
            job.finished_at = dag_run_details.end_date
        return job


class InvalidInputsError(Exception):
    """The user inputs don't match the JSON schema of the process."""

    def __init__(self, errors: list[dict[str, Any]]) -> None:
        super().__init__("Invalid process inputs")
        self.errors = errors


# FastAPI dependency injection function and definitions
def get_process_service(session: DatabaseSessionDep, airflow: AirflowDep) -> ProcessService:
    return ProcessService(session, airflow)


ProcessServiceDep = Annotated[ProcessService, Depends(get_process_service)]


def _validate_inputs(schema: dict[str, Any], inputs: dict[str, Any]) -> list[dict[str, Any]]:
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    return [
        {
            "loc": ["body", "inputs", *error.absolute_path],
            "msg": error.message,
            "type": error.validator,
        }
        for error in validator.iter_errors(inputs)
    ]


def _build_dag_conf(
    internal_config: dict[str, Any],
    parameter_mapping: dict[str, str],
    inputs: dict[str, Any],
) -> dict[str, Any]:
    """Combine fixed internal values and mapped user inputs into the conf of a DAG run.

    Example: mapping {"startDate": "options.date_range.start"} and inputs
    {"startDate": "2026-01-01"} result in {"options": {"date_range": {"start": "2026-01-01"}}}
    merged into internal_config.

    If anything in input already exists in internal_config, an error is raised.
    """
    conf = copy.deepcopy(internal_config)  # to not change the ORM attribute values
    for name, value in inputs.items():
        path = parameter_mapping.get(name)
        if path is None:
            # Each user input must have a mapping
            raise ValueError(f"No parameter mapping for input '{name}'")
        _set_path(conf, path.split("."), value)
    return conf


def _set_path(target: dict[str, Any], keys: list[str], value: Any) -> None:
    """Sets the value under the path keys in the target. If a value already
    exists in the target, an error is raised.
    """
    *parents, last = keys
    for key in parents:
        target = target.setdefault(key, {})
        if not isinstance(target, dict):
            raise TypeError(f"Cannot set {'.'.join(keys)}: {key} is not an object")
    if last in target:
        # This prevents user input from overwriting anything in internal_config.
        raise ValueError(f"{'.'.join(keys)} is already set by internal_config")
    target[last] = value
