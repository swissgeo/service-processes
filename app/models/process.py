from datetime import datetime
from enum import StrEnum, auto
from types import MappingProxyType
from typing import Any

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Index,
    String,
    Text,
    UniqueConstraint,
    desc,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base

SGID_MAX_LENGTH = 128


class Process(Base):
    """A process that users can run, backed by an Airflow DAG.
    There is no check that all processes of a dataset have the same unit and organization, but this
    should be the case. These are only used for authorization checks. At some point we could split
    this into a separate table for (or best read it from an outside cache). But for now we keep it
    as is."""

    __tablename__ = "process"
    __table_args__ = (UniqueConstraint("organization_sgid", "name"),)
    # When you access a database-generated field like sgid or timestamps that is not yet loaded in
    # the Python object, SQLAlchemy fires an extra hidden query to fetch its value ("lazy load").
    # With async SQLAlchemy that hidden query would crash, so we configure SQLAlchemy to fetch the
    # database-generated values immediately as part of the INSERT/UPDATE itself (using Postgres'
    # RETURNING clause). MappingProxyType makes this config dict read-only, so it can't be edited by
    # accident.
    __mapper_args__ = MappingProxyType({"eager_defaults": True})

    # only used as primary key
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Returned in api responses. Any string up to 128 characters, defaults to a TypeID with the
    # prefix "prc" (e.g. prc_01h455vb4pex5vsknk084sn02q) if not set.
    sgid: Mapped[str] = mapped_column(
        String(SGID_MAX_LENGTH), unique=True, server_default=text("typeid_generate('prc')")
    )
    # Organization, unit and dataset live outside this
    # service and will be used for authorization checks.
    organization_sgid: Mapped[str] = mapped_column(String(SGID_MAX_LENGTH))
    organization_unit_sgid: Mapped[str] = mapped_column(String(SGID_MAX_LENGTH))
    dataset_sgid: Mapped[str] = mapped_column(
        String(SGID_MAX_LENGTH),
        index=True,  # used for lookups
    )

    name: Mapped[str] = mapped_column(String(100))
    description: Mapped[str] = mapped_column(Text)
    dag_id: Mapped[str] = mapped_column(String(250))
    # JSON schema user must supply in requests
    user_parameter_schema: Mapped[dict[str, Any]] = mapped_column(JSONB)
    # Fixed parameter values passed to Airflow
    internal_config: Mapped[dict[str, Any]] = mapped_column(JSONB)
    # Maps user parameter names to what Airflow expects
    # example: {"startDate": "options.date_range.start"}
    # Each user input MUST have a mapping.
    parameter_mapping: Mapped[dict[str, str]] = mapped_column(JSONB)

    @property
    def input_schema(self) -> dict[str, Any]:
        """JSON schema user inputs are validated against, as published in the API.

        Same as user_parameter_schema, but unknown inputs are never allowed.
        """
        return {**self.user_parameter_schema, "additionalProperties": False}


class JobStatus(StrEnum):
    """Status of a job, stored as string.

    Possible status transitions:

    Happy path:
        SUBMITTING -> QUEUED -> RUNNING -> SUCCESS

    Other paths:
        SUBMITTING -> FAILED_SUBMIT
        SUBMITTING -> QUEUED -> RUNNING -> FAILED

    SUCCESS, FAILED and FAILED_SUBMIT are final states.
    """

    # service-processes internal status options
    SUBMITTING = auto()  # Job inserted, DAG run not (yet) triggered in Airflow
    FAILED_SUBMIT = auto()  # Triggering the DAG run failed

    # Airflow Dag run status options (must match the airflow states)
    QUEUED = "queued"  # DAG run triggered
    RUNNING = "running"  # DAG running
    SUCCESS = "success"  # DAG run completed successfully
    FAILED = "failed"  # DAG run failed


# Statuses of jobs whose dag run can still change in Airflow. Not included: SUBMITTING and
# FAILED_SUBMIT (the dag run doesn't (yet) exist), SUCCESS and FAILED (the dag run finished).
# Changing this requires a migration to recreate the ix_job_active_process_id partial index.
ACTIVE_STATUSES = (JobStatus.QUEUED, JobStatus.RUNNING)


class Job(Base):
    """A process run instance, backed by an airflow dag run.

    The `created_at` timestamp is set only in the database when the job is created (independent of
    airflow). Until the dag run is actually created in airflow, the job has status SUBMITTING and
    the timestamps `started_at` `finished_at` are null. Once airflow has the dag run created (even
    if not yet running), the status is updated to QUEUED. Only once airflow starts running the dag
    run the status is updated to RUNNING and the `started_at` is set. `finished_at` is set once the
    dag run is completed (either SUCCESS or FAILED).
    """

    __tablename__ = "job"
    # Jobs are listed per process, newest first (see ProcessService.list_jobs)
    __table_args__ = (
        Index("ix_job_process_id_created_at", "process_id", desc("created_at"), desc("id")),
        # We expect many jobs but not many active jobs compared to finished ones. When listing
        # jobs we refresh all the active ones. This index should improve selecting them.
        Index(
            "ix_job_active_process_id",
            "process_id",
            postgresql_where=text(
                f"status IN ({', '.join(f"'{status}'" for status in ACTIVE_STATUSES)})"
            ),
        ),
    )
    # When you access a database-generated field like sgid or timestamps that is not yet loaded in
    # the Python object, SQLAlchemy fires an extra hidden query to fetch its value ("lazy load").
    # With async SQLAlchemy that hidden query would crash, so we configure SQLAlchemy to fetch the
    # database-generated values immediately as part of the INSERT/UPDATE itself (using Postgres'
    # RETURNING clause). MappingProxyType makes this config dict read-only, so it can't be edited by
    # accident.
    __mapper_args__ = MappingProxyType({"eager_defaults": True})

    # only used as primary key
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    # Returned in api responses. Any string up to 128 characters, defaults to a TypeID with the
    # prefix "pjb" (e.g. pjb_01h455vb4pex5vsknk084sn02q) if not set.
    sgid: Mapped[str] = mapped_column(
        String(SGID_MAX_LENGTH), unique=True, server_default=text("typeid_generate('pjb')")
    )
    process_id: Mapped[int] = mapped_column(ForeignKey("process.id"))
    # lazy="raise": async sessions can't lazy-load, so fail loudly if a query forgot to load it
    process: Mapped[Process] = relationship(lazy="raise")

    # Chosen by us when triggering the DAG run, so a job can always be matched to its run
    dag_run_id: Mapped[str] = mapped_column(String(250), unique=True)
    status: Mapped[str] = mapped_column(String(128))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
