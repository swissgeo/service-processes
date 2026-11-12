from datetime import datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Any

from sqlalchemy import BigInteger, DateTime, ForeignKey, String, Text, UniqueConstraint, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


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
        String(128), unique=True, server_default=text("typeid_generate('prc')")
    )
    # Organization, unit and dataset live outside this
    # service and will be used for authorization checks.
    organization_sgid: Mapped[str] = mapped_column(String(128))
    organization_unit_sgid: Mapped[str] = mapped_column(String(128))
    dataset_sgid: Mapped[str] = mapped_column(String(128), index=True)  # used for lookups

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
    """Status of a job, stored as string."""

    # TODO: This might change once the get Job endpoints are implemented and we align with the
    # airflow status options.

    # Job inserted, DAG run not (yet) triggered successfully in Airflow
    SUBMITTING = "submitting"
    # DAG run triggered in Airflow
    QUEUED = "queued"
    # Triggering the DAG run failed
    FAILED = "failed"


class Job(Base):
    """A process run instance, backed by an airflow dag run."""

    __tablename__ = "job"
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
        String(128), unique=True, server_default=text("typeid_generate('pjb')")
    )
    process_id: Mapped[int] = mapped_column(ForeignKey("process.id"), index=True)

    # Chosen by us when triggering the DAG run, so a job can always be matched to its run
    dag_run_id: Mapped[str] = mapped_column(String(250), unique=True)
    status: Mapped[str] = mapped_column(String(128))
