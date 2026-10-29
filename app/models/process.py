import uuid
from datetime import datetime
from types import MappingProxyType
from typing import Any

from sqlalchemy import BigInteger, DateTime, String, Text, UniqueConstraint, func, text
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
    # Fetch server-generated values (sgid, timestamps) via RETURNING on INSERT and UPDATE, so
    # accessing them afterwards doesn't trigger a lazy load (not allowed with asyncio).
    __mapper_args__ = MappingProxyType({"eager_defaults": True})

    # only used as primary key
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Returned in api responses
    sgid: Mapped[uuid.UUID] = mapped_column(unique=True, server_default=text("uuidv7()"))
    # Organization, unit and dataset live outside this
    # service and will be used for authorization checks.
    organization_sgid: Mapped[uuid.UUID]
    organization_unit_sgid: Mapped[uuid.UUID]
    dataset_sgid: Mapped[uuid.UUID] = mapped_column(index=True)  # used for lookups

    name: Mapped[str] = mapped_column(String(100))
    description: Mapped[str] = mapped_column(Text)
    dag_id: Mapped[str] = mapped_column(String(250))
    # JSON schema user must supply in requests
    user_parameter_schema: Mapped[dict[str, Any]] = mapped_column(JSONB)
    # Fixed parameter values passed to Airflow
    internal_config: Mapped[dict[str, Any]] = mapped_column(JSONB)
    # Maps user parameter names to what Airflow expects
    # example: {"startDate": "options.date_range.start"}
    parameter_mapping: Mapped[dict[str, str]] = mapped_column(JSONB)
