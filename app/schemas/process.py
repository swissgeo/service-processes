from datetime import datetime
from typing import Annotated, Any, Self

from pydantic import BaseModel, BeforeValidator, Field

from app.models import process as models
from app.schemas.pagination import PaginatedResponse, PaginationParams

SGID_MAX_LENGTH = 128

# SGIDs are arbitrary strings (not necessarily UUIDs), limited to the length of the DB columns.
SgidStr = Annotated[str, Field(min_length=1, max_length=SGID_MAX_LENGTH)]

# Max 30 datasets in query as this is the limit of a batch request to AWS Verified permissions.
MAX_DATASETS = 30


def _split_comma_separated(value: Any) -> Any:
    """Split comma separated query values, e.g. ["a,b", "c"] -> ["a", "b", "c"]."""
    if isinstance(value, list):
        return [part.strip() for item in value for part in str(item).split(",") if part.strip()]
    return value


class ListProcessesParams(PaginationParams):
    """Query parameters of the list processes endpoint."""

    datasets: Annotated[
        list[SgidStr],
        Field(
            description=f"Comma separated IDs of the datasets (1 to {MAX_DATASETS})",
            min_length=1,
            max_length=MAX_DATASETS,
        ),
        BeforeValidator(_split_comma_separated),
    ]


class Process(BaseModel):
    sgid: SgidStr = Field(description="Identifier of the process")
    name: str = Field(description="Name of the process", examples=["Export dataset"])
    description: str = Field(description="Description of what the process does")
    inputs: dict[str, Any] = Field(
        description="JSON schema of the parameters to supply when starting the process"
    )

    @classmethod
    def from_model(cls, process: models.Process) -> Self:
        return cls(
            sgid=process.sgid,
            name=process.name,
            description=process.description,
            inputs=process.input_schema,
        )


class ProcessResponse(Process):
    pass


class ListProcessesResponse(PaginatedResponse[Process]):
    pass


class ProcessInput(BaseModel):
    inputs: dict[str, Any] = Field(
        description="Parameters of the process, must match the JSON schema in `inputs` of the "
        "process"
    )


class Job(BaseModel):
    sgid: SgidStr = Field(description="Identifier of the job")
    status: str = Field(description="Status of the job", examples=["queued"])
    created_at: datetime = Field(description="When the Job was initialized")
    started_at: datetime | None = Field(description="When the Job started to run")
    finished_at: datetime | None = Field(description="When the Job completed")

    @classmethod
    def from_model(cls, job: models.Job) -> Self:
        return cls(
            sgid=job.sgid,
            status=job.status,
            created_at=job.created_at,
            started_at=job.started_at,
            finished_at=job.finished_at,
        )


class JobResponse(Job):
    pass
