import uuid
from typing import Annotated, Any, Self

from pydantic import BaseModel, BeforeValidator, Field

from app.models import process as models
from app.schemas.pagination import PaginatedResponse, PaginationParams

sgid = Annotated[
    uuid.UUID,
    Field(description="ID of a process"),
]

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
        list[uuid.UUID],
        Field(
            description=f"Comma separated IDs of the datasets (1 to {MAX_DATASETS})",
            min_length=1,
            max_length=MAX_DATASETS,
        ),
        BeforeValidator(_split_comma_separated),
    ]


class Process(BaseModel):
    sgid: uuid.UUID = Field(description="Identifier of the process")
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
            inputs=process.user_parameter_schema,
        )


class ProcessResponse(Process):
    pass


class ListProcessesResponse(PaginatedResponse[Process]):
    pass
