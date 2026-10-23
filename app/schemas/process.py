import uuid
from typing import Annotated, Any, Self

from pydantic import BaseModel, Field

from app.models import process as models
from app.schemas.pagination import PaginatedResponse

sgid = Annotated[
    uuid.UUID,
    Field(description="ID of a process"),
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
