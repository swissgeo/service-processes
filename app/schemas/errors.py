from typing import Any

from pydantic import BaseModel, Field


class ErrorResponse(BaseModel):
    error: str = Field(description="Error text code", examples=["validation_error"])
    message: str = Field(
        description="Error message",
    )
    detail: Any | None = Field(
        description="Details on the error",
        default=None,
    )
