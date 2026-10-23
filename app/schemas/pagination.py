from typing import Any

from pydantic import (
    BaseModel,
    Field,
    ValidationError,
    ValidatorFunctionWrapHandler,
    field_validator,
)

DEFAULT_LIMIT = 50
MAX_LIMIT = 100


class PaginatedResponse[T](BaseModel):
    """Base model for paginated list responses."""

    items: list[T]
    total: int = Field(description="Total number of items, independent of pagination")
    limit: int = Field(description="Maximum number of items returned")
    offset: int = Field(description="Number of items skipped")


class PaginationParams(BaseModel):
    """Pagination query parameters.

    Invalid values don't fail the request: out of range values are clamped to the closest valid
    value and non-integer values fall back to the default.
    """

    limit: int = Field(
        default=DEFAULT_LIMIT,
        description=f"Maximum number of items to return, values above {MAX_LIMIT} are capped",
        # Only documents the range in OpenAPI, the validator below does the clamping
        json_schema_extra={"minimum": 1, "maximum": MAX_LIMIT},
    )
    offset: int = Field(
        default=0,
        description="Number of items to skip",
        json_schema_extra={"minimum": 0},
    )

    @field_validator("limit", mode="wrap")
    @classmethod
    def clamp_limit(cls, value: Any, handler: ValidatorFunctionWrapHandler) -> int:
        try:
            limit = handler(value)
        except ValidationError:
            return DEFAULT_LIMIT
        return min(max(limit, 1), MAX_LIMIT)

    @field_validator("offset", mode="wrap")
    @classmethod
    def clamp_offset(cls, value: Any, handler: ValidatorFunctionWrapHandler) -> int:
        try:
            offset = handler(value)
        except ValidationError:
            return 0
        return max(offset, 0)
