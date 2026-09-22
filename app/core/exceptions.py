from http import HTTPStatus

from starlette.requests import Request
from starlette.responses import Response

from fastapi import FastAPI, HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.schemas.errors import ErrorResponse


async def validation_exception_handler(
    _request: Request, exc: RequestValidationError
) -> JSONResponse:
    """
    Normalize all RequestValidationError outputs into a single JSON schema with
    HTTP 400 Bad Request status code.

    By default FastAPI returns a 422 Unprocessable Entity status code for
    validation errors, but for consistency with other APIs we return
    400 Bad Request instead since the client request is invalid in either case.
    """
    return JSONResponse(
        status_code=400,
        content=ErrorResponse(
            error="Validation Error",
            message="Invalid request payload",
            detail=exc.errors() or None,
        ).model_dump(),
    )


async def http_exception_handler(_request: Request, exc: HTTPException) -> JSONResponse:
    """
    Normalize all HTTPException outputs into a single JSON schema.
    """

    # Default fallback values (in case detail is a string)
    error = HTTPStatus(exc.status_code).phrase
    message = str(exc.detail)
    detail = None

    # If detail is already structured, extract it
    if isinstance(exc.detail, dict):
        message = exc.detail.get("message", message)
        detail = exc.detail.get("detail")

    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            error=error,
            message=message,
            detail=detail,
        ).model_dump(exclude_none=True),
    )


async def unified_exception_handler(request: Request, exc: Exception) -> Response:
    if isinstance(exc, RequestValidationError):
        return await validation_exception_handler(request, exc)

    if isinstance(exc, HTTPException):
        return await http_exception_handler(request, exc)

    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            error="internal server error",
            message="An unexpected error occurred",
        ).model_dump(),
    )


def register_exception_handlers(app: FastAPI) -> None:
    # Register handlers for FastAPI/Starlette exception classes explicitly.
    #
    # FastAPI installs dedicated handlers for common framework exceptions such as:
    # - RequestValidationError: invalid request body/path/query parameters (422)
    # - HTTPException: expected client/server errors raised by routes/services
    #
    # Those built-in handlers take precedence over a generic Exception handler, so
    # registering only Exception would NOT unify these responses. To guarantee that
    # validation errors, explicit HTTP errors, and unexpected uncaught exceptions all
    # pass through the same response formatter/logging path, each category must be
    # registered separately.
    #
    # Order does not matter here; Starlette resolves the most specific matching type.
    app.add_exception_handler(Exception, unified_exception_handler)
    app.add_exception_handler(RequestValidationError, unified_exception_handler)
    app.add_exception_handler(HTTPException, unified_exception_handler)
