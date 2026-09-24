import json

from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.requests import Request

from fastapi import HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.testclient import TestClient

import pytest

from app.core.exceptions import (
    http_exception_handler,
    unified_exception_handler,
)


def _make_request() -> Request:
    """Minimal Request for handlers that never inspect the request itself."""
    return Request(scope={"type": "http"})


@pytest.mark.asyncio
async def test_unified_exception_handler_validation_error():
    exc = RequestValidationError(
        errors=[{"loc": ["body", "field"], "msg": "field required", "type": "missing"}]
    )

    response = await unified_exception_handler(_make_request(), exc)

    expected = {
        "error": "Validation Error",
        "message": "Invalid request payload",
        "detail": [{"loc": ["body", "field"], "msg": "field required", "type": "missing"}],
    }

    assert response.status_code == 400
    body = json.loads(bytes(response.body))
    assert body == expected


@pytest.mark.asyncio
async def test_unified_exception_handler_http_exception_string_detail():
    exc = StarletteHTTPException(status_code=404, detail="Not Found")

    response = await unified_exception_handler(_make_request(), exc)

    expected = {"error": "Not Found", "message": "Not Found"}

    assert response.status_code == 404
    body = json.loads(bytes(response.body))
    assert body == expected


@pytest.mark.asyncio
async def test_http_exception_handler_dict_detail():
    exc = HTTPException(status_code=400, detail={"message": "Bad input", "detail": {"field": "x"}})

    response = await http_exception_handler(_make_request(), exc)

    expected = {"error": "Bad Request", "message": "Bad input", "detail": {"field": "x"}}

    body = json.loads(bytes(response.body))
    assert body == expected


@pytest.mark.asyncio
async def test_unified_exception_handler_unhandled_exception():
    response = await unified_exception_handler(_make_request(), ValueError("boom"))

    expected = {"error": "Internal Server Error", "message": "An unexpected error occurred"}

    assert response.status_code == 500
    body = json.loads(bytes(response.body))
    assert body == expected


def test_404_not_found_uses_unified_error_schema(client: TestClient):
    response = client.get("/this-route-does-not-exist")

    expected = {"error": "Not Found", "message": "Not Found"}

    assert response.status_code == 404
    body = response.json()
    assert body == expected


def test_405_method_not_allowed_uses_unified_error_schema(client: TestClient):
    response = client.post("/checker")

    expected = {"error": "Method Not Allowed", "message": "Method Not Allowed"}

    assert response.status_code == 405
    body = response.json()
    assert body == expected
