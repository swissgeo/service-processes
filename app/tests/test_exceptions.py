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

    assert response.status_code == 400
    body = json.loads(bytes(response.body))
    assert body["error"] == "Validation Error"
    assert body["detail"] is not None


@pytest.mark.asyncio
async def test_unified_exception_handler_http_exception_string_detail():
    exc = StarletteHTTPException(status_code=404, detail="Not Found")

    response = await unified_exception_handler(_make_request(), exc)

    assert response.status_code == 404
    body = json.loads(bytes(response.body))
    assert body["message"] == "Not Found"
    assert "detail" not in body


@pytest.mark.asyncio
async def test_http_exception_handler_dict_detail():
    exc = HTTPException(status_code=400, detail={"message": "Bad input", "detail": {"field": "x"}})

    response = await http_exception_handler(_make_request(), exc)

    body = json.loads(bytes(response.body))
    assert body["message"] == "Bad input"
    assert body["detail"] == {"field": "x"}


@pytest.mark.asyncio
async def test_unified_exception_handler_unhandled_exception():
    response = await unified_exception_handler(_make_request(), ValueError("boom"))

    assert response.status_code == 500
    body = json.loads(bytes(response.body))
    assert body["error"] == "internal server error"


def test_404_not_found_uses_unified_error_schema(client: TestClient):
    response = client.get("/this-route-does-not-exist")

    assert response.status_code == 404
    body = response.json()
    assert body["error"] == "Not Found"
    assert "message" in body
    assert "detail" not in body


def test_405_method_not_allowed_uses_unified_error_schema(client: TestClient):
    response = client.post("/checker")

    assert response.status_code == 405
    body = response.json()
    assert body["error"] == "Method Not Allowed"
    assert "message" in body
