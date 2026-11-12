import json
from collections.abc import AsyncGenerator, Callable
<<<<<<< HEAD
from datetime import UTC, datetime
=======
>>>>>>> 9e5f173 (GPS-1011: Local airflow)
from typing import Any

import httpx
import pytest_asyncio

from fastapi import FastAPI

import pytest

from app.core import airflow
from app.core.airflow import (
    AirflowClient,
<<<<<<< HEAD
    AirflowDagRunResponse,
    AirflowError,
=======
>>>>>>> 9e5f173 (GPS-1011: Local airflow)
    initialize_airflow_client,
    k8s_token_request,
    password_token_request,
)
from app.settings import Settings

HOST = "http://airflow.test"
TOKEN_TTL = 3600
K8S_TOKEN = "k8s-service-account-token"  # noqa: S105


class FakeAirflow:
    """Records requests and answers like Airflow: issues numbered tokens on /auth/token."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.issued_tokens = 0
        # Status codes returned by the API (not /auth/token), in order, then 200
        self.api_statuses: list[int] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if request.url.path == "/auth/token":
            self.issued_tokens += 1
            return httpx.Response(201, json={"access_token": f"jwt-{self.issued_tokens}"})
        status = self.api_statuses.pop(0) if self.api_statuses else 200
        return httpx.Response(status, json={})

    @property
    def api_requests(self) -> list[httpx.Request]:
        return [r for r in self.requests if r.url.path != "/auth/token"]

    @property
    def token_requests(self) -> list[httpx.Request]:
        return [r for r in self.requests if r.url.path == "/auth/token"]


class Clock:
    """Replacement for time.monotonic that only advances when told to."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def fake_airflow() -> FakeAirflow:
    return FakeAirflow()


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Clock:
    clock = Clock()
    monkeypatch.setattr(airflow.time, "monotonic", clock)
    return clock


@pytest.fixture(autouse=True)
def k8s_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(airflow, "_read_k8s_token", lambda: K8S_TOKEN)


@pytest_asyncio.fixture
async def airflow_client(fake_airflow: FakeAirflow) -> AsyncGenerator[AirflowClient]:
    client = AirflowClient(HOST, TOKEN_TTL, 10, transport=httpx.MockTransport(fake_airflow.handler))
    yield client
    await client.aclose()


def _trigger(client: AirflowClient) -> Callable:
    return lambda: client.trigger_dag_run("my_dag", "run-1", {"file": "points.csv"})


def _cause_status(error: AirflowError) -> int:
    """Status code of the Airflow response that caused the error."""
    cause = error.__cause__
    assert isinstance(cause, httpx.HTTPStatusError)
    return cause.response.status_code


@pytest.mark.asyncio
async def test_trigger_dag_run_request(airflow_client: AirflowClient, fake_airflow: FakeAirflow):
    await airflow_client.trigger_dag_run("my_dag", "run-1", {"file": "points.csv"})

    (request,) = fake_airflow.api_requests
    assert request.method == "POST"
    assert str(request.url) == f"{HOST}/api/v2/dags/my_dag/dagRuns"
    assert request.headers["Authorization"] == "Bearer jwt-1"
    # logical_date must be sent explicitly (null) for Airflow 3
    assert json.loads(request.content) == {
        "dag_run_id": "run-1",
        "conf": {"file": "points.csv"},
        "logical_date": None,
    }


@pytest.mark.asyncio
async def test_token_is_exchanged_for_k8s_token(
    airflow_client: AirflowClient, fake_airflow: FakeAirflow
):
    await _trigger(airflow_client)()

    (request,) = fake_airflow.token_requests
    assert request.method == "POST"
    assert request.headers["Authorization"] == f"Bearer {K8S_TOKEN}"


@pytest.mark.usefixtures("clock")
@pytest.mark.asyncio
async def test_token_is_cached(airflow_client: AirflowClient, fake_airflow: FakeAirflow):
    await _trigger(airflow_client)()
    await _trigger(airflow_client)()

    assert len(fake_airflow.token_requests) == 1
    assert [r.headers["Authorization"] for r in fake_airflow.api_requests] == ["Bearer jwt-1"] * 2


@pytest.mark.asyncio
async def test_token_is_refreshed_before_expiry(
    airflow_client: AirflowClient, fake_airflow: FakeAirflow, clock: Clock
):
    await _trigger(airflow_client)()
    clock.now += TOKEN_TTL - 61  # still valid (refreshed 60s before the ttl)
    await _trigger(airflow_client)()
    clock.now += 1
    await _trigger(airflow_client)()

    assert len(fake_airflow.token_requests) == 2
    assert [r.headers["Authorization"] for r in fake_airflow.api_requests] == [
        "Bearer jwt-1",
        "Bearer jwt-1",
        "Bearer jwt-2",
    ]


@pytest.mark.usefixtures("clock")
@pytest.mark.asyncio
async def test_unauthorized_retries_once_with_new_token(
    airflow_client: AirflowClient, fake_airflow: FakeAirflow
):
    fake_airflow.api_statuses = [401]

    await _trigger(airflow_client)()

    assert len(fake_airflow.token_requests) == 2
    assert [r.headers["Authorization"] for r in fake_airflow.api_requests] == [
        "Bearer jwt-1",
        "Bearer jwt-2",
    ]


@pytest.mark.usefixtures("clock")
@pytest.mark.asyncio
async def test_unauthorized_twice_raises(airflow_client: AirflowClient, fake_airflow: FakeAirflow):
    fake_airflow.api_statuses = [401, 401]

    with pytest.raises(AirflowError) as error:
        await _trigger(airflow_client)()

    assert _cause_status(error.value) == 401
    assert len(fake_airflow.api_requests) == 2  # no endless retrying


@pytest.mark.usefixtures("clock")
@pytest.mark.asyncio
async def test_error_response_raises_without_retry(
    airflow_client: AirflowClient, fake_airflow: FakeAirflow
):
    fake_airflow.api_statuses = [500]

    with pytest.raises(AirflowError) as error:
        await _trigger(airflow_client)()

    assert _cause_status(error.value) == 500
    assert len(fake_airflow.api_requests) == 1
    assert len(fake_airflow.token_requests) == 1


@pytest.mark.asyncio
async def test_token_request_failure_raises():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/auth/token"
        return httpx.Response(403, json={"detail": "Forbidden"})

    client = AirflowClient(HOST, TOKEN_TTL, 10, transport=httpx.MockTransport(handler))
    try:
        with pytest.raises(AirflowError) as error:
            await _trigger(client)()
    finally:
        await client.aclose()

<<<<<<< HEAD
    assert _cause_status(error.value) == 403


@pytest.mark.parametrize(
    "content",
    [
        pytest.param(b"{}", id="missing access_token"),
        pytest.param(b"<html>proxy error</html>", id="not json"),
        pytest.param(b"[]", id="json list"),
    ],
)
@pytest.mark.asyncio
async def test_unexpected_token_response_raises(content: bytes):
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/auth/token"
        return httpx.Response(201, content=content)

    client = AirflowClient(HOST, TOKEN_TTL, 10, transport=httpx.MockTransport(handler))
    try:
        with pytest.raises(AirflowError, match="Unexpected token response"):
            await client.get_dag_run("my_dag", "run-1")
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_connection_error_raises_airflow_error():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    client = AirflowClient(HOST, TOKEN_TTL, 10, transport=httpx.MockTransport(handler))
    try:
        with pytest.raises(AirflowError) as error:
            await _trigger(client)()
    finally:
        await client.aclose()

    assert isinstance(error.value.__cause__, httpx.ConnectError)


def _dag_run_client(status: int, body: Any) -> tuple[AirflowClient, list[httpx.Request]]:
    """Client answering Get Dag Run with the given status and (JSON or raw) body."""
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/auth/token":
            return httpx.Response(201, json={"access_token": "jwt"})
        requests.append(request)
        if isinstance(body, bytes):
            return httpx.Response(status, content=body)
        return httpx.Response(status, json=body)

    return AirflowClient(HOST, TOKEN_TTL, 10, transport=httpx.MockTransport(handler)), requests


@pytest.mark.asyncio
async def test_get_dag_run():
    client, requests = _dag_run_client(
        200,
        {
            "dag_run_id": "run-1",
            "start_date": "2026-01-01T10:00:05Z",
            "end_date": "2026-01-01T10:01:00Z",
            "state": "success",
            "conf": {},  # unknown fields are ignored
        },
    )
    try:
        dag_run = await client.get_dag_run("my_dag", "run-1")
    finally:
        await client.aclose()

    (request,) = requests
    assert request.method == "GET"
    assert str(request.url) == f"{HOST}/api/v2/dags/my_dag/dagRuns/run-1"
    assert request.headers["Authorization"] == "Bearer jwt"
    assert dag_run == AirflowDagRunResponse(
        start_date=datetime(2026, 1, 1, 10, 0, 5, tzinfo=UTC),
        end_date=datetime(2026, 1, 1, 10, 1, 0, tzinfo=UTC),
        state="success",
    )


@pytest.mark.asyncio
async def test_get_dag_run_not_started():
    # Airflow returns null dates for runs that are queued (start_date) or running (end_date)
    client, _ = _dag_run_client(
        200,
        {
            "start_date": None,
            "end_date": None,
            "state": "queued",
        },
    )
    try:
        dag_run = await client.get_dag_run("my_dag", "run-1")
    finally:
        await client.aclose()

    assert dag_run.state == "queued"
    assert dag_run.start_date is None
    assert dag_run.end_date is None


@pytest.mark.parametrize(
    "body",
    [
        pytest.param({}, id="missing state"),
        pytest.param({"state": "running", "start_date": "not a date"}, id="invalid date"),
        pytest.param(b"<html>proxy error</html>", id="not json"),
        pytest.param([], id="json list"),
        pytest.param(None, id="json null"),
    ],
)
@pytest.mark.asyncio
async def test_get_dag_run_unexpected_response_raises(body: Any):
    client, _ = _dag_run_client(200, body)
    try:
        with pytest.raises(AirflowError, match="Unexpected dag run response"):
            await client.get_dag_run("my_dag", "run-1")
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_get_dag_run_not_found_raises():
    client, _ = _dag_run_client(404, {"detail": "DagRun not found"})
    try:
        with pytest.raises(AirflowError) as error:
            await client.get_dag_run("my_dag", "run-1")
    finally:
        await client.aclose()

    assert _cause_status(error.value) == 404
=======
    assert error.value.response.status_code == 403
>>>>>>> 9e5f173 (GPS-1011: Local airflow)


@pytest.mark.asyncio
async def test_password_token_request():
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/auth/token":
            return httpx.Response(201, json={"access_token": "local-jwt"})
        return httpx.Response(200, json={})

    client = AirflowClient(
        HOST,
        TOKEN_TTL,
        10,
        token_request=password_token_request("service", "secret"),
        transport=httpx.MockTransport(handler),
    )
    try:
        await _trigger(client)()
    finally:
        await client.aclose()

    token_request, api_request = requests
    assert json.loads(token_request.content) == {"username": "service", "password": "secret"}
    assert "Authorization" not in token_request.headers
    assert api_request.headers["Authorization"] == "Bearer local-jwt"


def _settings(**overrides: Any) -> Settings:
    return Settings(_env_file=None, **overrides)


@pytest.mark.asyncio
async def test_initialize_uses_k8s_token_by_default():
    app = FastAPI()

    initialize_airflow_client(app, _settings())

    assert app.state.airflow_client._token_request is k8s_token_request  # noqa: SLF001
    await app.state.airflow_client.aclose()


@pytest.mark.asyncio
async def test_initialize_local_airflow_uses_password_login():
    app = FastAPI()

    initialize_airflow_client(
        app,
        _settings(
            use_local_airflow=True,
            airflow_local_username="service",
            airflow_local_password="pw",  # noqa: S106
        ),
    )

    assert app.state.airflow_client._token_request() == {  # noqa: SLF001
        "json": {"username": "service", "password": "pw"}
    }
    await app.state.airflow_client.aclose()


def test_initialize_local_airflow_requires_credentials():
    with pytest.raises(RuntimeError, match="AIRFLOW_LOCAL_USERNAME"):
        initialize_airflow_client(FastAPI(), _settings(use_local_airflow=True))
