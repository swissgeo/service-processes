import uuid
from collections.abc import Generator
from unittest.mock import AsyncMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

import pytest

from app.core.authorization import (
    Action,
    AuthorizationError,
    VerifiedPermissionsAuthorizer,
    dataset_resource,
    get_authorizer,
    process_resource,
)
from app.core.process_service import ProcessService, get_process_service
from app.models.process import Process
from app.schemas.pagination import DEFAULT_LIMIT, MAX_LIMIT
from app.schemas.process import MAX_DATASETS

TOKEN = "header.payload.signature"  # noqa: S105
AUTH_HEADERS = {"X-Auth-Request-Access-Token": TOKEN}

DATASET = uuid.UUID("0195f3a2-7c1e-7b3a-9d4e-000000000001")

PROCESS = Process(
    sgid=uuid.UUID("0195f3a2-7c1e-7b3a-9d4e-1f2a3b4c5d6e"),
    organization_sgid=uuid.uuid7(),
    organization_unit_sgid=uuid.uuid7(),
    dataset_sgid=DATASET,
    name="Dummy export",
    description="Dummy process",
    dag_id="dummy_export_dag",
    user_parameter_schema={"type": "object", "properties": {"startDate": {"type": "string"}}},
    internal_config={"output_format": "geojson"},
    parameter_mapping={"startDate": "options.date_range.start"},
)


@pytest.fixture
def process_service(app: FastAPI) -> Generator[AsyncMock]:
    """Replace the process service by a mock, so that the tests don't require a database."""
    service = AsyncMock(spec=ProcessService)
    # Every requested dataset exists and belongs to the unit and organization of PROCESS
    service.get_datasets.side_effect = lambda dataset_sgids: [
        (dataset_sgid, PROCESS.organization_unit_sgid, PROCESS.organization_sgid)
        for dataset_sgid in dataset_sgids
    ]
    service.list_processes.return_value = ([PROCESS], 1)
    app.dependency_overrides[get_process_service] = lambda: service
    yield service
    app.dependency_overrides.pop(get_process_service)


@pytest.fixture
def authorizer(app: FastAPI) -> Generator[AsyncMock]:
    """Replace the authorizer by a mock allowing everything, so that the tests don't call AWS."""
    authorizer = AsyncMock(spec=VerifiedPermissionsAuthorizer)
    authorizer.is_authorized.return_value = True
    authorizer.batch_is_authorized.side_effect = lambda _token, _action, resources: resources
    app.dependency_overrides[get_authorizer] = lambda: authorizer
    yield authorizer
    app.dependency_overrides.pop(get_authorizer)


@pytest.mark.usefixtures("authorizer")
def test_list_processes(client: TestClient, process_service: AsyncMock):
    response = client.get(f"/processes?datasets={DATASET}", headers=AUTH_HEADERS)

    assert response.status_code == 200
    assert response.json() == {
        "items": [
            {
                "sgid": "0195f3a2-7c1e-7b3a-9d4e-1f2a3b4c5d6e",
                "name": "Dummy export",
                "description": "Dummy process",
                "inputs": {"type": "object", "properties": {"startDate": {"type": "string"}}},
            }
        ],
        "total": 1,
        "limit": DEFAULT_LIMIT,
        "offset": 0,
    }
    process_service.list_processes.assert_awaited_once_with([DATASET], DEFAULT_LIMIT, 0)


@pytest.mark.usefixtures("process_service", "authorizer")
def test_list_processes_does_not_expose_internal_fields(client: TestClient):
    item = client.get(f"/processes?datasets={DATASET}", headers=AUTH_HEADERS).json()["items"][0]

    assert "dag_id" not in item
    assert "internal_config" not in item
    assert "parameter_mapping" not in item


@pytest.mark.usefixtures("authorizer")
def test_list_processes_empty(client: TestClient, process_service: AsyncMock):
    process_service.list_processes.return_value = ([], 0)

    response = client.get(f"/processes?datasets={DATASET}", headers=AUTH_HEADERS)

    assert response.status_code == 200
    assert response.json() == {"items": [], "total": 0, "limit": DEFAULT_LIMIT, "offset": 0}


@pytest.mark.parametrize(
    ("query", "expected_limit", "expected_offset"),
    [
        ("&limit=20&offset=40", 20, 40),
        ("&limit=1000", MAX_LIMIT, 0),
        ("&limit=0&offset=-1", 1, 0),
        ("&limit=abc&offset=xyz", DEFAULT_LIMIT, 0),
    ],
)
@pytest.mark.usefixtures("authorizer")
def test_list_processes_pagination(
    client: TestClient,
    process_service: AsyncMock,
    query: str,
    expected_limit: int,
    expected_offset: int,
):
    response = client.get(f"/processes?datasets={DATASET}{query}", headers=AUTH_HEADERS)

    assert response.status_code == 200
    assert response.json()["limit"] == expected_limit
    assert response.json()["offset"] == expected_offset
    process_service.list_processes.assert_awaited_once_with(
        [DATASET], expected_limit, expected_offset
    )


@pytest.mark.parametrize(
    "query",
    [
        "?datasets={a},{b}",
        "?datasets={a}&datasets={b}",
        "?datasets= {a} , {b} ,",
    ],
)
@pytest.mark.usefixtures("authorizer")
def test_list_processes_multiple_datasets(
    client: TestClient, process_service: AsyncMock, query: str
):
    a, b = uuid.uuid7(), uuid.uuid7()

    response = client.get("/processes" + query.format(a=a, b=b), headers=AUTH_HEADERS)

    assert response.status_code == 200
    process_service.get_datasets.assert_awaited_once_with([a, b])
    process_service.list_processes.assert_awaited_once_with([a, b], DEFAULT_LIMIT, 0)


@pytest.mark.parametrize(
    "query",
    [
        "",
        "?datasets=",
        "?datasets=,",
        "?datasets=not-a-uuid",
        "?datasets=" + ",".join([str(DATASET)] * (MAX_DATASETS + 1)),
    ],
)
@pytest.mark.usefixtures("authorizer")
def test_list_processes_invalid_datasets(
    client: TestClient, process_service: AsyncMock, query: str
):
    response = client.get(f"/processes{query}", headers=AUTH_HEADERS)

    assert response.status_code == 400
    process_service.list_processes.assert_not_awaited()


def test_list_processes_only_of_allowed_datasets(
    client: TestClient, process_service: AsyncMock, authorizer: AsyncMock
):
    allowed, denied = uuid.uuid7(), uuid.uuid7()
    authorizer.batch_is_authorized.side_effect = lambda _token, _action, resources: [
        resource for resource in resources if resource.id == allowed
    ]

    response = client.get(f"/processes?datasets={allowed},{denied}", headers=AUTH_HEADERS)

    assert response.status_code == 200
    unit, organization = PROCESS.organization_unit_sgid, PROCESS.organization_sgid
    authorizer.batch_is_authorized.assert_awaited_once_with(
        TOKEN,
        Action.LIST_PROCESSES,
        [
            dataset_resource(allowed, unit, organization),
            dataset_resource(denied, unit, organization),
        ],
    )
    process_service.list_processes.assert_awaited_once_with([allowed], DEFAULT_LIMIT, 0)


def test_list_processes_all_datasets_denied(
    client: TestClient, process_service: AsyncMock, authorizer: AsyncMock
):
    process_service.list_processes.return_value = ([], 0)
    authorizer.batch_is_authorized.side_effect = None
    authorizer.batch_is_authorized.return_value = []

    response = client.get(f"/processes?datasets={DATASET}", headers=AUTH_HEADERS)

    assert response.status_code == 200
    assert response.json() == {"items": [], "total": 0, "limit": DEFAULT_LIMIT, "offset": 0}
    process_service.list_processes.assert_awaited_once_with([], DEFAULT_LIMIT, 0)


def test_list_processes_unknown_datasets_are_not_authorized(
    client: TestClient, process_service: AsyncMock, authorizer: AsyncMock
):
    # Datasets without processes are not returned by get_datasets
    process_service.get_datasets.side_effect = None
    process_service.get_datasets.return_value = []
    process_service.list_processes.return_value = ([], 0)

    response = client.get(f"/processes?datasets={DATASET}", headers=AUTH_HEADERS)

    assert response.status_code == 200
    authorizer.batch_is_authorized.assert_awaited_once_with(TOKEN, Action.LIST_PROCESSES, [])
    process_service.list_processes.assert_awaited_once_with([], DEFAULT_LIMIT, 0)


def test_list_processes_authorization_unavailable(
    client: TestClient, process_service: AsyncMock, authorizer: AsyncMock
):
    authorizer.batch_is_authorized.side_effect = AuthorizationError("unavailable")

    response = client.get(f"/processes?datasets={DATASET}", headers=AUTH_HEADERS)

    assert response.status_code == 503
    assert response.json() == {
        "error": "Service Unavailable",
        "message": "Service Unavailable",
    }
    process_service.list_processes.assert_not_awaited()


@pytest.mark.parametrize("headers", [{}, {"X-Auth-Request-Access-Token": ""}])
def test_list_processes_missing_token(
    client: TestClient, process_service: AsyncMock, authorizer: AsyncMock, headers: dict
):
    response = client.get(f"/processes?datasets={DATASET}", headers=headers)

    assert response.status_code == 401
    assert response.json() == {"error": "Unauthorized", "message": "Missing access token"}
    authorizer.batch_is_authorized.assert_not_awaited()
    process_service.list_processes.assert_not_awaited()


def test_get_process(client: TestClient, process_service: AsyncMock, authorizer: AsyncMock):
    process_service.get_process.return_value = PROCESS

    response = client.get(f"/processes/{PROCESS.sgid}", headers=AUTH_HEADERS)

    assert response.status_code == 200
    assert response.json() == {
        "sgid": "0195f3a2-7c1e-7b3a-9d4e-1f2a3b4c5d6e",
        "name": "Dummy export",
        "description": "Dummy process",
        "inputs": {"type": "object", "properties": {"startDate": {"type": "string"}}},
    }
    process_service.get_process.assert_awaited_once_with(PROCESS.sgid)
    authorizer.is_authorized.assert_awaited_once_with(
        TOKEN, Action.GET_PROCESS, process_resource(PROCESS)
    )


def test_get_process_forbidden(
    client: TestClient, process_service: AsyncMock, authorizer: AsyncMock
):
    process_service.get_process.return_value = PROCESS
    authorizer.is_authorized.return_value = False

    response = client.get(f"/processes/{PROCESS.sgid}", headers=AUTH_HEADERS)

    assert response.status_code == 403
    assert response.json() == {"error": "Forbidden", "message": "Forbidden"}


def test_get_process_authorization_unavailable(
    client: TestClient, process_service: AsyncMock, authorizer: AsyncMock
):
    process_service.get_process.return_value = PROCESS
    authorizer.is_authorized.side_effect = AuthorizationError("unavailable")

    response = client.get(f"/processes/{PROCESS.sgid}", headers=AUTH_HEADERS)

    assert response.status_code == 503


@pytest.mark.parametrize("headers", [{}, {"X-Auth-Request-Access-Token": ""}])
def test_get_process_missing_token(
    client: TestClient, process_service: AsyncMock, authorizer: AsyncMock, headers: dict
):
    process_service.get_process.return_value = PROCESS

    response = client.get(f"/processes/{PROCESS.sgid}", headers=headers)

    assert response.status_code == 401
    assert response.json() == {"error": "Unauthorized", "message": "Missing access token"}
    authorizer.is_authorized.assert_not_awaited()


def test_get_process_not_found(
    client: TestClient, process_service: AsyncMock, authorizer: AsyncMock
):
    process_service.get_process.return_value = None

    response = client.get(f"/processes/{uuid.uuid7()}", headers=AUTH_HEADERS)

    assert response.status_code == 404
    assert response.json() == {"error": "Not Found", "message": "Process not found"}
    authorizer.is_authorized.assert_not_awaited()


@pytest.mark.usefixtures("authorizer")
def test_get_process_invalid_sgid(client: TestClient, process_service: AsyncMock):
    response = client.get("/processes/not-a-uuid", headers=AUTH_HEADERS)

    assert response.status_code == 400
    process_service.get_process.assert_not_awaited()
