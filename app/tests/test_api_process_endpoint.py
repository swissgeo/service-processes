import uuid
from collections.abc import Generator
from unittest.mock import AsyncMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

import pytest

from app.core.process_service import ProcessService, get_process_service
from app.models.process import Process
from app.schemas.pagination import DEFAULT_LIMIT, MAX_LIMIT
from app.schemas.process import MAX_DATASETS

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
    service.list_processes.return_value = ([PROCESS], 1)
    app.dependency_overrides[get_process_service] = lambda: service
    yield service
    app.dependency_overrides.pop(get_process_service)


def test_list_processes(client: TestClient, process_service: AsyncMock):
    response = client.get(f"/processes?datasets={DATASET}")

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


@pytest.mark.usefixtures("process_service")
def test_list_processes_does_not_expose_internal_fields(client: TestClient):
    item = client.get(f"/processes?datasets={DATASET}").json()["items"][0]

    assert "dag_id" not in item
    assert "internal_config" not in item
    assert "parameter_mapping" not in item


def test_list_processes_empty(client: TestClient, process_service: AsyncMock):
    process_service.list_processes.return_value = ([], 0)

    response = client.get(f"/processes?datasets={DATASET}")

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
def test_list_processes_pagination(
    client: TestClient,
    process_service: AsyncMock,
    query: str,
    expected_limit: int,
    expected_offset: int,
):
    response = client.get(f"/processes?datasets={DATASET}{query}")

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
def test_list_processes_multiple_datasets(
    client: TestClient, process_service: AsyncMock, query: str
):
    a, b = uuid.uuid7(), uuid.uuid7()

    response = client.get("/processes" + query.format(a=a, b=b))

    assert response.status_code == 200
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
def test_list_processes_invalid_datasets(
    client: TestClient, process_service: AsyncMock, query: str
):
    response = client.get(f"/processes{query}")

    assert response.status_code == 400
    process_service.list_processes.assert_not_awaited()


def test_get_process(client: TestClient, process_service: AsyncMock):
    process_service.get_process.return_value = PROCESS

    response = client.get(f"/processes/{PROCESS.sgid}")

    assert response.status_code == 200
    assert response.json() == {
        "sgid": "0195f3a2-7c1e-7b3a-9d4e-1f2a3b4c5d6e",
        "name": "Dummy export",
        "description": "Dummy process",
        "inputs": {"type": "object", "properties": {"startDate": {"type": "string"}}},
    }
    process_service.get_process.assert_awaited_once_with(PROCESS.sgid)


def test_get_process_not_found(client: TestClient, process_service: AsyncMock):
    process_service.get_process.return_value = None

    response = client.get(f"/processes/{uuid.uuid7()}")

    assert response.status_code == 404
    assert response.json() == {"error": "Not Found", "message": "Process not found"}


def test_get_process_invalid_sgid(client: TestClient, process_service: AsyncMock):
    response = client.get("/processes/not-a-uuid")

    assert response.status_code == 400
    process_service.get_process.assert_not_awaited()
