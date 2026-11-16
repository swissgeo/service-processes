import uuid
from collections.abc import Generator
from datetime import UTC, datetime, timedelta
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
from app.core.process_service import (
    IdempotencyKeyReusedError,
    InvalidInputsError,
    ProcessService,
    get_process_service,
)
from app.models.process import SGID_MAX_LENGTH, Job, JobStatus, Process
from app.schemas.pagination import DEFAULT_LIMIT, MAX_LIMIT
from app.schemas.process import IDEMPOTENCY_KEY_MAX_LENGTH, MAX_FILTER_VALUES

TOKEN = "header.payload.signature"  # noqa: S105
AUTH_HEADERS = {"X-Auth-Request-Access-Token": TOKEN}

DATASET = "0195f3a2-7c1e-7b3a-9d4e-000000000001"
CREATED_AT = datetime(2026, 1, 1, 10, 0, 0, tzinfo=UTC)

# Published schema of PROCESS: unknown inputs are always forbidden, see Process.input_schema
PUBLISHED_INPUTS = {
    "type": "object",
    "properties": {"startDate": {"type": "string"}},
    "additionalProperties": False,
}

PROCESS = Process(
    id=7,
    sgid="0195f3a2-7c1e-7b3a-9d4e-1f2a3b4c5d6e",
    organization_sgid=str(uuid.uuid7()),
    organization_unit_sgid=str(uuid.uuid7()),
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


class TestListProcesses:
    @pytest.mark.usefixtures("authorizer")
    def test_list_processes(self, client: TestClient, process_service: AsyncMock):
        response = client.get(f"/processes?datasets={DATASET}", headers=AUTH_HEADERS)

        assert response.status_code == 200
        assert response.json() == {
            "items": [
                {
                    "sgid": "0195f3a2-7c1e-7b3a-9d4e-1f2a3b4c5d6e",
                    "name": "Dummy export",
                    "description": "Dummy process",
                    "inputs": PUBLISHED_INPUTS,
                }
            ],
            "total": 1,
            "limit": DEFAULT_LIMIT,
            "offset": 0,
        }
        process_service.list_processes.assert_awaited_once_with([DATASET], DEFAULT_LIMIT, 0)

    @pytest.mark.usefixtures("process_service", "authorizer")
    def test_list_processes_does_not_expose_internal_fields(self, client: TestClient):
        item = client.get(f"/processes?datasets={DATASET}", headers=AUTH_HEADERS).json()["items"][0]

        assert "dag_id" not in item
        assert "internal_config" not in item
        assert "parameter_mapping" not in item

    @pytest.mark.usefixtures("authorizer")
    def test_list_processes_empty(self, client: TestClient, process_service: AsyncMock):
        process_service.list_processes.return_value = ([], 0)

        response = client.get(f"/processes?datasets={DATASET}", headers=AUTH_HEADERS)

        assert response.status_code == 200
        assert response.json() == {"items": [], "total": 0, "limit": DEFAULT_LIMIT, "offset": 0}

    @pytest.mark.parametrize(
        ("query", "expected_limit", "expected_offset"),
        [
            pytest.param("&limit=20&offset=40", 20, 40, id="valid"),
            pytest.param("&limit=1000", MAX_LIMIT, 0, id="limit above max"),
            pytest.param("&limit=0&offset=-1", 1, 0, id="limit and offset below min"),
            pytest.param("&limit=abc&offset=xyz", DEFAULT_LIMIT, 0, id="not numeric"),
        ],
    )
    @pytest.mark.usefixtures("authorizer")
    def test_list_processes_pagination(
        self,
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
            pytest.param("?datasets={a},{b}", id="comma separated"),
            pytest.param("?datasets={a}&datasets={b}", id="repeated parameter"),
            pytest.param("?datasets= {a} , {b} ,", id="whitespace and trailing comma"),
        ],
    )
    @pytest.mark.usefixtures("authorizer")
    def test_list_processes_multiple_datasets(
        self, client: TestClient, process_service: AsyncMock, query: str
    ):
        a, b = str(uuid.uuid7()), "dataset_b-2"

        response = client.get("/processes" + query.format(a=a, b=b), headers=AUTH_HEADERS)

        assert response.status_code == 200
        process_service.get_datasets.assert_awaited_once_with([a, b])
        process_service.list_processes.assert_awaited_once_with([a, b], DEFAULT_LIMIT, 0)

    @pytest.mark.parametrize(
        "query",
        [
            pytest.param("", id="missing datasets"),
            pytest.param("?datasets=", id="empty datasets"),
            pytest.param("?datasets=,", id="only separator"),
            pytest.param(
                "?datasets=" + ",".join([str(DATASET)] * (MAX_FILTER_VALUES + 1)),
                id="too many datasets",
            ),
            pytest.param("?datasets=" + "a" * (SGID_MAX_LENGTH + 1), id="dataset id too long"),
            pytest.param(
                f"?datasets={DATASET}," + "a" * (SGID_MAX_LENGTH + 1),
                id="mix valid and too long dataset id",
            ),
        ],
    )
    @pytest.mark.usefixtures("authorizer")
    def test_list_processes_invalid_datasets(
        self, client: TestClient, process_service: AsyncMock, query: str
    ):
        response = client.get(f"/processes{query}", headers=AUTH_HEADERS)

        assert response.status_code == 400
        process_service.list_processes.assert_not_awaited()

    @pytest.mark.usefixtures("authorizer")
    def test_list_processes_accepts_non_uuid_dataset_of_max_length(
        self, client: TestClient, process_service: AsyncMock
    ):
        dataset = "a" * SGID_MAX_LENGTH

        response = client.get(f"/processes?datasets={dataset}", headers=AUTH_HEADERS)

        assert response.status_code == 200
        process_service.list_processes.assert_awaited_once_with([dataset], DEFAULT_LIMIT, 0)

    def test_list_processes_only_of_allowed_datasets(
        self, client: TestClient, process_service: AsyncMock, authorizer: AsyncMock
    ):
        allowed, denied = str(uuid.uuid7()), str(uuid.uuid7())
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
        self, client: TestClient, process_service: AsyncMock, authorizer: AsyncMock
    ):
        process_service.list_processes.return_value = ([], 0)
        authorizer.batch_is_authorized.side_effect = None
        authorizer.batch_is_authorized.return_value = []

        response = client.get(f"/processes?datasets={DATASET}", headers=AUTH_HEADERS)

        assert response.status_code == 200
        assert response.json() == {"items": [], "total": 0, "limit": DEFAULT_LIMIT, "offset": 0}
        process_service.list_processes.assert_awaited_once_with([], DEFAULT_LIMIT, 0)

    def test_list_processes_unknown_datasets_are_not_authorized(
        self, client: TestClient, process_service: AsyncMock, authorizer: AsyncMock
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
        self, client: TestClient, process_service: AsyncMock, authorizer: AsyncMock
    ):
        authorizer.batch_is_authorized.side_effect = AuthorizationError("unavailable")

        response = client.get(f"/processes?datasets={DATASET}", headers=AUTH_HEADERS)

        assert response.status_code == 503
        assert response.json() == {
            "error": "Service Unavailable",
            "message": "Service Unavailable",
        }
        process_service.list_processes.assert_not_awaited()

    @pytest.mark.parametrize(
        "headers",
        [
            pytest.param({}, id="no header"),
            pytest.param({"X-Auth-Request-Access-Token": ""}, id="empty token"),
        ],
    )
    def test_list_processes_missing_token(
        self, client: TestClient, process_service: AsyncMock, authorizer: AsyncMock, headers: dict
    ):
        response = client.get(f"/processes?datasets={DATASET}", headers=headers)

        assert response.status_code == 401
        assert response.json() == {"error": "Unauthorized", "message": "Missing access token"}
        authorizer.batch_is_authorized.assert_not_awaited()
        process_service.list_processes.assert_not_awaited()


class TestGetProcess:
    def test_get_process(
        self, client: TestClient, process_service: AsyncMock, authorizer: AsyncMock
    ):
        process_service.get_process.return_value = PROCESS

        response = client.get(f"/processes/{PROCESS.sgid}", headers=AUTH_HEADERS)

        assert response.status_code == 200
        assert response.json() == {
            "sgid": "0195f3a2-7c1e-7b3a-9d4e-1f2a3b4c5d6e",
            "name": "Dummy export",
            "description": "Dummy process",
            "inputs": PUBLISHED_INPUTS,
        }
        process_service.get_process.assert_awaited_once_with(PROCESS.sgid)
        authorizer.is_authorized.assert_awaited_once_with(
            TOKEN, Action.GET_PROCESS, process_resource(PROCESS)
        )

    def test_get_process_forbidden(
        self, client: TestClient, process_service: AsyncMock, authorizer: AsyncMock
    ):
        process_service.get_process.return_value = PROCESS
        authorizer.is_authorized.return_value = False

        response = client.get(f"/processes/{PROCESS.sgid}", headers=AUTH_HEADERS)

        assert response.status_code == 403
        assert response.json() == {"error": "Forbidden", "message": "Forbidden"}

    def test_get_process_authorization_unavailable(
        self, client: TestClient, process_service: AsyncMock, authorizer: AsyncMock
    ):
        process_service.get_process.return_value = PROCESS
        authorizer.is_authorized.side_effect = AuthorizationError("unavailable")

        response = client.get(f"/processes/{PROCESS.sgid}", headers=AUTH_HEADERS)

        assert response.status_code == 503

    @pytest.mark.parametrize(
        "headers",
        [
            pytest.param({}, id="no header"),
            pytest.param({"X-Auth-Request-Access-Token": ""}, id="empty token"),
        ],
    )
    def test_get_process_missing_token(
        self, client: TestClient, process_service: AsyncMock, authorizer: AsyncMock, headers: dict
    ):
        process_service.get_process.return_value = PROCESS

        response = client.get(f"/processes/{PROCESS.sgid}", headers=headers)

        assert response.status_code == 401
        assert response.json() == {"error": "Unauthorized", "message": "Missing access token"}
        authorizer.is_authorized.assert_not_awaited()

    @pytest.mark.usefixtures("authorizer")
    def test_get_process_with_non_uuid_sgid(self, client: TestClient, process_service: AsyncMock):
        sgid = ("my.process_ID-1:" + "x" * SGID_MAX_LENGTH)[:SGID_MAX_LENGTH]
        assert len(sgid) == SGID_MAX_LENGTH
        process_service.get_process.return_value = Process(
            sgid=sgid,
            organization_sgid="org",
            organization_unit_sgid="unit",
            dataset_sgid="dataset",
            name="Dummy export",
            description="Dummy process",
            user_parameter_schema={},
        )

        response = client.get(f"/processes/{sgid}", headers=AUTH_HEADERS)

        assert response.status_code == 200
        assert response.json()["sgid"] == sgid
        process_service.get_process.assert_awaited_once_with(sgid)

    @pytest.mark.usefixtures("authorizer")
    def test_get_process_sgid_too_long(self, client: TestClient, process_service: AsyncMock):
        response = client.get(f"/processes/{'a' * (SGID_MAX_LENGTH + 1)}", headers=AUTH_HEADERS)

        assert response.status_code == 400
        process_service.get_process.assert_not_awaited()

    def test_get_process_not_found(
        self, client: TestClient, process_service: AsyncMock, authorizer: AsyncMock
    ):
        process_service.get_process.return_value = None

        response = client.get(f"/processes/{uuid.uuid7()}", headers=AUTH_HEADERS)

        assert response.status_code == 404
        assert response.json() == {"error": "Not Found", "message": "Process not found"}
        authorizer.is_authorized.assert_not_awaited()


IDEMPOTENCY_KEY = "0195f3a2-7c1e-7b3a-9d4e-00000000000a"
EXECUTION_HEADERS = {**AUTH_HEADERS, "Idempotency-Key": IDEMPOTENCY_KEY}
EXECUTED_JOB = Job(sgid="pjb_123", status="queued", created_at=CREATED_AT, process=PROCESS)
EXECUTED_JOB_JSON = {
    "sgid": "pjb_123",
    "process_sgid": PROCESS.sgid,
    "status": "queued",
    "created_at": "2026-01-01T10:00:00Z",
    "started_at": None,
    "finished_at": None,
}


class TestProcessExecution:
    def test_execute_process(
        self, client: TestClient, process_service: AsyncMock, authorizer: AsyncMock
    ):
        process_service.get_process.return_value = PROCESS
        process_service.execute_process.return_value = (EXECUTED_JOB, True)

        response = client.post(
            f"/processes/{PROCESS.sgid}/execution",
            json={"inputs": {"startDate": "2026-01-01"}},
            headers=EXECUTION_HEADERS,
        )

        assert response.status_code == 201
        assert response.json() == EXECUTED_JOB_JSON
        authorizer.is_authorized.assert_awaited_once_with(
            TOKEN, Action.EXECUTE_PROCESS, process_resource(PROCESS)
        )
        process_service.execute_process.assert_awaited_once_with(
            PROCESS, {"startDate": "2026-01-01"}, IDEMPOTENCY_KEY
        )

    @pytest.mark.usefixtures("authorizer")
    def test_execute_process_returns_existing_job_for_same_key(
        self, client: TestClient, process_service: AsyncMock
    ):
        process_service.get_process.return_value = PROCESS
        process_service.execute_process.return_value = (EXECUTED_JOB, False)

        response = client.post(
            f"/processes/{PROCESS.sgid}/execution",
            json={"inputs": {"startDate": "2026-01-01"}},
            headers=EXECUTION_HEADERS,
        )

        assert response.status_code == 200
        assert response.json() == EXECUTED_JOB_JSON

    @pytest.mark.usefixtures("authorizer")
    def test_execute_process_same_key_with_other_inputs(
        self, client: TestClient, process_service: AsyncMock
    ):
        process_service.get_process.return_value = PROCESS
        process_service.execute_process.side_effect = IdempotencyKeyReusedError

        response = client.post(
            f"/processes/{PROCESS.sgid}/execution",
            json={"inputs": {"startDate": "2026-01-01"}},
            headers=EXECUTION_HEADERS,
        )

        assert response.status_code == 409
        assert response.json() == {
            "error": "Conflict",
            "message": "Idempotency-Key was already used with a different request body",
        }

    @pytest.mark.parametrize(
        "headers",
        [
            pytest.param(AUTH_HEADERS, id="missing"),
            pytest.param({**AUTH_HEADERS, "Idempotency-Key": ""}, id="empty"),
            pytest.param(
                {**AUTH_HEADERS, "Idempotency-Key": "a" * (IDEMPOTENCY_KEY_MAX_LENGTH + 1)},
                id="too-long",
            ),
        ],
    )
    def test_execute_process_invalid_idempotency_key(
        self, client: TestClient, process_service: AsyncMock, headers: dict[str, str]
    ):
        response = client.post(
            f"/processes/{PROCESS.sgid}/execution",
            json={"inputs": {"startDate": "2026-01-01"}},
            headers=headers,
        )

        assert response.status_code == 400
        assert response.json()["detail"][0]["loc"] == ["header", "idempotency-key"]
        process_service.execute_process.assert_not_awaited()

    @pytest.mark.usefixtures("authorizer")
    def test_execute_process_accepts_idempotency_key_of_max_length(
        self, client: TestClient, process_service: AsyncMock
    ):
        process_service.get_process.return_value = PROCESS
        process_service.execute_process.return_value = (EXECUTED_JOB, True)
        key = "a" * IDEMPOTENCY_KEY_MAX_LENGTH

        response = client.post(
            f"/processes/{PROCESS.sgid}/execution",
            json={"inputs": {"startDate": "2026-01-01"}},
            headers={**AUTH_HEADERS, "Idempotency-Key": key},
        )

        assert response.status_code == 201
        process_service.execute_process.assert_awaited_once_with(
            PROCESS, {"startDate": "2026-01-01"}, key
        )

    def test_execute_process_forbidden(
        self, client: TestClient, process_service: AsyncMock, authorizer: AsyncMock
    ):
        process_service.get_process.return_value = PROCESS
        authorizer.is_authorized.return_value = False

        response = client.post(
            f"/processes/{PROCESS.sgid}/execution", json={"inputs": {}}, headers=EXECUTION_HEADERS
        )

        assert response.status_code == 403
        process_service.execute_process.assert_not_awaited()

    @pytest.mark.usefixtures("authorizer")
    def test_execute_process_not_found(self, client: TestClient, process_service: AsyncMock):
        process_service.get_process.return_value = None

        response = client.post(
            f"/processes/{uuid.uuid7()}/execution", json={"inputs": {}}, headers=EXECUTION_HEADERS
        )

        assert response.status_code == 404
        assert response.json() == {"error": "Not Found", "message": "Process not found"}
        process_service.execute_process.assert_not_awaited()

    @pytest.mark.usefixtures("authorizer")
    def test_execute_process_invalid_inputs(self, client: TestClient, process_service: AsyncMock):
        process_service.get_process.return_value = PROCESS
        errors = [
            {
                "loc": ["body", "inputs", "startDate"],
                "msg": "1 is not of type 'string'",
                "type": "type",
            }
        ]
        process_service.execute_process.side_effect = InvalidInputsError(errors)

        response = client.post(
            f"/processes/{PROCESS.sgid}/execution",
            json={"inputs": {"startDate": 1}},
            headers=EXECUTION_HEADERS,
        )

        assert response.status_code == 400
        assert response.json() == {
            "error": "Validation Error",
            "message": "Invalid request payload",
            "detail": errors,
        }


class TestGetJobAPI:
    JOB = Job(sgid="pjb_123", dag_run_id="run-1", status="running", process=PROCESS)

    def test_get_job(self, client: TestClient, process_service: AsyncMock, authorizer: AsyncMock):
        process_service.get_job.return_value = self.JOB
        process_service.get_updated_job.return_value = Job(
            sgid="pjb_123",
            status="success",
            process=PROCESS,
            created_at=CREATED_AT,
            started_at=CREATED_AT + timedelta(seconds=5),
            finished_at=CREATED_AT + timedelta(minutes=1),
        )

        response = client.get(f"/jobs/{self.JOB.sgid}", headers=AUTH_HEADERS)

        assert response.status_code == 200
        assert response.json() == {
            "sgid": "pjb_123",
            "process_sgid": PROCESS.sgid,
            "status": "success",
            "created_at": "2026-01-01T10:00:00Z",
            "started_at": "2026-01-01T10:00:05Z",
            "finished_at": "2026-01-01T10:01:00Z",
        }
        process_service.get_job.assert_awaited_once_with(self.JOB.sgid)
        authorizer.is_authorized.assert_awaited_once_with(
            TOKEN, Action.GET_PROCESS, process_resource(PROCESS)
        )
        process_service.get_updated_job.assert_awaited_once_with(self.JOB)

    def test_get_job_forbidden(
        self, client: TestClient, process_service: AsyncMock, authorizer: AsyncMock
    ):
        process_service.get_job.return_value = self.JOB
        authorizer.is_authorized.return_value = False

        response = client.get(f"/jobs/{self.JOB.sgid}", headers=AUTH_HEADERS)

        assert response.status_code == 403
        # Unauthorized users must not trigger calls to Airflow
        process_service.get_updated_job.assert_not_awaited()

    def test_get_job_not_found(
        self, client: TestClient, process_service: AsyncMock, authorizer: AsyncMock
    ):
        process_service.get_job.return_value = None

        response = client.get(f"/jobs/{self.JOB.sgid}", headers=AUTH_HEADERS)

        assert response.status_code == 404
        assert response.json() == {"error": "Not Found", "message": "Job not found"}
        authorizer.is_authorized.assert_not_awaited()
        process_service.get_updated_job.assert_not_awaited()

    @pytest.mark.usefixtures("authorizer")
    def test_get_job_sgid_too_long(self, client: TestClient, process_service: AsyncMock):
        response = client.get(f"/jobs/{'a' * (SGID_MAX_LENGTH + 1)}", headers=AUTH_HEADERS)

        assert response.status_code == 400
        process_service.get_job.assert_not_awaited()

    @pytest.mark.usefixtures("process_service", "authorizer")
    def test_get_job_without_token(self, client: TestClient):
        response = client.get(f"/jobs/{self.JOB.sgid}")

        assert response.status_code == 401


class TestListJobsAPI:
    # PROCESS_ID = PROCESS.id
    OTHER_PROCESS = Process(
        id=8,
        sgid="prc_other",
        organization_sgid=PROCESS.organization_sgid,
        organization_unit_sgid=PROCESS.organization_unit_sgid,
        dataset_sgid=DATASET,
    )
    LISTED_JOB = Job(sgid="pjb_123", status="running", created_at=CREATED_AT, process=PROCESS)

    @pytest.fixture
    def job_service(self, process_service: AsyncMock) -> AsyncMock:
        process_service.get_processes.return_value = [PROCESS]
        process_service.list_jobs.return_value = ([self.LISTED_JOB], 1)
        return process_service

    def test_list_jobs(self, client: TestClient, job_service: AsyncMock, authorizer: AsyncMock):
        response = client.get(f"/jobs?processes={PROCESS.sgid}", headers=AUTH_HEADERS)

        assert response.status_code == 200
        assert response.json() == {
            "items": [
                {
                    "sgid": "pjb_123",
                    "process_sgid": PROCESS.sgid,
                    "status": "running",
                    "created_at": "2026-01-01T10:00:00Z",
                    "started_at": None,
                    "finished_at": None,
                }
            ],
            "total": 1,
            "limit": DEFAULT_LIMIT,
            "offset": 0,
        }
        job_service.get_processes.assert_awaited_once_with([PROCESS.sgid], None)
        authorizer.batch_is_authorized.assert_awaited_once_with(
            TOKEN, Action.GET_PROCESS, [process_resource(PROCESS)]
        )
        job_service.list_jobs.assert_awaited_once_with([PROCESS.id], None, DEFAULT_LIMIT, 0)

    @pytest.mark.parametrize(
        ("query", "expected_processes", "expected_datasets"),
        [
            pytest.param("?processes=a,b", ["a", "b"], None, id="processes"),
            pytest.param("?datasets=d1&datasets=d2", None, ["d1", "d2"], id="datasets"),
            pytest.param("?processes=a&datasets=d1", ["a"], ["d1"], id="processes and datasets"),
        ],
    )
    @pytest.mark.usefixtures("authorizer")
    def test_list_jobs_scope(
        self,
        client: TestClient,
        job_service: AsyncMock,
        query: str,
        expected_processes: list[str] | None,
        expected_datasets: list[str] | None,
    ):
        response = client.get(f"/jobs{query}", headers=AUTH_HEADERS)

        assert response.status_code == 200
        job_service.get_processes.assert_awaited_once_with(expected_processes, expected_datasets)

    @pytest.mark.usefixtures("authorizer")
    def test_list_jobs_status_and_pagination(self, client: TestClient, job_service: AsyncMock):
        response = client.get(
            f"/jobs?processes={PROCESS.sgid}&status=queued,running&limit=20&offset=40",
            headers=AUTH_HEADERS,
        )

        assert response.status_code == 200
        assert response.json()["limit"] == 20
        assert response.json()["offset"] == 40
        job_service.list_jobs.assert_awaited_once_with(
            [PROCESS.id], [JobStatus.QUEUED, JobStatus.RUNNING], 20, 40
        )

    @pytest.mark.parametrize(
        "query",
        [
            pytest.param("", id="missing scope"),
            pytest.param("?status=running", id="status without scope"),
            pytest.param("?processes=", id="empty processes"),
            pytest.param("?datasets=,", id="only separator datasets"),
            pytest.param(
                "?processes=" + ",".join(["p"] * (MAX_FILTER_VALUES + 1)), id="too many processes"
            ),
            pytest.param(
                "?datasets=" + ",".join(["d"] * (MAX_FILTER_VALUES + 1)), id="too many datasets"
            ),
            pytest.param("?processes=" + "a" * (SGID_MAX_LENGTH + 1), id="process id too long"),
            pytest.param(f"?processes={PROCESS.sgid}&status=unknown", id="unknown status"),
            pytest.param(f"?processes={PROCESS.sgid}&status=", id="empty status"),
        ],
    )
    @pytest.mark.usefixtures("authorizer")
    def test_list_jobs_invalid_query(self, client: TestClient, job_service: AsyncMock, query: str):
        response = client.get(f"/jobs{query}", headers=AUTH_HEADERS)

        assert response.status_code == 400
        job_service.get_processes.assert_not_awaited()
        job_service.list_jobs.assert_not_awaited()

    def test_list_jobs_only_of_allowed_processes(
        self, client: TestClient, job_service: AsyncMock, authorizer: AsyncMock
    ):
        job_service.get_processes.return_value = [PROCESS, self.OTHER_PROCESS]
        authorizer.batch_is_authorized.side_effect = lambda _token, _action, resources: [
            resource for resource in resources if resource.id == self.OTHER_PROCESS.sgid
        ]

        response = client.get(f"/jobs?datasets={DATASET}", headers=AUTH_HEADERS)

        assert response.status_code == 200
        authorizer.batch_is_authorized.assert_awaited_once_with(
            TOKEN,
            Action.GET_PROCESS,
            [process_resource(PROCESS), process_resource(self.OTHER_PROCESS)],
        )
        job_service.list_jobs.assert_awaited_once_with(
            [self.OTHER_PROCESS.id], None, DEFAULT_LIMIT, 0
        )

    def test_list_jobs_all_processes_denied(
        self, client: TestClient, job_service: AsyncMock, authorizer: AsyncMock
    ):
        job_service.list_jobs.return_value = ([], 0)
        authorizer.batch_is_authorized.side_effect = None
        authorizer.batch_is_authorized.return_value = []

        response = client.get(f"/jobs?processes={PROCESS.sgid}", headers=AUTH_HEADERS)

        assert response.status_code == 200
        assert response.json() == {"items": [], "total": 0, "limit": DEFAULT_LIMIT, "offset": 0}
        job_service.list_jobs.assert_awaited_once_with([], None, DEFAULT_LIMIT, 0)

    def test_list_jobs_authorization_unavailable(
        self, client: TestClient, job_service: AsyncMock, authorizer: AsyncMock
    ):
        authorizer.batch_is_authorized.side_effect = AuthorizationError("unavailable")

        response = client.get(f"/jobs?processes={PROCESS.sgid}", headers=AUTH_HEADERS)

        assert response.status_code == 503
        # Unauthorized users must not trigger calls to Airflow
        job_service.list_jobs.assert_not_awaited()

    @pytest.mark.usefixtures("authorizer")
    def test_list_jobs_without_token(self, client: TestClient, job_service: AsyncMock):
        response = client.get(f"/jobs?processes={PROCESS.sgid}")

        assert response.status_code == 401
        job_service.list_jobs.assert_not_awaited()

    @pytest.mark.usefixtures("authorizer", "job_service")
    def test_list_jobs_missing_scope_message(self, client: TestClient):
        response = client.get("/jobs?status=running", headers=AUTH_HEADERS)

        assert response.status_code == 400
        assert response.json()["detail"][0]["msg"] == (
            "At least one of 'processes' or 'datasets' is required"
        )
