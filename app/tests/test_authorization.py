import sys
import uuid
from collections.abc import Generator
from unittest.mock import AsyncMock

import boto3
from botocore.stub import Stubber

from fastapi import FastAPI, HTTPException

import pytest

from app.core.authorization import (
    Action,
    AuthorizationError,
    Resource,
    VerifiedPermissionsAuthorizer,
    VPEntityType,
    authorize,
    initialize_authorizer,
    process_resource,
)
from app.local_dev.verified_permissions.client import DummyAuthorizer
from app.models.process import Process
from app.settings import Settings

TOKEN = "header.payload.signature"  # noqa: S105

ORGANIZATION = uuid.uuid7()
ORGANIZATION_UNIT = uuid.uuid7()
DATASET = uuid.uuid7()
PROCESS = Process(
    sgid=uuid.uuid7(),
    organization_sgid=ORGANIZATION,
    organization_unit_sgid=ORGANIZATION_UNIT,
    dataset_sgid=DATASET,
)


@pytest.fixture
def stubber() -> Generator[Stubber]:
    client = boto3.client("verifiedpermissions", region_name="eu-central-1")
    with Stubber(client) as stubber:
        yield stubber
    stubber.assert_no_pending_responses()


@pytest.fixture
def authorizer(stubber: Stubber) -> VerifiedPermissionsAuthorizer:
    return VerifiedPermissionsAuthorizer(stubber.client, "policy-store", "Test")


def decision(value: str, errors: list | None = None) -> dict:
    return {"decision": value, "determiningPolicies": [], "errors": errors or []}


def test_process_resource_hierarchy():
    resource = process_resource(PROCESS)

    assert [(r.type, r.id) for r in resource.hierarchy()] == [
        ("Process", PROCESS.sgid),
        ("Dataset", DATASET),
        ("Unit", ORGANIZATION_UNIT),
        ("Organization", ORGANIZATION),
    ]


@pytest.mark.asyncio
async def test_is_authorized_sends_token_action_and_entities(
    stubber: Stubber, authorizer: VerifiedPermissionsAuthorizer
):
    def entity(entity_type: str, entity_id: uuid.UUID) -> dict:
        return {"entityType": f"Test::{entity_type}", "entityId": str(entity_id)}

    stubber.add_response(
        "is_authorized_with_token",
        decision("ALLOW"),
        {
            "policyStoreId": "policy-store",
            "accessToken": TOKEN,
            "action": {"actionType": "Test::Action", "actionId": "getProcess"},
            "resource": entity("Process", PROCESS.sgid),
            "entities": {
                "entityList": [
                    {
                        "identifier": entity("Process", PROCESS.sgid),
                        "parents": [entity("Dataset", DATASET)],
                    },
                    {
                        "identifier": entity("Dataset", DATASET),
                        "parents": [entity("Unit", ORGANIZATION_UNIT)],
                    },
                    {
                        "identifier": entity("Unit", ORGANIZATION_UNIT),
                        "parents": [entity("Organization", ORGANIZATION)],
                    },
                    {"identifier": entity("Organization", ORGANIZATION), "parents": []},
                ]
            },
        },
    )

    assert await authorizer.is_authorized(TOKEN, Action.GET_PROCESS, process_resource(PROCESS))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [
        decision("DENY"),
        decision("DENY", [{"errorDescription": "policy evaluation failed"}]),
    ],
)
async def test_is_authorized_denied(
    stubber: Stubber, authorizer: VerifiedPermissionsAuthorizer, response: dict
):
    stubber.add_response("is_authorized_with_token", response)

    assert not await authorizer.is_authorized(TOKEN, Action.GET_PROCESS, process_resource(PROCESS))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error_code", ["ValidationException", "AccessDeniedException", "ThrottlingException"]
)
async def test_is_authorized_raises_on_client_error(
    stubber: Stubber, authorizer: VerifiedPermissionsAuthorizer, error_code: str
):
    stubber.add_client_error("is_authorized_with_token", service_error_code=error_code)

    with pytest.raises(AuthorizationError):
        await authorizer.is_authorized(TOKEN, Action.GET_PROCESS, process_resource(PROCESS))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("result", "expected_status"),
    [
        ({"return_value": False}, 403),
        ({"side_effect": AuthorizationError("unavailable")}, 503),
    ],
)
async def test_authorize_rejects(result: dict, expected_status: int):
    authorizer = AsyncMock()
    authorizer.is_authorized = AsyncMock(**result)

    with pytest.raises(HTTPException) as exc_info:
        await authorize(
            authorizer, TOKEN, Action.GET_PROCESS, Resource(VPEntityType.PROCESS, uuid.uuid7())
        )

    assert exc_info.value.status_code == expected_status


@pytest.mark.asyncio
async def test_authorize_allows():
    authorizer = AsyncMock()
    authorizer.is_authorized.return_value = True

    await authorize(
        authorizer, TOKEN, Action.GET_PROCESS, Resource(VPEntityType.PROCESS, uuid.uuid7())
    )


@pytest.mark.usefixtures("client")
def test_startup_initializes_authorizer(app: FastAPI):
    assert isinstance(app.state.authorizer, VerifiedPermissionsAuthorizer)


@pytest.mark.asyncio
async def test_initialize_local_dummy_authorizer():
    app = FastAPI()

    initialize_authorizer(app, Settings(_env_file=None, use_local_verified_permissions=True))

    assert isinstance(app.state.authorizer, DummyAuthorizer)
    assert await app.state.authorizer.is_authorized(
        TOKEN, Action.GET_PROCESS, process_resource(PROCESS)
    )


def test_initialize_local_dummy_authorizer_fails_without_local_dev_module(
    monkeypatch: pytest.MonkeyPatch,
):
    # Simulate the docker image, from which the local_dev module is excluded
    monkeypatch.setitem(sys.modules, "app.local_dev.verified_permissions.client", None)

    with pytest.raises(RuntimeError, match="USE_LOCAL_VERIFIED_PERMISSIONS"):
        initialize_authorizer(
            FastAPI(), Settings(_env_file=None, use_local_verified_permissions=True)
        )
