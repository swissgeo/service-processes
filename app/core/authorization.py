import logging
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from enum import StrEnum
from typing import Annotated, Any, Protocol

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from starlette.concurrency import run_in_threadpool

from fastapi import Depends, FastAPI, Header, HTTPException, Request

from app.models.process import Process
from app.settings import Settings

logger = logging.getLogger(__name__)


class Action(StrEnum):
    """Actions defined in the policy store schema."""

    GET_PROCESS = "getProcess"
    LIST_PROCESSES = "listProcesses"


class VPEntityType(StrEnum):
    """Verified permissions schema entity types."""

    ORGANIZATION = "Organization"
    UNIT = "Unit"
    DATASET = "Dataset"
    PROCESS = "Process"


@dataclass(frozen=True)
class Resource:
    """An entity of the policy store schema together with its parent."""

    type: VPEntityType
    id: uuid.UUID
    parent: Resource | None = None

    def hierarchy(self) -> Iterator[Resource]:
        """Yield the resource itself followed by all its ancestors."""
        resource: Resource | None = self
        while resource is not None:
            yield resource
            resource = resource.parent


def process_resource(process: Process) -> Resource:
    """Return the process as resource: Process -> Dataset -> Unit -> Organization."""
    organization = Resource(VPEntityType.ORGANIZATION, process.organization_sgid)
    organization_unit = Resource(VPEntityType.UNIT, process.organization_unit_sgid, organization)
    dataset = Resource(VPEntityType.DATASET, process.dataset_sgid, organization_unit)
    return Resource(VPEntityType.PROCESS, process.sgid, dataset)


def dataset_resource(
    dataset_sgid: uuid.UUID,
    organization_unit_sgid: uuid.UUID,
    organization_sgid: uuid.UUID,
) -> Resource:
    """Return the dataset as resource: Dataset -> Unit -> Organization."""
    organization = Resource(VPEntityType.ORGANIZATION, organization_sgid)
    organization_unit = Resource(VPEntityType.UNIT, organization_unit_sgid, organization)
    return Resource(VPEntityType.DATASET, dataset_sgid, organization_unit)


class AuthorizationError(Exception):
    """The authorization decision could not be made."""


class Authorizer(Protocol):
    async def is_authorized(self, token: str, action: Action, resource: Resource) -> bool:
        """Return whether the principal of the token may perform the action on the resource."""
        ...

    async def batch_is_authorized(
        self, token: str, action: Action, resources: list[Resource]
    ) -> list[Resource]:
        """Return the resources on which the principal of the token may perform the action."""
        ...


class VerifiedPermissionsAuthorizer:
    """Authorizer backed by an AWS Verified Permissions policy store.

    The principal is taken from the access token, which is validated by Verified Permissions
    against the identity source of the policy store.
    """

    def __init__(self, client: Any, policy_store_id: str, namespace: str) -> None:
        self._client = client
        self._policy_store_id = policy_store_id
        self._namespace = namespace

    def _identifier(self, resource: Resource) -> dict[str, str]:
        return {"entityType": f"{self._namespace}::{resource.type}", "entityId": str(resource.id)}

    def _entities(self, *resources: Resource) -> list[dict[str, Any]]:
        # Resources can share ancestors, dict.fromkeys removes duplicates while keeping the order
        unique = dict.fromkeys(r for resource in resources for r in resource.hierarchy())
        return [
            {
                "identifier": self._identifier(r),
                "parents": [self._identifier(r.parent)] if r.parent else [],
            }
            for r in unique
        ]

    async def is_authorized(self, token: str, action: Action, resource: Resource) -> bool:
        try:
            # boto3 is synchronous, so run it in the threadpool to not block the event loop
            response = await run_in_threadpool(
                self._client.is_authorized_with_token,
                policyStoreId=self._policy_store_id,
                accessToken=token,
                action={"actionType": f"{self._namespace}::Action", "actionId": action},
                resource=self._identifier(resource),
                entities={"entityList": self._entities(resource)},
            )
        except (ClientError, BotoCoreError) as error:
            raise AuthorizationError(str(error)) from error

        # Cedar skips policies that fail to evaluate, so an erroring forbid policy could turn a
        # DENY into an ALLOW. The decision cannot be trusted, deny instead.
        if response["errors"]:
            logger.error(
                "Errors while evaluating policies for %s on %s, denying: %s",
                action,
                resource,
                response["errors"],
            )
            return False
        return response["decision"] == "ALLOW"

    # Hard limit of verified permissions for number of requests per Batch.
    # https://docs.aws.amazon.com/verifiedpermissions/latest/apireference/API_BatchIsAuthorizedWithToken.html
    _MAX_BATCH_SIZE = 30

    async def batch_is_authorized(
        self, token: str, action: Action, resources: list[Resource]
    ) -> list[Resource]:
        """Return the resources on which the principal of the token may perform the action."""
        allowed: list[Resource] = []
        for start in range(0, len(resources), self._MAX_BATCH_SIZE):
            allowed.extend(
                await self._batch_is_authorized(
                    token, action, resources[start : start + self._MAX_BATCH_SIZE]
                )
            )
        return allowed

    async def _batch_is_authorized(
        self, token: str, action: Action, resources: list[Resource]
    ) -> list[Resource]:
        by_identifier = {str(resource.id): resource for resource in resources}
        try:
            # boto3 is synchronous, so run it in the threadpool to not block the event loop
            response = await run_in_threadpool(
                self._client.batch_is_authorized_with_token,
                policyStoreId=self._policy_store_id,
                accessToken=token,
                requests=[
                    {
                        "action": {"actionType": f"{self._namespace}::Action", "actionId": action},
                        "resource": self._identifier(resource),
                    }
                    for resource in resources
                ],
                entities={"entityList": self._entities(*resources)},
            )
        except (ClientError, BotoCoreError) as error:
            raise AuthorizationError(str(error)) from error

        allowed = []
        for result in response["results"]:
            entity_id = result["request"]["resource"]["entityId"]
            # See is_authorized: the decision cannot be trusted if any policy errored
            if result["errors"]:
                logger.error(
                    "Errors while evaluating policies for %s on %s, denying: %s",
                    action,
                    entity_id,
                    result["errors"],
                )
                continue
            if result["decision"] == "ALLOW":
                allowed.append(by_identifier[entity_id])
        return allowed


def initialize_authorizer(app: FastAPI, settings: Settings) -> None:
    """Initialize the Verified Permissions authorizer for the application."""
    if settings.use_local_verified_permissions:
        try:
            # Only available locally, module is excluded from docker image
            from app.local_dev.verified_permissions.client import DummyAuthorizer  # noqa: PLC0415
        except ImportError as error:
            raise RuntimeError(
                "USE_LOCAL_VERIFIED_PERMISSIONS is only supported for local development and must "
                "not be enabled in the docker image"
            ) from error

        logger.warning("Using dummy authorizer, ALL requests are allowed")
        app.state.authorizer = DummyAuthorizer()
        return

    client = boto3.client(
        "verifiedpermissions",
        config=Config(
            connect_timeout=settings.avp_connect_timeout,
            read_timeout=settings.avp_read_timeout,
            retries={"mode": "standard", "total_max_attempts": 2},
        ),
    )
    app.state.authorizer = VerifiedPermissionsAuthorizer(
        client, settings.avp_policy_store_id, settings.avp_namespace
    )
    logger.info("Initialized Verified Permissions authorizer")


def get_authorizer(request: Request) -> Authorizer:
    return request.app.state.authorizer


AuthorizerDep = Annotated[Authorizer, Depends(get_authorizer)]


def get_access_token(
    token: Annotated[
        str | None,
        Header(
            alias="X-Auth-Request-Access-Token",
            description="Access token (JWT) of the user, set by the authentication proxy",
        ),
    ] = None,
) -> str:
    # The header is optional on purpose, so that a missing token results in a 401 instead of a
    # validation error
    if not token:
        raise HTTPException(status_code=401, detail="Missing access token")
    return token


AccessTokenDep = Annotated[str, Depends(get_access_token)]


class Authorization:
    """Authorizes actions of the principal of the request's access token.

    Fails closed: if no decision can be made, the request is rejected.
    """

    def __init__(self, authorizer: Authorizer, token: str) -> None:
        self._authorizer = authorizer
        self._token = token

    async def require(self, action: Action, resource: Resource) -> None:
        """Raise an HTTPException unless the principal may perform the action on the resource."""
        try:
            allowed = await self._authorizer.is_authorized(self._token, action, resource)
        except AuthorizationError:
            logger.exception("Authorization of %s on %s failed", action, resource)
            raise HTTPException(status_code=503) from None
        if not allowed:
            raise HTTPException(status_code=403, detail="Forbidden")

    async def filter(self, action: Action, resources: list[Resource]) -> list[Resource]:
        """Return the resources on which the principal may perform the action."""
        try:
            return await self._authorizer.batch_is_authorized(self._token, action, resources)
        except AuthorizationError:
            logger.exception("Batch authorization of %s failed", action)
            raise HTTPException(status_code=503) from None


def get_authorization(authorizer: AuthorizerDep, token: AccessTokenDep) -> Authorization:
    return Authorization(authorizer, token)


AuthorizationDep = Annotated[Authorization, Depends(get_authorization)]
