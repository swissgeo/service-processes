import asyncio
import logging
import time
from collections.abc import Callable
from datetime import datetime
from typing import Annotated, Any

import httpx

from fastapi import Depends, FastAPI, Request
from pydantic import BaseModel

from app.settings import Settings

logger = logging.getLogger(__name__)


# What we expect back from auth/token
class AirflowAccessTokenResponse(BaseModel):
    access_token: str


# What we expect back from Get Dag Run (only relevant fields)
class AirflowDagRunResponse(BaseModel):
    start_date: datetime | None = None
    end_date: datetime | None = None
    state: str


def _read_k8s_token() -> str:
    with open("/var/run/secrets/airflow/token") as f:  # re-read every time; kubelet rotates it
        return f.read().strip()


# Returns the request arguments for POST /auth/token. Only how the Airflow token is obtained
# differs between environments, caching, retrying and the API calls are the same.
TokenRequest = Callable[[], dict[str, Any]]


def k8s_token_request() -> dict[str, Any]:
    """Exchange the Kubernetes service account token for an Airflow token (in the cluster)."""
    return {"headers": {"Authorization": f"Bearer {_read_k8s_token()}"}}


def password_token_request(username: str, password: str) -> TokenRequest:
    """Log in with username and password (simple auth manager of the local Airflow)."""
    return lambda: {"json": {"username": username, "password": password}}


class AirflowError(Exception):
    """A request to Airflow failed or returned an unexpected response."""


class AirflowClient:
    """Airflow client"""

    def __init__(
        self,
        host: str,
        airflow_token_ttl: int,
        airflow_client_timeout: float,
        token_request: TokenRequest = k8s_token_request,
        transport: httpx.AsyncBaseTransport | None = None,  # e.g. httpx.MockTransport in tests
    ) -> None:
        self._token_request = token_request
        self._airflow_token_ttl = airflow_token_ttl
        self._http = httpx.AsyncClient(
            base_url=host, timeout=airflow_client_timeout, transport=transport
        )
        self._token: str | None = None
        self._token_expires_at = 0.0
        self._lock = asyncio.Lock()

    async def _auth_headers(self) -> dict[str, str]:
        async with self._lock:
            if self._token is None or time.monotonic() >= self._token_expires_at:
                response = await self._http.post("/auth/token", **self._token_request())
                response.raise_for_status()
                try:
                    token = AirflowAccessTokenResponse.model_validate(response.json())
                except ValueError as error:
                    raise AirflowError(f"Unexpected token response: {error}") from error
                self._token = token.access_token
                self._token_expires_at = time.monotonic() + self._airflow_token_ttl - 60
            return {"Authorization": f"Bearer {self._token}"}

    async def _request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        """Retry requests once with a new token if status 401 is returned."""
        try:
            response = await self._http.request(
                method, url, headers=await self._auth_headers(), **kwargs
            )
            if response.status_code == httpx.codes.UNAUTHORIZED:
                # Retry request using a new token
                self._token = None
                response = await self._http.request(
                    method, url, headers=await self._auth_headers(), **kwargs
                )
            response.raise_for_status()
        except httpx.HTTPError as error:
            raise AirflowError(f"{method} {url} failed: {error}") from error
        return response

    async def trigger_dag_run(self, dag_id: str, dag_run_id: str, conf: dict[str, Any]) -> None:
        """Trigger a run of the DAG with the given (caller chosen) run id and conf."""
        await self._request(
            method="POST",
            url=f"/api/v2/dags/{dag_id}/dagRuns",
            json={"dag_run_id": dag_run_id, "conf": conf, "logical_date": None},
        )

    async def get_dag_run(self, dag_id: str, dag_run_id: str) -> AirflowDagRunResponse:
        """Get details of a dag run."""
        response = await self._request(
            method="GET",
            url=f"/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}",
        )
        try:
            return AirflowDagRunResponse.model_validate(response.json())
        except ValueError as error:
            raise AirflowError(f"Unexpected dag run response: {error}") from error

    # async def get_dag_run_task_instances(self, dag_id: str, dag_run_id: str) -> Any:
    #     """Get details of each step of a dag run."""
    #     return {}

    async def aclose(self) -> None:
        await self._http.aclose()


def initialize_airflow_client(app: FastAPI, settings: Settings) -> None:
    token_request = k8s_token_request
    if settings.use_local_airflow:
        if not settings.airflow_local_username or not settings.airflow_local_password:
            raise RuntimeError(
                "USE_LOCAL_AIRFLOW requires AIRFLOW_LOCAL_USERNAME and AIRFLOW_LOCAL_PASSWORD"
            )
        logger.warning("Using local Airflow with username/password login")
        token_request = password_token_request(
            settings.airflow_local_username, settings.airflow_local_password.get_secret_value()
        )
    app.state.airflow_client = AirflowClient(
        settings.airflow_host,
        settings.airflow_token_ttl,
        settings.airflow_client_timeout,
        token_request=token_request,
    )


async def close_airflow_client(app: FastAPI) -> None:
    await app.state.airflow_client.aclose()


def get_airflow_client(request: Request) -> AirflowClient:
    return request.app.state.airflow_client


AirflowDep = Annotated[AirflowClient, Depends(get_airflow_client)]
