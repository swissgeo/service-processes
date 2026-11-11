import asyncio
import logging
import time
from typing import Annotated, Any

import httpx

from fastapi import Depends, FastAPI, Request
from pydantic import BaseModel

from app.settings import Settings

logger = logging.getLogger(__name__)


# What we expect back from auth/token
class AirflowAccessTokenResponse(BaseModel):
    access_token: str


def _read_k8s_token() -> str:
    with open("/var/run/secrets/airflow/token") as f:  # re-read every time; kubelet rotates it
        return f.read().strip()


class AirflowClient:
    """Airflow client"""

    def __init__(
        self,
        host: str,
        airflow_token_ttl: int,
        airflow_client_timeout: float,
        transport: httpx.AsyncBaseTransport | None = None,  # e.g. httpx.MockTransport in tests
    ) -> None:
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
                response = await self._http.post(
                    "/auth/token", headers={"Authorization": f"Bearer {_read_k8s_token()}"}
                )
                response.raise_for_status()
                self._token = AirflowAccessTokenResponse(**response.json()).access_token
                self._token_expires_at = time.monotonic() + self._airflow_token_ttl - 60
            return {"Authorization": f"Bearer {self._token}"}

    async def _request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        """Retry requests once with a new token if status 401 is returned."""
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
        return response

    async def trigger_dag_run(self, dag_id: str, dag_run_id: str, conf: dict[str, Any]) -> None:
        """Trigger a run of the DAG with the given (caller chosen) run id and conf."""
        await self._request(
            method="POST",
            url=f"/api/v2/dags/{dag_id}/dagRuns",
            json={"dag_run_id": dag_run_id, "conf": conf, "logical_date": None},
        )

    # async def get_dag_run(self, dag_id: str, dag_run_id: str) -> Any:
    #     """Get details of a dag run."""
    #     return {}

    # async def get_dag_run_task_instances(self, dag_id: str, dag_run_id: str) -> Any:
    #     """Get details of each step of a dag run."""
    #     return {}

    async def aclose(self) -> None:
        await self._http.aclose()


def initialize_airflow_client(app: FastAPI, settings: Settings) -> None:
    app.state.airflow_client = AirflowClient(
        settings.airflow_host, settings.airflow_token_ttl, settings.airflow_client_timeout
    )


async def close_airflow_client(app: FastAPI) -> None:
    await app.state.airflow_client.aclose()


def get_airflow_client(request: Request) -> AirflowClient:
    return request.app.state.airflow_client


AirflowDep = Annotated[AirflowClient, Depends(get_airflow_client)]
