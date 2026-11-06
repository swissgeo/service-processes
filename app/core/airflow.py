import logging

import airflow_client.client
import requests
from airflow_client.client import ApiException

from pydantic import BaseModel

# TODO: This file is only a placeholder to show using the k8s
# service account token to get an airflow token

# TODO: Get from config
AIRFLOW = "http://airflow-api-server.airflow.svc.cluster.local:8080"
TIMEOUT = 10  # seconds

logger = logging.getLogger(__name__)


# What we expect back from auth/token
class AirflowAccessTokenResponse(BaseModel):
    access_token: str


def airflow_jwt() -> str:
    with open("/var/run/secrets/airflow/token") as f:  # re-read every time; kubelet rotates it
        k8s_token = f.read().strip()
    r = requests.post(
        f"{AIRFLOW}/auth/token",
        headers={"Authorization": f"Bearer {k8s_token}"},
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    return AirflowAccessTokenResponse(**r.json()).access_token


def create_asset_event_example() -> None:
    configuration = airflow_client.client.Configuration(host=AIRFLOW)
    configuration.access_token = airflow_jwt()

    with airflow_client.client.ApiClient(configuration) as api_client:
        api_instance = airflow_client.client.AssetApi(api_client)
        create_asset_events_body = airflow_client.client.CreateAssetEventsBody(asset_id=1)

        try:
            api_response = api_instance.create_asset_event(
                create_asset_events_body, _request_timeout=TIMEOUT
            )
            logger.info("The response of AssetApi->create_asset_event: %s", api_response)
        except ApiException:
            logger.exception("Exception when calling AssetApi->create_asset_event")


# jwt = airflow_jwt()
# requests.get(
#     f"{AIRFLOW}/api/v2/dags", headers={"Authorization": f"Bearer {jwt}"}, timeout=TIMEOUT
# )
