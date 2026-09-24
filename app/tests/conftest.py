from collections.abc import Generator

from fastapi import FastAPI
from fastapi.testclient import TestClient

import pytest

from app.settings import Settings, get_settings


@pytest.fixture
def settings() -> Settings:
    """Fixture to provide application settings for testing."""
    return Settings(
        # Pydantic will automatically load any .env or .env.default file, so for testing to avoid
        # any different test result between CI and local environment (in which .env file can differ)
        # we make sure pydantic doesn't load the environment file with `_env_file=None`
        _env_file=None,
        cors_origins=["http://test.com", "https://hello.com"],
        cors_origin_regex=r"http://localhost:\d+",
        root_path="",
        otel_sdk_disabled=True,
        publish_openapi_spec=True,
    )


@pytest.fixture
def app() -> FastAPI:
    """Fixture to provide the FastAPI application instance for testing.

    This is important to use it in order to have all the mocking, especially the settings mocking,
    in place before the application is initialized.
    """
    # Do the import here to ensure that the application is initialized after the settings
    # are mocked.
    from app.main import app as fastapi_app  # noqa: PLC0415

    return fastapi_app


@pytest.fixture
def client(app: FastAPI, settings: Settings) -> Generator[TestClient]:
    """Fixture to provide a TestClient for the FastAPI application with settings dependency
    injection mocked.
    """

    def get_settings_override() -> Settings:
        return settings

    with TestClient(app) as client:
        app.dependency_overrides[get_settings] = get_settings_override
        yield client
