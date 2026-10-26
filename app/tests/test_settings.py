from sqlalchemy import make_url

from pydantic import ValidationError

import pytest

from app.settings import Settings

# Pydantic will automatically load any .env or .env.default file, so for testing to avoid
# any different test result between CI and local environment (in which .env file can differ)
# we make sure pydantic doesn't load the environment file with `_env_file=None`

REQUIRED_DB_SETTINGS = ("DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PW")


@pytest.fixture(autouse=True)
def required_db_settings(monkeypatch: pytest.MonkeyPatch):
    # The database connection settings have no defaults and must always be provided, so set
    # valid values that individual tests can override or delete.
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_PORT", "5432")
    monkeypatch.setenv("DB_NAME", "processes")
    monkeypatch.setenv("DB_USER", "user")
    monkeypatch.setenv("DB_PW", "pw")


def test_defaults(monkeypatch: pytest.MonkeyPatch):
    # `_env_file=None` only disables pydantic-settings' own dotenv loading, it does not
    # prevent pydantic from reading real OS environment variables, so we clear them here
    # to make sure the actual field defaults are exercised.
    monkeypatch.delenv("ROOT_PATH", raising=False)
    monkeypatch.delenv("CORS_ORIGINS", raising=False)
    monkeypatch.delenv("CORS_METHOD", raising=False)
    monkeypatch.delenv("CORS_HEADERS", raising=False)
    monkeypatch.delenv("CORS_MAX_AGE", raising=False)
    monkeypatch.delenv("DB_CONNECT_TIMEOUT", raising=False)
    monkeypatch.delenv("DOC_PREFIX_PATH", raising=False)

    settings = Settings(
        _env_file=None,
    )

    assert settings.root_path == ""
    assert settings.doc_prefix_path == ""
    assert settings.cors_origins == []
    assert settings.cors_method == ["GET", "POST"]
    assert settings.cors_headers == ["*"]
    assert settings.cors_max_age == 600
    assert settings.db_connect_timeout == 10


def test_missing_db_settings_raise(monkeypatch: pytest.MonkeyPatch):
    for name in REQUIRED_DB_SETTINGS:
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(ValidationError) as exc_info:
        Settings(
            _env_file=None,
        )

    missing = {error["loc"][0] for error in exc_info.value.errors() if error["type"] == "missing"}
    assert missing == {"db_host", "db_port", "db_name", "db_user", "db_pw"}


def test_async_database_url_from_component_settings(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DB_HOST", "db.example.test")
    monkeypatch.setenv("DB_PORT", "5432")
    monkeypatch.setenv("DB_NAME", "processes")
    monkeypatch.setenv("DB_USER", "readonly")
    monkeypatch.setenv("DB_PW", "secret")

    settings = Settings(
        _env_file=None,
    )

    assert (
        settings.async_database_url
        == "postgresql+asyncpg://readonly:secret@db.example.test:5432/processes"
    )


def test_async_database_url_escapes_special_characters(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DB_USER", "us@r")
    monkeypatch.setenv("DB_PW", "p@ss:w/rd#%?")

    settings = Settings(
        _env_file=None,
    )

    url = make_url(settings.async_database_url)
    assert url.username == "us@r"
    assert url.password == "p@ss:w/rd#%?"  # noqa: S105


def test_env_overrides_simple_values(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("ROOT_PATH", "/weird/path")

    settings = Settings(
        _env_file=None,
    )

    assert settings.root_path == "/weird/path"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("", ""),
        ("/", ""),
        ("specs", "/specs"),
        ("/specs", "/specs"),
        ("/specs/", "/specs"),
        ("/api/specs/", "/api/specs"),
    ],
)
def test_doc_prefix_path_is_normalized(monkeypatch: pytest.MonkeyPatch, value: str, expected: str):
    monkeypatch.setenv("DOC_PREFIX_PATH", value)

    settings = Settings(
        _env_file=None,
    )

    assert settings.doc_prefix_path == expected


def test_parse_list_from_comma_separated_string(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("CORS_ORIGINS", "https://example.com,http://localhost")
    monkeypatch.setenv("CORS_METHOD", "GET,POST,PUT")
    monkeypatch.setenv("CORS_HEADERS", "Authorization,Content-Type")

    settings = Settings(
        _env_file=None,
    )

    assert settings.cors_origins == ["https://example.com", "http://localhost"]
    assert settings.cors_method == ["GET", "POST", "PUT"]
    assert settings.cors_headers == ["Authorization", "Content-Type"]
