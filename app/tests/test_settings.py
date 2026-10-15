import pytest

from app.settings import Settings

# Pydantic will automatically load any .env or .env.default file, so for testing to avoid
# any different test result between CI and local environment (in which .env file can differ)
# we make sure pydantic doesn't load the environment file with `_env_file=None`


def test_defaults(monkeypatch: pytest.MonkeyPatch):
    # `_env_file=None` only disables pydantic-settings' own dotenv loading, it does not
    # prevent pydantic from reading real OS environment variables, so we clear them here
    # to make sure the actual field defaults are exercised.
    monkeypatch.delenv("ROOT_PATH", raising=False)
    monkeypatch.delenv("CORS_ORIGINS", raising=False)
    monkeypatch.delenv("CORS_METHOD", raising=False)
    monkeypatch.delenv("CORS_HEADERS", raising=False)
    monkeypatch.delenv("CORS_MAX_AGE", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("DB_HOST", raising=False)
    monkeypatch.delenv("DB_PORT", raising=False)
    monkeypatch.delenv("DB_NAME", raising=False)
    monkeypatch.delenv("DB_USER", raising=False)
    monkeypatch.delenv("DB_PW", raising=False)
    monkeypatch.delenv("DB_CONNECT_TIMEOUT", raising=False)

    settings = Settings(
        _env_file=None,
    )

    assert settings.root_path == ""
    assert settings.cors_origins == []
    assert settings.cors_method == ["GET", "POST"]
    assert settings.cors_headers == ["*"]
    assert settings.cors_max_age == 600
    assert (
        settings.async_database_url
        == "postgresql+asyncpg://service_processes:service_processes@localhost:15432/service_processes"
    )
    assert settings.db_connect_timeout == 10


def test_database_url_takes_precedence_over_component_settings(monkeypatch: pytest.MonkeyPatch):
    database_url = "postgresql+asyncpg://readonly:secret@db.example.test:5432/processes"
    monkeypatch.setenv("DATABASE_URL", database_url)
    monkeypatch.setenv("DB_HOST", "ignored.example.test")

    settings = Settings(
        _env_file=None,
    )

    assert settings.async_database_url == database_url


def test_env_overrides_simple_values(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("ROOT_PATH", "/weird/path")

    settings = Settings(
        _env_file=None,
    )

    assert settings.root_path == "/weird/path"


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
