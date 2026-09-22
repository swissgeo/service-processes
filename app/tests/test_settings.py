import pytest

from app.settings import Settings

# NOTE: Pydantic will automatically load any .env or .env.default file, so for testing to avoid
# any different test result between CI and local environment (in which .env file can differ)
# we make sure pydantic doesn't load the environment file with `_env_file=None`


def test_defaults():
    settings = Settings(
        _env_file=None,
    )

    assert settings.root_path == "/api/oap/v1"
    assert settings.cors_origins == []
    assert settings.cors_method == ["GET", "POST"]
    assert settings.cors_headers == ["*"]
    assert settings.cors_max_age == 600


def test_env_overrides_simple_values(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("ROOT_PATH", "/api")

    settings = Settings(
        _env_file=None,
    )

    assert settings.root_path == "/api"


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
