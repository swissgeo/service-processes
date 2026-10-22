from unittest.mock import AsyncMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

import pytest


def test_startup_pings_database(app: FastAPI, monkeypatch: pytest.MonkeyPatch) -> None:
    check_database_connection = AsyncMock()
    monkeypatch.setattr("app.main.check_database_connection", check_database_connection)

    with TestClient(app):
        check_database_connection.assert_awaited_once_with(app.state.database_engine)


def test_startup_fails_and_closes_database_when_database_is_unreachable(
    app: FastAPI, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "app.main.check_database_connection",
        AsyncMock(side_effect=ConnectionRefusedError("database unreachable")),
    )

    with pytest.raises(ConnectionRefusedError, match="database unreachable"), TestClient(app):
        pass

    assert app.state.database_engine is None
    assert app.state.database_session_maker is None
