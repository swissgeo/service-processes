from typing import Any

import pytest

from app.schemas.pagination import DEFAULT_LIMIT, MAX_LIMIT, PaginationParams


def test_pagination_params_defaults():
    params = PaginationParams()

    assert params.limit == DEFAULT_LIMIT
    assert params.offset == 0


@pytest.mark.parametrize(
    ("limit", "expected"),
    [
        pytest.param(1, 1, id="min"),
        pytest.param(20, 20, id="valid"),
        pytest.param(MAX_LIMIT, MAX_LIMIT, id="max"),
        pytest.param("20", 20, id="numeric string"),
        pytest.param(MAX_LIMIT + 1, MAX_LIMIT, id="above max"),
        pytest.param(1000, MAX_LIMIT, id="far above max"),
        pytest.param(0, 1, id="zero"),
        pytest.param(-5, 1, id="negative"),
        pytest.param("abc", DEFAULT_LIMIT, id="not numeric"),
        pytest.param("", DEFAULT_LIMIT, id="empty string"),
        pytest.param(1.5, DEFAULT_LIMIT, id="float"),
    ],
)
def test_pagination_params_clamps_limit(limit: Any, expected: int):
    assert PaginationParams(limit=limit).limit == expected


@pytest.mark.parametrize(
    ("offset", "expected"),
    [
        pytest.param(0, 0, id="zero"),
        pytest.param(40, 40, id="valid"),
        pytest.param("40", 40, id="numeric string"),
        pytest.param(-1, 0, id="negative"),
        pytest.param("abc", 0, id="not numeric"),
        pytest.param("", 0, id="empty string"),
    ],
)
def test_pagination_params_clamps_offset(offset: Any, expected: int):
    assert PaginationParams(offset=offset).offset == expected
