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
        (1, 1),
        (20, 20),
        (MAX_LIMIT, MAX_LIMIT),
        ("20", 20),
        (MAX_LIMIT + 1, MAX_LIMIT),
        (1000, MAX_LIMIT),
        (0, 1),
        (-5, 1),
        ("abc", DEFAULT_LIMIT),
        ("", DEFAULT_LIMIT),
        (1.5, DEFAULT_LIMIT),
    ],
)
def test_pagination_params_clamps_limit(limit: Any, expected: int):
    assert PaginationParams(limit=limit).limit == expected


@pytest.mark.parametrize(
    ("offset", "expected"),
    [
        (0, 0),
        (40, 40),
        ("40", 40),
        (-1, 0),
        ("abc", 0),
        ("", 0),
    ],
)
def test_pagination_params_clamps_offset(offset: Any, expected: int):
    assert PaginationParams(offset=offset).offset == expected
