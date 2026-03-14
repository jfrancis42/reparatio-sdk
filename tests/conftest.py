"""Shared fixtures and helpers for reparatio-sdk tests."""
import json

import httpx
import pytest

from reparatio import Reparatio


def make_response(
    status: int = 200,
    *,
    json_data: dict | list | None = None,
    content: bytes = b"",
    headers: dict | None = None,
) -> httpx.Response:
    """Build a real httpx.Response suitable for mocking."""
    h = dict(headers or {})
    if json_data is not None:
        content = json.dumps(json_data).encode()
        h.setdefault("content-type", "application/json")
    return httpx.Response(status, content=content, headers=h)


@pytest.fixture
def client():
    """Reparatio client with a fake API key; never makes real HTTP calls."""
    return Reparatio(api_key="rp_test_xxxxxxxxxxxxxxxxxxxx")
