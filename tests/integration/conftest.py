# -*- coding: utf-8 -*-
"""
Pytest fixtures for the DataPusher+ integration test suite.

The fixtures assume the docker-compose.integration.yaml stack is running
on localhost (or whatever ``CKAN_URL`` / ``PREFECT_URL`` env vars
point at). See ``tests/integration/README.md`` for the run procedure.

The whole module short-circuits when ``INTEGRATION=1`` isn't set in the
environment, so a normal ``pytest tests/`` run skips these without
attempting any HTTP calls.
"""

from __future__ import annotations

import os
import time
import uuid
from typing import Any, Dict

import pytest


def pytest_collection_modifyitems(config, items):
    """Skip integration tests unless ``INTEGRATION=1`` is set."""
    if os.environ.get("INTEGRATION") == "1":
        return
    skip = pytest.mark.skip(reason="set INTEGRATION=1 to run integration tests")
    for item in items:
        if "tests/integration" in str(item.fspath):
            item.add_marker(skip)


@pytest.fixture(scope="session")
def ckan_url() -> str:
    return os.environ.get("CKAN_URL", "http://localhost:5000")


@pytest.fixture(scope="session")
def prefect_url() -> str:
    return os.environ.get("PREFECT_URL", "http://localhost:4200")


@pytest.fixture(scope="session")
def ckan_api_key() -> str:
    """API key for the test sysadmin user.

    Lookup order:
      1. ``CKAN_API_KEY`` env var (CI / explicit override).
      2. ``./.integration-token`` (written by ``scripts/integration-up``).
         Preferred for local dev — the file is chmod 600 and gitignored,
         so the JWT never lands in the process command line (``ps -ef``)
         the way ``CKAN_API_KEY=$(cat .integration-token) pytest …``
         would expose it on a multi-user box.

    Skips the test if neither is available.
    """
    key = os.environ.get("CKAN_API_KEY", "")
    if key:
        return key

    # Walk up from the conftest dir to find the repo root that holds
    # ``.integration-token``. ``Path(__file__).resolve()`` is in
    # ``<repo>/tests/integration/`` so two parents up is the repo root.
    import pathlib
    token_path = pathlib.Path(__file__).resolve().parents[2] / ".integration-token"
    if token_path.is_file():
        try:
            return token_path.read_text().strip()
        except OSError:
            pass

    pytest.skip(
        "CKAN_API_KEY env var (or ./.integration-token written by "
        "`scripts/integration-up`) required for integration tests."
    )


@pytest.fixture
def ckan_session(ckan_api_key):
    """A ``requests.Session`` pre-authenticated for the CKAN API."""
    import requests

    session = requests.Session()
    session.headers["Authorization"] = ckan_api_key
    return session


def _post_action(session, ckan_url: str, action: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """POST to a CKAN action endpoint and return its ``result`` field."""
    r = session.post(f"{ckan_url}/api/3/action/{action}", json=payload, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not data.get("success"):
        raise RuntimeError(f"{action} failed: {data}")
    return data["result"]


@pytest.fixture
def fresh_resource(ckan_session, ckan_url):
    """Create a dataset + CSV resource for one test; clean up afterwards."""
    dataset_name = f"integration-{uuid.uuid4().hex[:8]}"
    pkg = _post_action(
        ckan_session,
        ckan_url,
        "package_create",
        {"name": dataset_name, "title": "DP+ integration test"},
    )
    yield pkg
    # Best-effort cleanup. We deliberately swallow exceptions so a failure
    # to delete one dataset doesn't blow up subsequent tests.
    try:
        _post_action(
            ckan_session, ckan_url, "package_delete", {"id": pkg["id"]}
        )
    except Exception:
        pass


def wait_for_status(
    session,
    ckan_url: str,
    resource_id: str,
    target_states: set[str],
    *,
    timeout: float = 300.0,
    interval: float = 5.0,
) -> Dict[str, Any]:
    """Poll ``datapusher_status`` until the task reaches a target state."""
    deadline = time.time() + timeout
    last: Dict[str, Any] = {}
    while time.time() < deadline:
        status = _post_action(
            session, ckan_url, "datapusher_status", {"resource_id": resource_id}
        )
        last = status
        if status.get("status") in target_states:
            return status
        time.sleep(interval)
    raise TimeoutError(
        f"datapusher_status did not reach {target_states} within {timeout}s. "
        f"Last status: {last}"
    )
