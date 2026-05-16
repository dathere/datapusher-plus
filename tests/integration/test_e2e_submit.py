# -*- coding: utf-8 -*-
"""
End-to-end integration tests for the v3.0 Prefect flow.

These tests assume the ``docker-compose.integration.yaml`` stack is up
and the DP+ deployment has been registered via
``ckan datapusher_plus prefect-deploy``. They are skipped unless the
``INTEGRATION=1`` environment variable is set (see conftest.py).

They cover the verification scenarios called out in the v3.0 plan §3:
happy path, flow-run visibility in the Prefect UI, datastore population,
and CKAN-side status reporting.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest
import requests

from tests.integration.conftest import _post_action, wait_for_status


SAMPLE_CSV = """id,name,amount
1,alice,100
2,bob,200
3,carol,300
"""


@pytest.fixture
def sample_csv_path(tmp_path: Path) -> str:
    """Write the SAMPLE_CSV constant to a tempfile and yield its path."""
    p = tmp_path / "sample.csv"
    p.write_text(SAMPLE_CSV)
    return str(p)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_csv_resource_lands_in_datastore(
    ckan_session, ckan_url, prefect_url, fresh_resource, sample_csv_path
):
    """Upload a small CSV → flow completes → rows queryable from datastore."""

    # Upload the CSV as a resource.
    with open(sample_csv_path, "rb") as fh:
        upload = ckan_session.post(
            f"{ckan_url}/api/3/action/resource_create",
            data={
                "package_id": fresh_resource["id"],
                "format": "CSV",
                "name": "sample",
            },
            files={"upload": ("sample.csv", fh, "text/csv")},
            timeout=60,
        )
    upload.raise_for_status()
    resource = upload.json()["result"]
    resource_id = resource["id"]

    # The resource_create hook should auto-submit; if not, force-submit.
    _post_action(
        ckan_session, ckan_url, "datapusher_submit",
        {"resource_id": resource_id, "ignore_hash": True},
    )

    # Poll until the flow finishes.
    status = wait_for_status(
        ckan_session, ckan_url, resource_id, {"complete", "error"}
    )
    assert status["status"] == "complete", f"flow ended with: {status}"
    assert status.get("flow_run_id"), "task_status.value should carry flow_run_id"

    # Verify the flow run is visible in the Prefect API.
    fr = requests.get(
        f"{prefect_url}/api/flow_runs/{status['flow_run_id']}", timeout=10
    )
    assert fr.status_code == 200, fr.text
    assert fr.json()["state"]["type"] == "COMPLETED"

    # Verify the data landed: datastore_search should return 3 rows.
    ds = _post_action(
        ckan_session, ckan_url, "datastore_search",
        {"resource_id": resource_id, "limit": 10},
    )
    assert ds["total"] == 3
    names = {row["name"] for row in ds["records"]}
    assert names == {"alice", "bob", "carol"}


# ---------------------------------------------------------------------------
# Cached resubmit
# ---------------------------------------------------------------------------


def test_resubmit_same_resource_hits_download_cache(
    ckan_session, ckan_url, prefect_url, fresh_resource, sample_csv_path
):
    """Submitting the same unchanged resource twice → second run hits cache.

    Specifically, the download_task should be cached on the second run
    (same URL, ignore_hash=False) and its persisted result reused.
    """
    with open(sample_csv_path, "rb") as fh:
        resource = ckan_session.post(
            f"{ckan_url}/api/3/action/resource_create",
            data={"package_id": fresh_resource["id"], "format": "CSV", "name": "cache"},
            files={"upload": ("sample.csv", fh, "text/csv")},
            timeout=60,
        ).json()["result"]
    rid = resource["id"]

    # First submission seeds the cache.
    _post_action(
        ckan_session, ckan_url, "datapusher_submit",
        {"resource_id": rid, "ignore_hash": True},
    )
    first = wait_for_status(ckan_session, ckan_url, rid, {"complete"})
    first_fr = first["flow_run_id"]

    # Re-submit without ignore_hash so the cache key matches.
    _post_action(
        ckan_session, ckan_url, "datapusher_submit",
        {"resource_id": rid, "ignore_hash": False},
    )
    second = wait_for_status(ckan_session, ckan_url, rid, {"complete"})
    second_fr = second["flow_run_id"]

    assert first_fr != second_fr, "second submit should produce a distinct flow run"

    # Inspect the download task run for the second flow: it should be in
    # a cached state.
    runs = requests.post(
        f"{prefect_url}/api/task_runs/filter",
        json={"flow_runs": {"id": {"any_": [second_fr]}}},
        timeout=10,
    ).json()
    download_runs = [r for r in runs if r["name"] == "download"]
    assert download_runs, f"no download task run for {second_fr}"
    # Prefect labels cache hits with a "Cached" state name.
    assert any(r["state"]["name"] == "Cached" for r in download_runs), (
        f"expected at least one Cached download run, got {[r['state'] for r in download_runs]}"
    )
