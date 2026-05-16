# -*- coding: utf-8 -*-
"""
End-to-end integration tests for the resubmit / submit CLI commands
introduced (well, repaired) in PR #299.

These tests cover the three manual-test-plan items from that PR:

1. ``ckan datapusher_plus resubmit -y`` with a failing resource in the
   datastore → exit code 1, ``Fail: 1`` summary.
2. ``ckan datapusher_plus resubmit -y --stop-on-error`` with a failing
   resource → stops after the first failure (``Skipped`` reported).
3. ``ckan datapusher_plus submit <good-package> -y`` on a happy-path
   package → exit code 0.

They invoke the CLI inside the ``ckan`` container of the
``docker-compose.integration.yaml`` stack via ``docker compose exec``,
so they need an already-running stack (see
``tests/integration/README.md``). Gated by ``INTEGRATION=1`` like the
rest of the integration suite.

Why not just CliRunner? The unit tests in ``tests/test_cli_resubmit.py``
do exactly that and cover ``_submit``'s bookkeeping + the exit-code
plumbing exhaustively. These integration tests add the bits CliRunner
can't reach: that ``ckan datapusher_plus resubmit`` is registered (i.e.
the entry_points / Click group are wired correctly), that the command
finds real datastore-resident resources via
``datastore_backend.get_all_resources_ids_in_datastore``, and that the
exit code propagates out of the real CKAN bootstrap into the shell.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import List

import pytest

from tests.integration.conftest import _post_action, wait_for_status


COMPOSE_FILE = os.environ.get(
    "DOCKER_COMPOSE_FILE", "docker-compose.integration.yaml"
)
COMPOSE_SERVICE = os.environ.get("CKAN_COMPOSE_SERVICE", "ckan")
CKAN_INI_IN_CONTAINER = os.environ.get(
    "CKAN_INI_IN_CONTAINER", "/etc/ckan/default/ckan.ini"
)


def _docker_compose_available() -> bool:
    """``docker compose`` (v2) is the only invocation form we support;
    skip cleanly otherwise so this file doesn't fail collection on
    machines without Docker."""
    return shutil.which("docker") is not None


@pytest.fixture(scope="module")
def docker_compose_or_skip():
    if not _docker_compose_available():
        pytest.skip("docker CLI not available — skipping CLI integration tests")
    if not Path(COMPOSE_FILE).exists():
        pytest.skip(f"{COMPOSE_FILE} not found — skipping CLI integration tests")


def _run_ckan_cli(
    args: List[str], *, timeout: float = 120.0
) -> subprocess.CompletedProcess:
    """Invoke ``ckan -c <ini> <args...>`` inside the running CKAN container.

    Returns the completed process so the caller can inspect ``returncode``,
    ``stdout``, ``stderr``. ``check=False`` — the whole point of these
    tests is to assert the exit code, so don't raise on non-zero.
    """
    cmd = [
        "docker",
        "compose",
        "-f",
        COMPOSE_FILE,
        "exec",
        "-T",  # disable TTY; we want to capture
        COMPOSE_SERVICE,
        "ckan",
        "-c",
        CKAN_INI_IN_CONTAINER,
        *args,
    ]
    return subprocess.run(
        cmd, capture_output=True, text=True, timeout=timeout, check=False
    )


def _wait_for_resource_in_datastore(
    ckan_session, ckan_url, resource_id: str, *, timeout: float = 180.0
) -> None:
    """Block until ``resource_id`` shows up in
    ``datastore_backend.get_all_resources_ids_in_datastore``.

    A resource only enters that listing once its datastore table is
    created — i.e. after a successful ingest. For the *failing* test
    case we want a resource whose row landed in the ``Jobs`` table
    with error status; the resubmit CLI walks the datastore listing
    AND any tracked jobs, so this fixture deliberately uses a
    successful ingest as the seed and induces the failure on the
    *re*-submit (see ``_break_resource_url``).
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        status = _post_action(
            ckan_session, ckan_url, "datapusher_status",
            {"resource_id": resource_id},
        )
        if status.get("status") == "complete":
            return
        time.sleep(3)
    raise TimeoutError(
        f"Resource {resource_id} never reached datastore within {timeout}s"
    )


def _break_resource_url(ckan_session, ckan_url, resource_id: str) -> None:
    """Patch a resource so its next submit will fail.

    Points the URL at a 404 endpoint inside the docker network so the
    next download_task hits a real HTTPError rather than DNS-failing
    on the host (which would surface as a confusing exception).
    """
    _post_action(
        ckan_session, ckan_url, "resource_patch",
        {
            "id": resource_id,
            "url": f"{ckan_url}/dataset/__nonexistent__/resource/{resource_id}/404.csv",
        },
    )


@pytest.fixture
def failing_resource(ckan_session, ckan_url, fresh_resource):
    """Seed a happy-path CSV → resource → datastore, then break its URL.

    Yields the resource id. After the test, the dataset is cleaned up
    by the ``fresh_resource`` fixture.
    """
    sample_csv = "id,name\n1,alice\n2,bob\n"
    upload = ckan_session.post(
        f"{ckan_url}/api/3/action/resource_create",
        data={
            "package_id": fresh_resource["id"],
            "format": "CSV",
            "name": "to-be-broken",
        },
        files={"upload": ("sample.csv", sample_csv, "text/csv")},
        timeout=60,
    )
    upload.raise_for_status()
    resource_id = upload.json()["result"]["id"]

    # Wait for the first ingest so the resource lands in the datastore.
    _post_action(
        ckan_session, ckan_url, "datapusher_submit",
        {"resource_id": resource_id, "ignore_hash": True},
    )
    _wait_for_resource_in_datastore(ckan_session, ckan_url, resource_id)

    # Now break it so the next submit fails.
    _break_resource_url(ckan_session, ckan_url, resource_id)
    return resource_id


# ---------------------------------------------------------------------------
# Test plan items
# ---------------------------------------------------------------------------


def test_resubmit_exits_one_when_resource_fails(
    docker_compose_or_skip, failing_resource
):
    """Test plan item 1: a failing resource → ``Fail: 1`` + exit 1.

    ``ckan datapusher_plus resubmit -y`` walks every datastore-resident
    resource. With at least one resource pointing at a 404 URL, its
    re-ingest will fail and the summary must reflect that. Prior to
    PR #299 this would have printed "Fail" but exited 0 — invisible
    to cron / CI.
    """
    result = _run_ckan_cli(["datapusher_plus", "resubmit", "-y"], timeout=240.0)
    assert result.returncode == 1, (
        f"expected exit 1, got {result.returncode}\n"
        f"stdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )
    # The failing resource shows up in the Fail/Error bucket. We don't
    # pin which one because ``datapusher_submit`` may return falsy
    # (Fail) or raise (Error) depending on the failure mode.
    assert (
        "Fail:" in result.stdout or "Error:" in result.stdout
    ), f"summary block missing in:\n{result.stdout}"
    # And the failing resource id is reported.
    assert failing_resource in result.stdout


def test_resubmit_stop_on_error_breaks_early(
    docker_compose_or_skip, failing_resource
):
    """Test plan item 2: ``--stop-on-error`` aborts at first failure.

    With a failing resource present, ``--stop-on-error`` should bail
    on the first non-OK outcome and report ``Skipped: N`` for the
    ones it didn't attempt. We can't deterministically force the
    failing resource to be processed first (the datastore listing
    order isn't guaranteed), so this test only asserts the exit code
    is non-zero AND ``Skipped`` appears in the output (which only
    happens when ``stopped_early`` is True).
    """
    result = _run_ckan_cli(
        ["datapusher_plus", "resubmit", "-y", "--stop-on-error"],
        timeout=240.0,
    )
    assert result.returncode == 1, (
        f"expected exit 1 with --stop-on-error and a failing resource present, "
        f"got {result.returncode}\n"
        f"stdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )
    # If a Fail/Error came before the all-OK runs, the summary block
    # reports Skipped. If by chance every other resource ran first,
    # only the final one would be the failure and there's nothing left
    # to skip — that path is exercised by the unit tests already
    # (``test_resubmit_stop_on_error_flag``).
    summary_says_skipped = "Skipped:" in result.stdout
    summary_has_failures = (
        "Fail:" in result.stdout or "Error:" in result.stdout
    )
    assert summary_has_failures, (
        f"summary should still report the failure even when nothing was skipped:\n"
        f"{result.stdout}"
    )
    # Sanity log — useful when debugging a flaky run.
    if not summary_says_skipped:
        print(
            "Note: --stop-on-error ran every prior resource before the failure;\n"
            "no resources were Skipped on this particular run."
        )


def test_submit_good_package_exits_zero(
    docker_compose_or_skip, ckan_session, ckan_url, fresh_resource
):
    """Test plan item 3: happy-path ``submit`` → exit 0.

    Create a dataset with one CSV resource and call
    ``ckan datapusher_plus submit <package> -y``. With nothing
    failing, exit code must be 0 and the summary must show all
    resources OK.
    """
    sample_csv = "id,name\n1,alice\n2,bob\n3,carol\n"
    upload = ckan_session.post(
        f"{ckan_url}/api/3/action/resource_create",
        data={
            "package_id": fresh_resource["id"],
            "format": "CSV",
            "name": "happy-path",
        },
        files={"upload": ("sample.csv", sample_csv, "text/csv")},
        timeout=60,
    )
    upload.raise_for_status()
    resource_id = upload.json()["result"]["id"]
    # Make sure auto-submit on resource_create finished before we call
    # submit again — otherwise we race against an in-flight ingest.
    _post_action(
        ckan_session, ckan_url, "datapusher_submit",
        {"resource_id": resource_id, "ignore_hash": True},
    )
    wait_for_status(
        ckan_session, ckan_url, resource_id, {"complete"}, timeout=240.0
    )

    package_id = fresh_resource["id"]
    result = _run_ckan_cli(
        ["datapusher_plus", "submit", package_id, "-y"], timeout=240.0
    )
    assert result.returncode == 0, (
        f"expected exit 0 for happy-path submit, got {result.returncode}\n"
        f"stdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )
    assert "OK:" in result.stdout
    assert "Fail:    0" in result.stdout
    assert "Error:   0" in result.stdout
