# -*- coding: utf-8 -*-
"""
Unit-level coverage for the ``migrate-from-rq`` CLI command.

These tests exercise the v2 -> v3 migration path without needing a
running CKAN, RQ, or Prefect by mocking each external dependency at
its import site. They follow the same pattern as
``test_prefect_flow.py`` and ``test_prefect_client.py``: lazy imports
inside fixtures and ``pytest.importorskip("ckan")`` so the file
collects in any environment but only runs where CKAN is installed
(the ``dpp-test`` ckan-dev container; CI).
"""

from __future__ import annotations

import json
from unittest import mock

import pytest


def _row(entity_id, value):
    """Build a ``MagicMock`` that behaves like a ``TaskStatus`` row."""
    row = mock.MagicMock()
    row.entity_id = entity_id
    row.value = json.dumps(value) if value is not None else None
    return row


@pytest.fixture
def cli():
    """Import the CLI module lazily — its top-level imports need CKAN."""
    pytest.importorskip("ckan")
    from ckanext.datapusher_plus import cli as cli_mod

    return cli_mod


@pytest.fixture
def patched(cli):
    """Patch every external dependency ``migrate-from-rq`` touches.

    The ``pending_rows`` list is mutable so each test can populate the
    ``TaskStatus`` query result inline before invoking the command.
    """
    fake_queue = mock.MagicMock()
    fake_queue.get_jobs.return_value = []

    mock_session = mock.MagicMock()
    pending_rows: list = []
    # ``side_effect`` (not ``return_value``) so tests can mutate the
    # list after the fixture sets up — each ``.all()`` call sees the
    # current contents.
    mock_session.query.return_value.filter_by.return_value.all.side_effect = (
        lambda: list(pending_rows)
    )

    patches = [
        # Patch ``get_queue`` on the real module instead of swapping
        # the module in ``sys.modules`` — the latter trips beartype's
        # ``claw`` import hook on subsequent imports inside the
        # function body (e.g. ``import ckanext.datapusher_plus
        # .prefect_client``) and surfaces as a spurious circular
        # import error in the ``except`` branch.
        mock.patch("ckan.lib.jobs.get_queue", return_value=fake_queue),
        # ``from ckan import model as ckan_model`` resolves
        # ``ckan_model.Session`` to this attribute on the (already
        # imported) ``ckan.model`` module.
        mock.patch("ckan.model.Session", mock_session),
        # ``_submit`` is a module-level helper — patching the module
        # attribute is enough because the function looks it up at
        # call time from globals.
        mock.patch.object(cli, "_submit"),
        mock.patch(
            "ckanext.datapusher_plus.prefect_client.get_running_resource_ids",
            return_value=[],
        ),
    ]
    started = [p.start() for p in patches]
    yield {
        "get_queue": started[0],
        "queue": fake_queue,
        "session": mock_session,
        "pending_rows": pending_rows,
        "submit": started[2],
        "get_running_resource_ids": started[3],
    }
    for p in patches:
        p.stop()


def test_migrate_completes_when_queue_and_taskstatus_are_empty(cli, patched):
    """No drainable RQ jobs and no pending TaskStatus rows: the command
    runs cleanly through the Prefect reachability check and exits 0."""
    from click.testing import CliRunner

    result = CliRunner().invoke(cli.migrate_from_rq, ["--yes"])

    assert result.exit_code == 0, result.output
    assert "Drained 0 RQ jobs" in result.output
    assert "Reset 0 stale" in result.output
    assert "Prefect server is reachable" in result.output
    patched["get_running_resource_ids"].assert_called_once()


def test_migrate_resets_pending_rows_to_error(cli, patched):
    """``pending`` TaskStatus rows without ``flow_run_id`` are flipped
    to ``error`` so the CKAN UI no longer treats them as in-flight;
    the mutation happens inside a single ``session.commit()``."""
    from click.testing import CliRunner

    row_a = _row("res-a", {"job_id": "j-a"})
    row_b = _row("res-b", None)
    patched["pending_rows"].extend([row_a, row_b])

    result = CliRunner().invoke(cli.migrate_from_rq, ["--yes"])

    assert result.exit_code == 0, result.output
    assert row_a.state == "error"
    assert row_b.state == "error"
    assert json.loads(row_a.error) == {
        "message": "migrated to Prefect; please resubmit"
    }
    patched["session"].commit.assert_called_once()
    assert "Reset 2 stale" in result.output


def test_migrate_skips_rows_already_on_prefect(cli, patched):
    """Rows whose ``value`` already carries a ``flow_run_id`` are on
    the new path. The command must leave them untouched, and under
    ``--resubmit`` must not include them in the resubmit list."""
    from click.testing import CliRunner

    on_rq = _row("res-rq", None)
    on_prefect = _row("res-prefect", {"flow_run_id": "abc-123"})
    patched["pending_rows"].extend([on_rq, on_prefect])

    result = CliRunner().invoke(
        cli.migrate_from_rq, ["--yes", "--resubmit"]
    )

    assert result.exit_code == 0, result.output
    # The Prefect-bound row was never mutated; its ``state`` is still
    # the default MagicMock attribute (a Mock object, not ``"error"``).
    assert on_prefect.state != "error"
    # Resubmit got the RQ-bound resource_id only.
    patched["submit"].assert_called_once_with(["res-rq"])


def test_migrate_aborts_when_prefect_unreachable(cli, patched):
    """If ``get_running_resource_ids`` raises (server down, wrong URL,
    bad auth), the command must abort with a non-zero exit code and
    must not invoke ``_submit`` — silently claiming success would
    leave the operator with no running worker."""
    from click.testing import CliRunner

    patched["get_running_resource_ids"].side_effect = RuntimeError(
        "connection refused"
    )

    result = CliRunner().invoke(
        cli.migrate_from_rq, ["--yes", "--resubmit"]
    )

    assert result.exit_code != 0
    assert "Cannot reach Prefect server" in result.output
    patched["submit"].assert_not_called()
