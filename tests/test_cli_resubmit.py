# -*- coding: utf-8 -*-
"""
Unit coverage for the resubmit / submit CLI batch-error handling.

Before this refactor, ``_submit`` was a 16-line loop that swallowed every
exception, printed "OK" or "Fail" per resource, and unconditionally
exited 0 — making ``ckan datapusher_plus resubmit`` impossible to wire
into CI / monitoring because a fully-failed batch and a fully-succeeded
batch were indistinguishable from a shell-exit perspective.

The new ``_submit`` returns a bool, and the ``resubmit`` / ``submit``
commands raise ``click.exceptions.Exit(code=1)`` when any resource
didn't make it. The unit tests below exercise the bookkeeping in
``_submit`` directly (so they don't need a full CKAN application
context) plus the CLI-level exit-code wiring via Click's CliRunner.
"""

from __future__ import annotations

from unittest import mock

import pytest


@pytest.fixture
def cli_module():
    """Import the CLI module once with CKAN's toolkit available."""
    pytest.importorskip("ckan")
    from ckanext.datapusher_plus import cli

    return cli


@pytest.fixture
def patched_actions(cli_module):
    """Stub the two ``tk.get_action`` lookups ``_submit`` depends on so
    we don't need a live CKAN application / datastore."""
    site_user = mock.Mock(return_value={"name": "site-user"})
    submit = mock.Mock()

    def _get_action(name):
        if name == "get_site_user":
            return site_user
        if name == "datapusher_submit":
            return submit
        raise AssertionError(f"unexpected action: {name}")

    with mock.patch.object(cli_module.tk, "get_action", side_effect=_get_action):
        yield site_user, submit


# ---------- _submit return value ------------------------------------


def test_submit_empty_list_returns_true(cli_module):
    assert cli_module._submit([]) is True


def test_submit_all_ok_returns_true(cli_module, patched_actions):
    _, submit = patched_actions
    submit.return_value = True
    assert cli_module._submit(["a", "b", "c"]) is True
    assert submit.call_count == 3


def test_submit_any_fail_returns_false(cli_module, patched_actions):
    """A resource that ``datapusher_submit`` declines (returns falsy) is
    a failure for the whole batch — the exit-code-1 case."""
    _, submit = patched_actions
    submit.side_effect = [True, False, True]
    assert cli_module._submit(["a", "b", "c"]) is False
    assert submit.call_count == 3


def test_submit_exception_isolated_and_continues(cli_module, patched_actions):
    """A single resource raising an exception must NOT take out the
    whole batch (network blip, transient auth issue, etc.) — the
    legacy try-less loop would have crashed out partway."""
    _, submit = patched_actions
    submit.side_effect = [True, RuntimeError("boom"), True]
    assert cli_module._submit(["a", "b", "c"]) is False
    assert submit.call_count == 3


def test_submit_stop_on_error_breaks_on_fail(cli_module, patched_actions):
    _, submit = patched_actions
    submit.side_effect = [True, False, True]  # third should be skipped
    assert (
        cli_module._submit(["a", "b", "c"], continue_on_error=False) is False
    )
    assert submit.call_count == 2


def test_submit_stop_on_error_breaks_on_exception(cli_module, patched_actions):
    _, submit = patched_actions
    submit.side_effect = [True, RuntimeError("boom"), True]
    assert (
        cli_module._submit(["a", "b", "c"], continue_on_error=False) is False
    )
    assert submit.call_count == 2


# ---------- _print_submit_summary -----------------------------------


def test_summary_lists_failed_and_errored_resource_ids(cli_module, capsys):
    cli_module._print_submit_summary(
        total=3,
        ok=["a"],
        failed=["b"],
        errored=[("c", "Connection refused")],
        stopped_early=False,
    )
    out = capsys.readouterr().out
    assert "OK:      1 / 3" in out
    assert "Fail:    1" in out
    assert "Error:   1" in out
    assert "- b" in out
    assert "- c: Connection refused" in out
    assert "Skipped" not in out


def test_summary_reports_skipped_when_stopped_early(cli_module, capsys):
    cli_module._print_submit_summary(
        total=5,
        ok=["a"],
        failed=["b"],
        errored=[],
        stopped_early=True,
    )
    out = capsys.readouterr().out
    # 5 total - 2 attempted = 3 skipped
    assert "Skipped: 3" in out


# ---------- CLI exit codes -------------------------------------------


def test_resubmit_exits_zero_when_all_ok(cli_module, patched_actions):
    """``resubmit`` walks the datastore-resources list and submits each.
    Mock both the listing and the per-resource submit so the test
    doesn't need a live datastore."""
    _, submit = patched_actions
    submit.return_value = True
    runner_cls = pytest.importorskip("click.testing").CliRunner
    runner = runner_cls()
    with mock.patch.object(
        cli_module.datastore_backend,
        "get_all_resources_ids_in_datastore",
        return_value=["r1", "r2"],
    ):
        result = runner.invoke(cli_module.resubmit, ["--yes"])
    assert result.exit_code == 0, result.output
    assert "OK:      2 / 2" in result.output


def test_resubmit_exits_one_on_any_failure(cli_module, patched_actions):
    _, submit = patched_actions
    submit.side_effect = [True, False]
    runner_cls = pytest.importorskip("click.testing").CliRunner
    runner = runner_cls()
    with mock.patch.object(
        cli_module.datastore_backend,
        "get_all_resources_ids_in_datastore",
        return_value=["r1", "r2"],
    ):
        result = runner.invoke(cli_module.resubmit, ["--yes"])
    assert result.exit_code == 1
    assert "Fail:    1" in result.output


def test_resubmit_stop_on_error_flag(cli_module, patched_actions):
    _, submit = patched_actions
    submit.side_effect = [False, True]  # second should not run
    runner_cls = pytest.importorskip("click.testing").CliRunner
    runner = runner_cls()
    with mock.patch.object(
        cli_module.datastore_backend,
        "get_all_resources_ids_in_datastore",
        return_value=["r1", "r2"],
    ):
        result = runner.invoke(
            cli_module.resubmit, ["--yes", "--stop-on-error"]
        )
    assert result.exit_code == 1
    assert submit.call_count == 1
    assert "Skipped: 1" in result.output
