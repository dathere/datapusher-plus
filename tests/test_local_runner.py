# -*- coding: utf-8 -*-
"""
Unit-level coverage for the no-Prefect (``prefect_enabled = false``) path.

Three things are under test here:

* ``config.prefect_enabled`` — the switch itself.
* ``jobs/local_runner.py`` — the in-process runner: stage order, the
  Jobs-row state machine, complete-with-skip, datastore rollback, the
  PII-review guard, and the RQ helpers (``enqueue_job`` /
  ``get_running_resource_ids``).
* ``logic/action.datapusher_submit`` routing — that flipping the flag
  moves submissions from Prefect to RQ, and that the task_status row
  records the run id under a key matching the backend.

Every test runs without a Prefect server, an RQ worker, CKAN, or
Postgres: stage classes are patched to no-ops that mutate the
``ProcessingContext`` the way the real stages would.

The critical invariant — that this path never imports Prefect — is
asserted directly in ``test_local_runner_does_not_import_prefect``.
"""

from __future__ import annotations

import logging
import sys
from contextlib import ExitStack
from types import SimpleNamespace
from unittest import mock

import pytest


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def job_input():
    from ckanext.datapusher_plus.jobs.runtime_context import JobInput

    return JobInput(
        task_id="test-task-local-1",
        resource_id="resource-abc",
        ckan_url="http://ckan.test",
        input={
            "api_key": "test-token",
            "job_type": "push_to_datastore",
            "result_url": "http://ckan.test/api/3/action/datapusher_hook",
            "metadata": {
                "resource_id": "resource-abc",
                "ckan_url": "http://ckan.test",
                "ignore_hash": False,
            },
        },
        dry_run=True,
    )


STAGE_NAMES = [
    "DownloadStage",
    "FormatConverterStage",
    "ValidationStage",
    "AnalysisStage",
    "AISuggestionsStage",
    "DatabaseStage",
    "IndexingStage",
    "FormulaStage",
    "MetadataStage",
]


@pytest.fixture
def patched_runner():
    """Patch every external integration so ``run_job`` runs in-process.

    Yields a dict with the stage mocks (keyed by class name), the
    ``mark_job_as_*`` mocks, the callback mock, and ``calls`` — the
    ordered list of stage names as they execute, which is what the
    stage-ordering assertions read.
    """
    from ckanext.datapusher_plus.jobs import local_runner

    calls: list[str] = []
    stages = {}

    with ExitStack() as stack:
        for name in STAGE_NAMES:
            stage_instance = mock.MagicMock()
            stage_instance.name = name

            def _run(ctx, _name=name):
                calls.append(_name)
                return ctx

            stage_instance.side_effect = _run
            stack.enter_context(
                mock.patch.object(
                    local_runner, name, return_value=stage_instance
                )
            )
            stages[name] = stage_instance

        stack.enter_context(
            mock.patch(
                "ckanext.datapusher_plus.jobs.pipeline_core.dsu.get_resource",
                return_value={
                    "url_type": "upload",
                    "format": "CSV",
                    "url": "x.csv",
                },
            )
        )
        stack.enter_context(
            mock.patch("ckanext.datapusher_plus.jobs.pipeline_core.QSVCommand")
        )
        stack.enter_context(
            mock.patch(
                "ckanext.datapusher_plus.jobs.pipeline_core.Path.is_file",
                return_value=True,
            )
        )
        stack.enter_context(
            mock.patch(
                "ckanext.datapusher_plus.jobs.pipeline_core.utils.StoringHandler",
                return_value=logging.NullHandler(),
            )
        )
        callback = stack.enter_context(
            mock.patch.object(
                local_runner, "callback_datapusher_hook", return_value=True
            )
        )
        stack.enter_context(mock.patch.object(local_runner.dph, "add_pending_job"))
        stack.enter_context(mock.patch.object(local_runner.dph, "set_aps_job_id"))
        mark_completed = stack.enter_context(
            mock.patch.object(local_runner.dph, "mark_job_as_completed")
        )
        mark_errored = stack.enter_context(
            mock.patch.object(local_runner.dph, "mark_job_as_errored")
        )
        stack.enter_context(
            mock.patch.object(local_runner.dph, "mark_job_as_failed_to_post_result")
        )
        stack.enter_context(mock.patch.object(local_runner.dict_stash, "clear"))

        yield {
            "stages": stages,
            "calls": calls,
            "callback": callback,
            "mark_completed": mark_completed,
            "mark_errored": mark_errored,
        }


# ---------------------------------------------------------------------------
# The switch
# ---------------------------------------------------------------------------


def test_prefect_enabled_defaults_to_true(monkeypatch):
    import ckanext.datapusher_plus.config as conf

    monkeypatch.setitem(conf.tk.config, "ckanext.datapusher_plus.prefect_enabled", "")
    assert conf.prefect_enabled() is True


@pytest.mark.parametrize(
    "value,expected",
    [("false", False), ("False", False), ("no", False), (False, False),
     ("true", True), (True, True), ("1", True)],
)
def test_prefect_enabled_reads_config(monkeypatch, value, expected):
    import ckanext.datapusher_plus.config as conf

    monkeypatch.setitem(
        conf.tk.config, "ckanext.datapusher_plus.prefect_enabled", value
    )
    assert conf.prefect_enabled() is expected


# ---------------------------------------------------------------------------
# The no-Prefect invariant
# ---------------------------------------------------------------------------


def test_local_runner_does_not_import_prefect():
    """Importing the local path must not pull Prefect in.

    This is the whole point of the mode: on a host whose
    ``$PREFECT_HOME`` is unwritable, ``import prefect`` itself raises
    ``PermissionError: '/root/.prefect/profiles.toml'``. Anything the
    disabled path imports has to stay clear of it.

    Runs in a subprocess because it needs a pristine ``sys.modules``:
    other tests in this session have Prefect imported already, and
    tearing it back out of ``sys.modules`` in-process would hand every
    later Prefect test a second, mismatched copy of the library.
    """
    import subprocess

    script = (
        "import sys\n"
        "import ckanext.datapusher_plus.jobs.pipeline_core\n"
        "import ckanext.datapusher_plus.jobs.local_runner\n"
        "leaked = sorted(n for n in sys.modules "
        "if n == 'prefect' or n.startswith('prefect.'))\n"
        "print(','.join(leaked))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.strip() == "", (
        f"the no-Prefect path imported Prefect: {result.stdout.strip()}"
    )


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_run_job_runs_all_stages_in_order_and_completes(job_input, patched_runner):
    from ckanext.datapusher_plus.jobs.local_runner import run_job

    result = run_job(job_input)

    assert result is None  # same contract as the flow
    assert patched_runner["calls"] == STAGE_NAMES
    patched_runner["mark_completed"].assert_called_once()
    patched_runner["mark_errored"].assert_not_called()


def test_run_job_accepts_the_enqueued_dict_payload(job_input, patched_runner):
    """RQ hands back the ``asdict``-ed payload, not the dataclass."""
    from dataclasses import asdict

    from ckanext.datapusher_plus.jobs.local_runner import run_job

    assert run_job(asdict(job_input)) is None
    patched_runner["mark_completed"].assert_called_once()


def test_run_job_posts_running_then_complete_callbacks(job_input, patched_runner):
    from ckanext.datapusher_plus.jobs.local_runner import run_job

    run_job(job_input)

    statuses = [
        call.kwargs["job_dict"]["status"]
        for call in patched_runner["callback"].call_args_list
    ]
    assert statuses == ["running", "complete"]


# ---------------------------------------------------------------------------
# Failure propagation
# ---------------------------------------------------------------------------


def test_run_job_marks_errored_and_posts_error_callback(job_input, patched_runner):
    from ckanext.datapusher_plus import utils
    from ckanext.datapusher_plus.jobs.local_runner import run_job

    patched_runner["stages"]["AnalysisStage"].side_effect = utils.JobError(
        "fake analysis failure"
    )

    with pytest.raises(utils.JobError):
        run_job(job_input)

    patched_runner["mark_errored"].assert_called_once()
    args, _ = patched_runner["mark_errored"].call_args
    assert "fake analysis failure" in args[1]

    statuses = [
        call.kwargs["job_dict"]["status"]
        for call in patched_runner["callback"].call_args_list
    ]
    assert statuses == ["running", "error"]


def test_stage_returning_none_completes_with_skip(job_input, patched_runner):
    """A stage returning ``None`` is "nothing to do", not a failure."""
    from ckanext.datapusher_plus.jobs.local_runner import run_job

    patched_runner["stages"]["AnalysisStage"].side_effect = lambda ctx: None

    assert run_job(job_input) is None

    patched_runner["mark_errored"].assert_not_called()
    patched_runner["mark_completed"].assert_called_once()
    _, kwargs = patched_runner["mark_completed"].call_args
    args, _ = patched_runner["mark_completed"].call_args
    assert args[1] == {"skipped": "AnalysisStage"}
    # Stages after the abort must not have run.
    assert "DatabaseStage" not in patched_runner["calls"]


def test_datastore_dump_resource_is_skipped(job_input, patched_runner):
    """``url_type == 'datastore'`` completes without running any stage."""
    from ckanext.datapusher_plus.jobs.local_runner import run_job

    with mock.patch(
        "ckanext.datapusher_plus.jobs.pipeline_core.dsu.get_resource",
        return_value={"url_type": "datastore"},
    ):
        assert run_job(job_input) is None

    assert patched_runner["calls"] == []
    args, _ = patched_runner["mark_completed"].call_args
    assert args[1] == {"skipped": "datastore-managed"}


# ---------------------------------------------------------------------------
# Rollback
# ---------------------------------------------------------------------------


def test_failure_after_database_stage_drops_the_datastore_table(
    job_input, patched_runner
):
    """Indexing blowing up must clean up the half-built table.

    Mirrors what ``database_task.on_rollback`` does under Prefect's
    ``transaction()``.
    """
    from ckanext.datapusher_plus import utils
    from ckanext.datapusher_plus.jobs import pipeline_core
    from ckanext.datapusher_plus.jobs.local_runner import run_job

    patched_runner["stages"]["IndexingStage"].side_effect = utils.JobError(
        "fake indexing failure"
    )

    with mock.patch.object(
        pipeline_core.dsu, "delete_datastore_resource"
    ) as delete_ds, mock.patch.object(
        pipeline_core.dict_stash, "load", return_value=None
    ):
        with pytest.raises(utils.JobError):
            run_job(job_input)

    delete_ds.assert_called_once_with(job_input.resource_id)


def test_database_stage_failure_leaves_the_datastore_alone(
    job_input, patched_runner
):
    """A database-stage failure must NOT drop the table.

    The stage raises before it is known to have touched anything — "could
    not connect to the Datastore" is the common case — so dropping here
    would destroy data this run never wrote.
    """
    from ckanext.datapusher_plus import utils
    from ckanext.datapusher_plus.jobs import pipeline_core
    from ckanext.datapusher_plus.jobs.local_runner import run_job

    patched_runner["stages"]["DatabaseStage"].side_effect = utils.JobError(
        "Could not connect to the Datastore"
    )

    with mock.patch.object(
        pipeline_core.dsu, "delete_datastore_resource"
    ) as delete_ds:
        with pytest.raises(utils.JobError):
            run_job(job_input)

    delete_ds.assert_not_called()


# ---------------------------------------------------------------------------
# PII review guard
# ---------------------------------------------------------------------------


def test_pii_review_gate_aborts_before_datastore_writes(job_input, patched_runner):
    """With no Prefect there is nothing to suspend on, so a flagged run
    must stop before the database stage rather than load unreviewed PII."""
    from ckanext.datapusher_plus import utils
    from ckanext.datapusher_plus.jobs import local_runner
    from ckanext.datapusher_plus.jobs.local_runner import run_job

    def _flag_pii(ctx):
        patched_runner["calls"].append("AnalysisStage")
        ctx.pii_found = True
        ctx.pii_candidate_count = 5
        return ctx

    patched_runner["stages"]["AnalysisStage"].side_effect = _flag_pii

    with mock.patch.object(local_runner, "resolve_int") as resolve_int:
        # ``resolve_int`` also resolves the job timeout — only the PII
        # threshold should be non-zero here.
        resolve_int.side_effect = lambda env, key, default: (
            1 if "pii_review_threshold" in key else default
        )
        with pytest.raises(utils.JobError, match="PII review gate crossed"):
            run_job(job_input)

    assert "DatabaseStage" not in patched_runner["calls"]
    patched_runner["mark_errored"].assert_called_once()


def test_pii_review_threshold_zero_does_not_gate(job_input, patched_runner):
    """The feature is off by default: PII alone must not stop a job."""
    from ckanext.datapusher_plus.jobs.local_runner import run_job

    def _flag_pii(ctx):
        patched_runner["calls"].append("AnalysisStage")
        ctx.pii_found = True
        ctx.pii_candidate_count = 5
        return ctx

    patched_runner["stages"]["AnalysisStage"].side_effect = _flag_pii

    assert run_job(job_input) is None
    assert "DatabaseStage" in patched_runner["calls"]


# ---------------------------------------------------------------------------
# RQ helpers
# ---------------------------------------------------------------------------


def test_enqueue_job_hands_the_payload_and_timeout_to_rq(job_input):
    from dataclasses import asdict

    from ckanext.datapusher_plus.jobs import local_runner

    with mock.patch.object(
        local_runner.tk, "enqueue_job", return_value=SimpleNamespace(id="rq-1")
    ) as enqueue:
        assert local_runner.enqueue_job(job_input, timeout=900) == "rq-1"

    args, kwargs = enqueue.call_args
    assert args[0] is local_runner.run_job
    assert args[1] == [asdict(job_input)]
    assert kwargs["rq_kwargs"] == {"timeout": 900}


def test_get_running_resource_ids_reads_queued_and_started_jobs():
    from ckanext.datapusher_plus.jobs import local_runner

    queued = SimpleNamespace(args=[{"resource_id": "res-queued"}])
    started = SimpleNamespace(args=[{"resource_id": "res-started"}])
    unrelated = SimpleNamespace(args=["not-a-payload"])

    queue = mock.MagicMock()
    queue.get_jobs.return_value = [queued, unrelated]
    queue.fetch_job.return_value = started

    fake_registry = mock.MagicMock()
    fake_registry.get_job_ids.return_value = ["started-id"]

    with mock.patch("ckan.lib.jobs.get_queue", return_value=queue), \
            mock.patch("rq.registry.StartedJobRegistry", return_value=fake_registry):
        ids = local_runner.get_running_resource_ids()

    assert ids == {"res-queued", "res-started"}


def test_get_running_resource_ids_fails_open():
    """An unreachable Redis must not block submissions."""
    from ckanext.datapusher_plus.jobs import local_runner

    with mock.patch(
        "ckan.lib.jobs.get_queue", side_effect=RuntimeError("redis down")
    ):
        assert local_runner.get_running_resource_ids() == set()


# ---------------------------------------------------------------------------
# datapusher_submit routing
# ---------------------------------------------------------------------------


@pytest.fixture
def prefect_off(monkeypatch):
    import ckanext.datapusher_plus.config as conf

    monkeypatch.setitem(
        conf.tk.config, "ckanext.datapusher_plus.prefect_enabled", "false"
    )


def test_orchestrator_is_prefect_by_default():
    import ckanext.datapusher_plus.prefect_client as prefect_client
    from ckanext.datapusher_plus.logic import action

    assert action._orchestrator() is prefect_client


def test_orchestrator_is_local_runner_when_disabled(prefect_off):
    from ckanext.datapusher_plus.jobs import local_runner
    from ckanext.datapusher_plus.logic import action

    assert action._orchestrator() is local_runner


def test_submit_job_goes_to_prefect_by_default(job_input):
    from dataclasses import asdict

    from ckanext.datapusher_plus.logic import action

    with mock.patch.object(
        action.prefect_client, "submit_flow_run", return_value="flow-1"
    ) as submit:
        run_id, via_prefect = action._submit_job(job_input, 900)

    assert (run_id, via_prefect) == ("flow-1", True)
    submit.assert_called_once_with(asdict(job_input), timeout=900)


def test_submit_job_goes_to_rq_when_disabled(job_input, prefect_off):
    from ckanext.datapusher_plus.jobs import local_runner
    from ckanext.datapusher_plus.logic import action

    with mock.patch.object(
        local_runner, "enqueue_job", return_value="rq-1"
    ) as enqueue:
        run_id, via_prefect = action._submit_job(job_input, 900)

    assert (run_id, via_prefect) == ("rq-1", False)
    enqueue.assert_called_once_with(job_input, timeout=900)


def _run_datapusher_submit(monkeypatch, submit_return):
    """Drive ``datapusher_submit`` with CKAN's I/O stubbed out.

    Returns the ``task_status_update`` dict the action wrote, which is
    what records the orchestrator's run id.
    """
    import json

    from ckanext.datapusher_plus.logic import action

    updates: list[dict] = []

    def _fake_get_action(name):
        if name == "resource_show":
            return lambda ctx, data: {"id": data["id"], "package_id": "pkg-1"}
        if name == "task_status_show":
            def _raise(ctx, data):
                raise action.tk.ObjectNotFound("no task")

            return _raise
        if name == "task_status_update":
            return lambda ctx, task: updates.append(task) or task
        raise AssertionError(f"unexpected action: {name}")

    monkeypatch.setattr(action.p.toolkit, "get_action", _fake_get_action)
    monkeypatch.setattr(action.p.toolkit, "check_access", lambda *a, **kw: True)
    monkeypatch.setattr(action.h, "url_for", lambda *a, **kw: "http://ckan.test/")
    monkeypatch.setattr(action.utils, "get_dp_plus_user_apitoken", lambda: "tok")
    monkeypatch.setattr(action, "_submit_job", lambda job_input, t: submit_return)

    context = {"model": mock.MagicMock(), "user": "tester"}
    assert action.datapusher_submit(context, {"resource_id": "resource-abc"}) is True
    assert updates, "the action never wrote a task_status row"
    return json.loads(updates[-1]["value"])


def test_submit_records_flow_run_id_under_prefect(monkeypatch):
    value = _run_datapusher_submit(monkeypatch, ("flow-1", True))

    assert value["flow_run_id"] == "flow-1"
    assert "rq_job_id" not in value
    assert value["job_id"]


def test_submit_records_rq_job_id_when_prefect_disabled(monkeypatch):
    """A Prefect-UI deep-link must never be built from an RQ job id."""
    value = _run_datapusher_submit(monkeypatch, ("rq-1", False))

    assert value["rq_job_id"] == "rq-1"
    assert "flow_run_id" not in value
    assert value["job_id"]
