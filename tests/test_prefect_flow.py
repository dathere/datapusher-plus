# -*- coding: utf-8 -*-
"""
Unit-level coverage for the Prefect flow.

These tests exercise the v3.0 control flow (RuntimeContext binding,
state-machine ordering, task-error propagation, callback POSTs) without
needing a running Prefect server, CKAN, or Postgres. Stage classes are
patched at import time so each "stage" is a no-op that just mutates the
``ProcessingContext`` the way the real stage would.

The integration tests under ``tests/integration/`` (added separately)
cover the full path with a real Prefect server and CKAN — those need
Docker compose, not pytest in a vacuum.
"""

from __future__ import annotations

import logging
from unittest import mock

import pytest


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def job_input():
    from ckanext.datapusher_plus.jobs.runtime_context import JobInput

    return JobInput(
        task_id="test-task-1",
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


@pytest.fixture
def patched_dependencies():
    """Patch every external integration so the flow runs in-process.

    Patches:
      * Each stage's ``__call__`` to a no-op that returns the context.
      * ``datastore_utils.get_resource`` to return a non-datastore resource.
      * ``helpers.add_pending_job`` / ``mark_job_as_*`` to no-op.
      * ``QSVCommand`` constructor + the ``QSV_BIN`` path check.
      * ``utils.StoringHandler`` → a ``NullHandler`` so the task logger
        does not write to the ``Logs`` table (no DB bind in unit tests).
      * ``callback_datapusher_hook`` so no HTTP POST goes out.
    """
    patches = [
        mock.patch(
            "ckanext.datapusher_plus.jobs.prefect_flow.DownloadStage",
            return_value=mock.MagicMock(side_effect=lambda ctx: ctx),
        ),
        mock.patch(
            "ckanext.datapusher_plus.jobs.prefect_flow.FormatConverterStage",
            return_value=mock.MagicMock(side_effect=lambda ctx: ctx),
        ),
        mock.patch(
            "ckanext.datapusher_plus.jobs.prefect_flow.ValidationStage",
            return_value=mock.MagicMock(side_effect=lambda ctx: ctx),
        ),
        mock.patch(
            "ckanext.datapusher_plus.jobs.prefect_flow.AnalysisStage",
            return_value=mock.MagicMock(side_effect=lambda ctx: ctx),
        ),
        mock.patch(
            "ckanext.datapusher_plus.jobs.prefect_flow.DatabaseStage",
            return_value=mock.MagicMock(side_effect=lambda ctx: ctx),
        ),
        mock.patch(
            "ckanext.datapusher_plus.jobs.prefect_flow.IndexingStage",
            return_value=mock.MagicMock(side_effect=lambda ctx: ctx),
        ),
        mock.patch(
            "ckanext.datapusher_plus.jobs.prefect_flow.FormulaStage",
            return_value=mock.MagicMock(side_effect=lambda ctx: ctx),
        ),
        mock.patch(
            "ckanext.datapusher_plus.jobs.prefect_flow.MetadataStage",
            return_value=mock.MagicMock(side_effect=lambda ctx: ctx),
        ),
        mock.patch(
            "ckanext.datapusher_plus.jobs.prefect_flow.dsu.get_resource",
            return_value={"url_type": "upload", "format": "CSV", "url": "x.csv"},
        ),
        mock.patch(
            "ckanext.datapusher_plus.jobs.prefect_flow.dph.add_pending_job"
        ),
        mock.patch(
            "ckanext.datapusher_plus.jobs.prefect_flow.dph.set_aps_job_id"
        ),
        mock.patch(
            "ckanext.datapusher_plus.jobs.prefect_flow.dph.mark_job_as_completed"
        ),
        mock.patch(
            "ckanext.datapusher_plus.jobs.prefect_flow.dph.mark_job_as_errored"
        ),
        mock.patch(
            "ckanext.datapusher_plus.jobs.prefect_flow.dph.mark_job_as_failed_to_post_result"
        ),
        mock.patch(
            "ckanext.datapusher_plus.jobs.prefect_flow.utils.StoringHandler",
            return_value=logging.NullHandler(),
        ),
        mock.patch(
            "ckanext.datapusher_plus.jobs.prefect_flow.QSVCommand"
        ),
        mock.patch(
            "ckanext.datapusher_plus.jobs.prefect_flow.Path.is_file",
            return_value=True,
        ),
        mock.patch(
            "ckanext.datapusher_plus.jobs.prefect_flow.callback_datapusher_hook",
            return_value=True,
        ),
        mock.patch(
            "ckanext.datapusher_plus.jobs.prefect_flow.prefect_client.get_current_flow_run_id",
            return_value="flow-run-uuid",
        ),
    ]
    started = [p.start() for p in patches]
    # Indices match the patches list above. Using indexOf-by-target attribute
    # to stay robust if the list grows in the future.
    by_target = {p.attribute: started[i] for i, p in enumerate(patches) if hasattr(p, "attribute")}
    yield {
        "mark_completed": by_target.get("mark_job_as_completed"),
        "mark_errored": by_target.get("mark_job_as_errored"),
    }
    for p in patches:
        p.stop()


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_flow_completes_and_marks_job_complete(job_input, patched_dependencies):
    """A dry-run flow with all stages no-op'd should mark the job complete."""
    from ckanext.datapusher_plus.jobs.prefect_flow import datapusher_plus_flow

    # The flow is a Prefect @flow; calling it directly runs it in-process
    # without needing a server.
    result = datapusher_plus_flow(job_input)

    assert result is None  # contract: returns None on success
    patched_dependencies["mark_completed"].assert_called_once()
    patched_dependencies["mark_errored"].assert_not_called()


# ---------------------------------------------------------------------------
# Failure propagation
# ---------------------------------------------------------------------------


def test_flow_marks_errored_when_a_stage_raises(job_input, patched_dependencies):
    """A JobError raised by any stage should land in mark_job_as_errored."""
    from ckanext.datapusher_plus import utils
    from ckanext.datapusher_plus.jobs import prefect_flow

    # Make the database stage raise.
    prefect_flow.DatabaseStage.return_value.side_effect = utils.JobError(
        "fake db failure"
    )

    with pytest.raises(utils.JobError):
        prefect_flow.datapusher_plus_flow(job_input)

    patched_dependencies["mark_errored"].assert_called_once()
    args, _ = patched_dependencies["mark_errored"].call_args
    assert "fake db failure" in args[1]



# ---------------------------------------------------------------------------
# Transactional rollback
# ---------------------------------------------------------------------------


def test_rollback_drops_datastore_when_indexing_fails(job_input, patched_dependencies):
    """A failure inside the transaction fires database_task.on_rollback.

    Scenario: download/convert/validate/analyze/database all succeed; the
    indexing stage raises. Because all four destructive tasks run inside
    ``with transaction():``, the transaction rolls back and the database
    task's on_rollback hook drops the datastore table — provided this run
    created it from empty (``existing_info`` is falsy by default in the
    test fixture).
    """
    from unittest import mock

    from ckanext.datapusher_plus import utils
    from ckanext.datapusher_plus.jobs import prefect_flow

    # Make indexing raise *after* database has committed within the
    # transaction.
    prefect_flow.IndexingStage.return_value.side_effect = utils.JobError(
        "fake indexing failure"
    )

    job_input_real = prefect_flow.JobInput(
        task_id=job_input.task_id,
        resource_id=job_input.resource_id,
        ckan_url=job_input.ckan_url,
        input=job_input.input,
        dry_run=False,  # rollback only matters when we actually write
    )

    with mock.patch.object(
        prefect_flow.dsu, "delete_datastore_resource"
    ) as delete_ds:
        with pytest.raises(utils.JobError):
            prefect_flow.datapusher_plus_flow(job_input_real)

    # Database rollback hook must have been called with the resource_id.
    delete_ds.assert_called_once_with(job_input.resource_id)



# ---------------------------------------------------------------------------
# PII review suspension
# ---------------------------------------------------------------------------


def test_pii_review_rejection_raises_before_database_writes(
    job_input, patched_dependencies
):
    """When the PII threshold is exceeded and the reviewer rejects, the
    flow must raise BEFORE entering the transactional write group.
    """
    from unittest import mock

    from ckanext.datapusher_plus import utils
    from ckanext.datapusher_plus.jobs import prefect_flow

    # Configure the AnalysisStage mock to report two PII candidate
    # matches on the ProcessingContext that the task wrapper reads.
    def _set_pii(ctx):
        ctx.pii_found = True
        ctx.pii_candidate_count = 2
        ctx.headers_dicts = [
            {"id": "email", "type": "text"},
            {"id": "ssn", "type": "text"},
            {"id": "amount", "type": "numeric"},
        ]
        return ctx

    prefect_flow.AnalysisStage.return_value.side_effect = _set_pii

    rejection = mock.MagicMock(approve=False, reviewer="alice", notes="not ok")

    job_input_real = prefect_flow.JobInput(
        task_id=job_input.task_id,
        resource_id=job_input.resource_id,
        ckan_url=job_input.ckan_url,
        input=job_input.input,
        dry_run=False,
    )

    with mock.patch.object(
        prefect_flow, "_pii_review_threshold", return_value=1
    ), mock.patch(
        "prefect.flow_runs.suspend_flow_run", return_value=rejection
    ), mock.patch.object(
        prefect_flow.dsu, "delete_datastore_resource"
    ) as delete_ds:
        with pytest.raises(utils.JobError, match="PII review rejected"):
            prefect_flow.datapusher_plus_flow(job_input_real)

    # The database task should never have run, so its rollback should not
    # have fired either.
    delete_ds.assert_not_called()


def test_pii_review_approval_lets_flow_proceed(job_input, patched_dependencies):
    """Crossing the threshold then approving continues into the writes.

    Exercises the real gate: pii_candidate_count (2) > threshold (1)
    triggers suspend_flow_run; an approving response lets the flow
    complete normally.
    """
    from unittest import mock

    from ckanext.datapusher_plus.jobs import prefect_flow

    def _set_pii(ctx):
        ctx.pii_found = True
        ctx.pii_candidate_count = 2
        ctx.headers_dicts = [{"id": "email", "type": "text"}]
        return ctx

    prefect_flow.AnalysisStage.return_value.side_effect = _set_pii

    approval = mock.MagicMock(approve=True, reviewer="bob", notes="reviewed")

    job_input_real = prefect_flow.JobInput(
        task_id=job_input.task_id,
        resource_id=job_input.resource_id,
        ckan_url=job_input.ckan_url,
        input=job_input.input,
        dry_run=False,
    )

    with mock.patch.object(
        prefect_flow, "_pii_review_threshold", return_value=1
    ), mock.patch(
        "prefect.flow_runs.suspend_flow_run", return_value=approval
    ) as suspend:
        prefect_flow.datapusher_plus_flow(job_input_real)

    suspend.assert_called_once()
    patched_dependencies["mark_completed"].assert_called_once()


def test_pii_review_quick_screen_suspends_on_any_pii(
    job_input, patched_dependencies
):
    """In quick-screen mode the gate fires on PII presence alone.

    Quick screen reports a degenerate pii_candidate_count of 1, which no
    numeric threshold could ever exceed — so the gate must treat
    presence alone as crossing it when conf.PII_QUICK_SCREEN is True,
    with the threshold acting purely as the feature on/off switch.
    """
    from unittest import mock

    from ckanext.datapusher_plus.jobs import prefect_flow

    def _set_pii(ctx):
        ctx.pii_found = True
        ctx.pii_candidate_count = 1  # quick-screen's degenerate count
        ctx.headers_dicts = [{"id": "email", "type": "text"}]
        return ctx

    prefect_flow.AnalysisStage.return_value.side_effect = _set_pii
    approval = mock.MagicMock(approve=True, reviewer="bob", notes="ok")

    job_input_real = prefect_flow.JobInput(
        task_id=job_input.task_id,
        resource_id=job_input.resource_id,
        ckan_url=job_input.ckan_url,
        input=job_input.input,
        dry_run=False,
    )

    # threshold=5 would NOT be exceeded by count=1 in full-screen mode;
    # quick-screen must suspend anyway.
    with mock.patch.object(
        prefect_flow, "_pii_review_threshold", return_value=5
    ), mock.patch.object(
        prefect_flow.conf, "PII_QUICK_SCREEN", True
    ), mock.patch(
        "prefect.flow_runs.suspend_flow_run", return_value=approval
    ) as suspend:
        prefect_flow.datapusher_plus_flow(job_input_real)

    suspend.assert_called_once()
    patched_dependencies["mark_completed"].assert_called_once()


# ---------------------------------------------------------------------------
# Early exit on datastore-managed URLs
# ---------------------------------------------------------------------------


def test_flow_short_circuits_for_datastore_dumps(job_input):
    """``url_type == 'datastore'`` resources are completed without running stages."""
    from contextlib import ExitStack

    from ckanext.datapusher_plus.jobs import prefect_flow

    with ExitStack() as stack:
        stack.enter_context(
            mock.patch.object(
                prefect_flow.dsu,
                "get_resource",
                return_value={"url_type": "datastore"},
            )
        )
        stack.enter_context(mock.patch.object(prefect_flow.dph, "add_pending_job"))
        stack.enter_context(mock.patch.object(prefect_flow.dph, "set_aps_job_id"))
        mark_completed = stack.enter_context(
            mock.patch.object(prefect_flow.dph, "mark_job_as_completed")
        )
        stack.enter_context(
            mock.patch.object(
                prefect_flow.utils,
                "StoringHandler",
                return_value=logging.NullHandler(),
            )
        )
        stack.enter_context(
            mock.patch.object(prefect_flow.QSVCommand, "__init__", return_value=None)
        )
        stack.enter_context(
            mock.patch.object(prefect_flow.Path, "is_file", return_value=True)
        )
        stack.enter_context(
            mock.patch.object(
                prefect_flow, "callback_datapusher_hook", return_value=True
            )
        )
        stack.enter_context(
            mock.patch.object(
                prefect_flow.prefect_client,
                "get_current_flow_run_id",
                return_value="flow-run-uuid",
            )
        )
        result = prefect_flow.datapusher_plus_flow(job_input)

    assert result is None
    mark_completed.assert_called_once()


# ---------------------------------------------------------------------------
# Graceful stage abort (a stage returns None == "nothing to do")
# ---------------------------------------------------------------------------


def test_stage_run_raises_stage_abort_on_none():
    """A stage returning ``None`` surfaces as ``_StageAbort`` from ``_stage_run``.

    Per the BaseStage contract a stage may return ``None`` to stop the
    pipeline gracefully (e.g. Analysis on a zero-record file). ``_stage_run``
    converts that into the ``_StageAbort`` control-flow signal carrying the
    stage name.
    """
    from ckanext.datapusher_plus.jobs.prefect_flow import _StageAbort, _stage_run
    from ckanext.datapusher_plus.jobs.runtime_context import (
        reset_runtime_context,
        set_runtime_context,
    )

    fake_stage = mock.MagicMock()
    fake_stage.name = "Analysis"
    fake_stage.side_effect = lambda ctx: None  # graceful "nothing to do"

    token = set_runtime_context(mock.MagicMock())
    try:
        with pytest.raises(_StageAbort) as exc_info:
            _stage_run(fake_stage)
    finally:
        reset_runtime_context(token)

    assert exc_info.value.stage_name == "Analysis"


def test_retry_if_transient_classification():
    """``_retry_if_transient`` classifies each failure class correctly.

    * ``_StageAbort`` — control-flow signal, never retried (otherwise a
      retrying task re-runs the stage and hits the identical abort after
      a pointless backoff before the flow's handler can mark the job
      complete-with-skip).
    * deterministic ``JobError`` — never retried (re-running fails
      identically).
    * ``HTTPError`` — retried only when transient: a ``None`` status
      (connection/DNS/timeout) or a retryable server status; a
      deterministic 4xx is not. The download stage wraps *all* download
      failures in ``HTTPError``, so without this split the download
      task's retries would be a dead no-op.
    * anything else — retried.
    """
    from ckanext.datapusher_plus import job_exceptions, utils
    from ckanext.datapusher_plus.jobs.prefect_flow import (
        _StageAbort,
        _retry_if_transient,
    )

    def _state(exc):
        s = mock.MagicMock()
        s.result.side_effect = exc
        return s

    def _http(status):
        return job_exceptions.HTTPError(
            "boom", status_code=status, request_url="http://x", response=b""
        )

    # Not retried.
    assert _retry_if_transient(None, None, _state(_StageAbort("Analysis"))) is False
    assert _retry_if_transient(
        None, None, _state(utils.JobError("deterministic bad data"))
    ) is False
    assert _retry_if_transient(None, None, _state(_http(404))) is False
    assert _retry_if_transient(None, None, _state(_http(403))) is False

    # Retried.
    assert _retry_if_transient(None, None, _state(_http(None))) is True
    assert _retry_if_transient(None, None, _state(_http(503))) is True
    assert _retry_if_transient(None, None, _state(_http(429))) is True
    assert _retry_if_transient(
        None, None, _state(ConnectionError("network blip"))
    ) is True


def test_flow_marks_skipped_when_a_stage_aborts(job_input, patched_dependencies):
    """A stage returning ``None`` mid-flow completes the job, not errors it.

    The Analysis stage returns ``None`` (zero-record file). The flow must
    catch ``_StageAbort``, mark the job completed with ``{"skipped": <stage>}``,
    and never call ``mark_job_as_errored``.
    """
    from ckanext.datapusher_plus.jobs import prefect_flow

    prefect_flow.AnalysisStage.return_value.name = "Analysis"
    prefect_flow.AnalysisStage.return_value.side_effect = lambda ctx: None

    result = prefect_flow.datapusher_plus_flow(job_input)

    assert result is None  # graceful skip returns None, same as success
    patched_dependencies["mark_completed"].assert_called_once()
    args, _ = patched_dependencies["mark_completed"].call_args
    assert args[1] == {"skipped": "Analysis"}
    patched_dependencies["mark_errored"].assert_not_called()


# ---------------------------------------------------------------------------
# Result-chain rehydration (ProcessingContext retirement)
# ---------------------------------------------------------------------------


def test_rehydrate_reconstitutes_context_from_nested_results():
    """``rehydrate`` walks a nested result chain and repopulates the
    RuntimeContext root-first, so a downstream stage sees correct state
    even though no upstream *task body* ran in this process.

    This is the invariant that lets Prefect skip a cached / persisted
    task body without leaving the next stage reading empty context.
    """
    from ckanext.datapusher_plus.jobs.context import ProcessingContext
    from ckanext.datapusher_plus.jobs.runtime_context import (
        AnalyzeResult,
        ConvertResult,
        DownloadResult,
        ValidateResult,
        rehydrate,
    )

    download = DownloadResult(
        resource={"id": "r1", "format": "CSV"},
        resource_url="http://x/r1.csv",
        file_hash="abc123",
        content_length=4096,
        downloaded_path="/tmp/run/r1.csv",
    )
    convert = ConvertResult(upstream=download, csv_path="/tmp/run/r1.converted.csv")
    validate = ValidateResult(
        upstream=convert,
        csv_path="/tmp/run/r1.clean.csv",
        rows_after_dedup=42,
        quarantined_rows=3,
        quarantine_csv_path="/tmp/run/r1.errors.csv",
    )
    analyze = AnalyzeResult(
        upstream=validate,
        csv_path="/tmp/run/r1.clean.csv",
        headers=["a", "b"],
        headers_dicts=[{"id": "a"}, {"id": "b"}],
        original_header_dict={0: "A", 1: "B"},
        dataset_stats={"rows": 42},
        resource_fields_stats={"a": {}},
        resource_fields_freqs={"a": {}},
        pii_found=True,
        pii_candidate_count=2,
    )

    # A bare context, as _build_runtime_context hands to the first task:
    # only build-time fields are set, everything else is at its default.
    ctx = ProcessingContext(task_id="t1", input={})

    rehydrate(ctx, analyze)

    # Root DownloadResult fields.
    assert ctx.resource == {"id": "r1", "format": "CSV"}
    assert ctx.resource_url == "http://x/r1.csv"
    assert ctx.file_hash == "abc123"
    assert ctx.content_length == 4096
    # Working path: each layer overwrites it, analysis' value wins.
    assert ctx.tmp == "/tmp/run/r1.clean.csv"
    # ValidateResult fields.
    assert ctx.rows_to_copy == 42
    assert ctx.quarantined_rows == 3
    assert ctx.quarantine_csv_path == "/tmp/run/r1.errors.csv"
    # AnalyzeResult fields.
    assert ctx.headers == ["a", "b"]
    assert ctx.headers_dicts == [{"id": "a"}, {"id": "b"}]
    assert ctx.pii_found is True
    assert ctx.pii_candidate_count == 2
    # The file_hash property walks the chain back to the root.
    assert analyze.file_hash == "abc123"
    assert validate.file_hash == "abc123"
    assert convert.file_hash == "abc123"


def test_stage_run_rehydrates_context_from_prev():
    """``_stage_run(stage, prev)`` applies ``prev`` onto the bound context
    *before* invoking the stage — the cache-hit safety net.

    Simulates a skipped upstream task body (cache hit / persisted-result
    replay): the only way the stage gets correct state is rehydration.
    """
    from ckanext.datapusher_plus.jobs import prefect_flow
    from ckanext.datapusher_plus.jobs.context import ProcessingContext
    from ckanext.datapusher_plus.jobs.runtime_context import (
        DownloadResult,
        reset_runtime_context,
        set_runtime_context,
    )

    prev = DownloadResult(
        resource={"id": "r1"},
        resource_url="http://x/r1.csv",
        file_hash="hash-xyz",
        content_length=10,
        downloaded_path="/tmp/run/r1.csv",
    )
    # Bare context — as if the download task body was skipped.
    ctx = ProcessingContext(task_id="t1", input={})
    seen = {}

    def _stage(c):
        # The stage must observe a rehydrated context, not the bare one.
        seen["tmp"] = c.tmp
        seen["file_hash"] = c.file_hash
        return c

    stage = mock.MagicMock(side_effect=_stage)
    token = set_runtime_context(ctx)
    try:
        prefect_flow._stage_run(stage, prev)
    finally:
        reset_runtime_context(token)

    assert seen["tmp"] == "/tmp/run/r1.csv"
    assert seen["file_hash"] == "hash-xyz"



# ---------------------------------------------------------------------------
# _bootstrap_ckan_app_context — no-op guards
# ---------------------------------------------------------------------------
#
# Covers the roborev follow-up "No dedicated unit test for
# `_bootstrap_ckan_app_context`'s no-op guards" and the new
# `DPP_PREFECT_WORKER` sentinel guard — the latter is what lets pytest
# (and the CKAN web app, ad-hoc tooling, etc.) import `prefect_flow`
# safely, since the bootstrap no-ops outside a genuine worker subprocess.


def test_bootstrap_noops_when_worker_sentinel_unset():
    """No ``DPP_PREFECT_WORKER=1`` → ``_bootstrap_ckan_app_context``
    returns silently without calling ``make_app``. This is the guard
    that lets pytest import ``prefect_flow`` without bringing up a full
    CKAN stack at module import.

    Forces guards #1 (Flask app context) and #2 (``ckan.common.config``
    populated) to NOT fire so the assertion exercises guard #3 even if
    an earlier test (or a future conftest) leaves either of the other
    guards primed.
    """
    import os
    from ckan.common import config as ckan_config
    from ckanext.datapusher_plus.jobs.prefect_flow import (
        _bootstrap_ckan_app_context,
    )

    with mock.patch.dict(
        os.environ, {"DPP_PREFECT_WORKER": "0"}, clear=False
    ), mock.patch("flask.has_app_context", return_value=False), mock.patch.object(
        ckan_config, "get", return_value=None
    ), mock.patch("ckan.config.middleware.make_app") as mk_make_app:
        _bootstrap_ckan_app_context()
    mk_make_app.assert_not_called()


def test_bootstrap_noops_when_app_context_present():
    """Existing guard #1: a Flask app context is already pushed (the
    CKAN web process imported the plugin) → bootstrap is a no-op even
    when the worker sentinel is set.
    """
    import os
    from ckanext.datapusher_plus.jobs.prefect_flow import (
        _bootstrap_ckan_app_context,
    )

    with mock.patch.dict(
        os.environ, {"DPP_PREFECT_WORKER": "1"}, clear=False
    ), mock.patch("flask.has_app_context", return_value=True), mock.patch(
        "ckan.config.middleware.make_app"
    ) as mk_make_app:
        _bootstrap_ckan_app_context()
    mk_make_app.assert_not_called()


def test_bootstrap_noops_when_ckan_config_populated():
    """Existing guard #2: ``ckan.common.config`` is already populated
    (a ``ckan`` CLI command already ran ``make_app``) → bootstrap is a
    no-op even when the worker sentinel is set.
    """
    import os
    from ckan.common import config as ckan_config
    from ckanext.datapusher_plus.jobs.prefect_flow import (
        _bootstrap_ckan_app_context,
    )

    with mock.patch.dict(
        os.environ, {"DPP_PREFECT_WORKER": "1"}, clear=False
    ), mock.patch("flask.has_app_context", return_value=False), mock.patch.object(
        ckan_config, "get", return_value="http://example.test"
    ), mock.patch("ckan.config.middleware.make_app") as mk_make_app:
        _bootstrap_ckan_app_context()
    mk_make_app.assert_not_called()
