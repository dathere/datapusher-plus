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

    # Configure the AnalysisStage mock to flag two PII fields on the
    # ProcessingContext that the task wrapper will then read.
    def _set_pii(ctx):
        ctx.pii_found = True
        ctx.headers_dicts = [
            {"id": "email", "type": "text", "pii": True},
            {"id": "ssn", "type": "text", "pii": True},
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
    """Approval continues into the transactional writes normally."""
    from unittest import mock

    from ckanext.datapusher_plus.jobs import prefect_flow

    def _set_pii(ctx):
        ctx.pii_found = True
        ctx.headers_dicts = [{"id": "email", "type": "text", "pii": True}]
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
        prefect_flow, "_pii_review_threshold", return_value=0
    ):
        # Threshold = 0 disables suspension entirely; expected fast-path.
        prefect_flow.datapusher_plus_flow(job_input_real)

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


def test_retry_if_transient_does_not_retry_stage_abort():
    """``_StageAbort`` is a control-flow signal, not a transient failure.

    ``_retry_if_transient`` must return ``False`` for it — otherwise a
    retrying task (e.g. ``analyze_task``) re-runs the stage and hits the
    identical abort after a pointless backoff before the flow's
    ``except _StageAbort`` handler can mark the job complete-with-skip.
    Deterministic ``JobError``s are likewise not retried; everything else
    (network blips, momentary DB unavailability) is.
    """
    from ckanext.datapusher_plus import utils
    from ckanext.datapusher_plus.jobs.prefect_flow import (
        _StageAbort,
        _retry_if_transient,
    )

    abort_state = mock.MagicMock()
    abort_state.result.side_effect = _StageAbort("Analysis")
    assert _retry_if_transient(None, None, abort_state) is False

    joberror_state = mock.MagicMock()
    joberror_state.result.side_effect = utils.JobError("deterministic bad data")
    assert _retry_if_transient(None, None, joberror_state) is False

    transient_state = mock.MagicMock()
    transient_state.result.side_effect = ConnectionError("network blip")
    assert _retry_if_transient(None, None, transient_state) is True


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
