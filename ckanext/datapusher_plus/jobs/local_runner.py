# -*- coding: utf-8 -*-
# flake8: noqa: E501
"""
In-process ingestion runner for ``prefect_enabled = false``.

This is the fallback path DataPusher+ takes when an operator turns
Prefect off in ``ckan.ini``::

    ckanext.datapusher_plus.prefect_enabled = false

``datapusher_submit`` then enqueues :func:`run_job` on CKAN's own RQ
background queue instead of creating a Prefect flow run, and a plain
``ckan jobs worker`` executes the same nine stage classes the Prefect
flow runs — sequentially, in one process, sharing one
``RuntimeContext``. No Prefect server, no Prefect worker, no Prefect
work pool, and — importantly — no ``import prefect`` anywhere in the
path: on a host where ``$PREFECT_HOME`` is unwritable, that import is
itself the failure (``PermissionError: '/root/.prefect/profiles.toml'``).

What is identical to the Prefect flow:

* the nine stages and the order they run in;
* the ``Jobs``-row state machine (pending → completed / errored) and the
  ``Logs`` table written through ``utils.StoringHandler``;
* the ``datapusher_hook`` callbacks (running / complete / error) that
  drive default views, ``IDataPusher.after_upload``, and auto-resubmit;
* the "stage returned ``None``" complete-with-skip contract;
* the soft between-stage deadline from
  ``ckanext.datapusher_plus.flow_timeout``;
* dropping the datastore table (and restoring a stashed Data Dictionary)
  when a stage after the database load fails.

What Prefect gives you that this does not — the reason it stays the
default:

* per-task retries and backoff (a flaky download fails the job here);
* task result caching / re-run-from-failed-stage;
* the run graph, artifacts, and ``datapusher.*`` events;
* human-in-the-loop PII review — ``pii_review_threshold`` cannot suspend
  a job here, so crossing the gate aborts before any datastore write
  rather than waiting for an approval that can never arrive;
* per-stage rollback granularity: when the *database* stage itself
  raises, this runner leaves the datastore alone rather than dropping a
  table it may never have touched (a failure to even connect must not
  destroy intact data). Cleanup happens for failures *after* the load,
  which is where the table is known to be half-built.

Retry policy is therefore RQ's: the job fails, and the operator (or
``ckan datapusher_plus resubmit``) submits it again.
"""

from __future__ import annotations

import logging
import sys
import tempfile
import time
import traceback
from dataclasses import asdict
from typing import Any, Dict, Optional, Set, Union

import sqlalchemy as sa

import ckan.plugins.toolkit as tk

import ckanext.datapusher_plus.config as conf
import ckanext.datapusher_plus.helpers as dph
import ckanext.datapusher_plus.utils as utils
from ckanext.datapusher_plus import __version__ as _dpp_version
import ckanext.datapusher_plus.dictionary_stash as dict_stash
from ckanext.datapusher_plus.jobs import quarantine
from ckanext.datapusher_plus.jobs.pipeline_core import (
    StageAbort,
    build_runtime_context,
    callback_datapusher_hook,
    resolve_int,
    resource_is_datastore_dump,
    rollback_datastore_writes,
    run_stage,
    validate_input,
)
from ckanext.datapusher_plus.jobs.runtime_context import (
    JobInput,
    reset_runtime_context,
    set_runtime_context,
)
from ckanext.datapusher_plus.jobs.stages.ai_suggestions import AISuggestionsStage
from ckanext.datapusher_plus.jobs.stages.analysis import AnalysisStage
from ckanext.datapusher_plus.jobs.stages.database import DatabaseStage
from ckanext.datapusher_plus.jobs.stages.download import DownloadStage
from ckanext.datapusher_plus.jobs.stages.format_converter import FormatConverterStage
from ckanext.datapusher_plus.jobs.stages.formula import FormulaStage
from ckanext.datapusher_plus.jobs.stages.indexing import IndexingStage
from ckanext.datapusher_plus.jobs.stages.metadata import MetadataStage
from ckanext.datapusher_plus.jobs.stages.validation import ValidationStage

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Submission
# ---------------------------------------------------------------------------


def enqueue_job(job_input: JobInput, *, timeout: Optional[int] = None) -> str:
    """Enqueue an ingestion on CKAN's RQ background queue.

    The local-mode counterpart of ``prefect_client.submit_flow_run``:
    non-blocking, returns as soon as Redis has accepted the job. A
    ``ckan jobs worker`` (CKAN's built-in worker — no extra service)
    picks it up and calls :func:`run_job`.

    The payload is the ``asdict``-ed ``JobInput``, i.e. exactly what the
    Prefect deployment receives as its ``job_input`` parameter, so both
    paths carry the same data contract.

    Args:
        job_input: the job to run.
        timeout: job timeout in seconds, handed to RQ. Pass one:
            ``None`` leaves RQ's own default (180s) in place, which
            kills any ingestion of consequence mid-COPY.
            ``datapusher_submit`` passes
            ``ckanext.datapusher_plus.flow_timeout`` here, the same
            value the runner enforces between stages.

    Returns:
        The RQ job id, which the caller records on the CKAN
        ``task_status`` row.

    Raises:
        Whatever ``enqueue_job`` raises when Redis is unreachable —
        the caller surfaces it exactly as it does a Prefect failure.
    """
    rq_kwargs: Dict[str, Any] = {}
    if timeout is not None:
        rq_kwargs["timeout"] = timeout

    job = tk.enqueue_job(
        run_job,
        [asdict(job_input)],
        title=f"DataPusher+ ingest {job_input.resource_id}",
        rq_kwargs=rq_kwargs or None,
    )
    return str(job.id)


def get_running_resource_ids() -> Set[str]:
    """Return the set of CKAN resource_ids currently being ingested.

    The local-mode counterpart of
    ``prefect_client.get_running_resource_ids``: scans CKAN's RQ queue
    (both waiting and started jobs) for DP+ ingestions and pulls
    ``resource_id`` out of each one's payload. ``datapusher_submit`` uses
    it to skip duplicate submissions of a resource that is already
    in-flight.

    Unlike the v2 regex-over-``job.description`` scan this reads the
    enqueued payload directly, so it cannot be defeated by a change in
    how RQ renders a job's description.

    Any error is logged and treated as "nothing running" — same
    fail-open behaviour as the Prefect implementation when its server is
    unreachable, so an unavailable Redis never blocks a submission.
    """
    ids: Set[str] = set()
    try:
        import ckan.lib.jobs as rq_jobs

        queue = rq_jobs.get_queue()
        jobs = list(queue.get_jobs())

        # Waiting jobs only come from ``queue.get_jobs()``; a job that a
        # worker already picked up has moved to the started registry and
        # is exactly the case we most need to catch.
        try:
            from rq.registry import StartedJobRegistry

            registry = StartedJobRegistry(queue=queue)
            for job_id in registry.get_job_ids():
                job = queue.fetch_job(job_id)
                if job is not None:
                    jobs.append(job)
        except Exception as e:
            log.warning("Could not read RQ started-job registry: %s", e)

        for job in jobs:
            args = job.args or ()
            payload = args[0] if args else None
            if isinstance(payload, dict) and payload.get("resource_id"):
                ids.add(payload["resource_id"])
    except Exception as e:
        log.warning("Failed to query RQ for running resource_ids: %s", e)
        return set()
    return ids


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------


def _current_rq_job_id() -> Optional[str]:
    """The id of the RQ job we are executing inside, if any."""
    try:
        from rq import get_current_job

        job = get_current_job()
        return str(job.id) if job is not None else None
    except Exception:
        return None


def _guard_pii_review(runtime) -> None:
    """Abort before any datastore write when the PII review gate is crossed.

    ``ckanext.datapusher_plus.pii_review_threshold`` asks for a human to
    approve an ingestion whose analysis flagged PII. The Prefect flow
    honours that by suspending the run until an operator answers a form
    in the Prefect UI; with Prefect off there is nothing that can hold a
    job open and nothing to answer on, so the only reading of the
    operator's intent that stays safe is "do not load it".

    No-op when the feature is off (threshold ``0``, the default) or when
    analysis found no PII.
    """
    threshold = resolve_int(
        "DATAPUSHER_PLUS_PII_REVIEW_THRESHOLD",
        "ckanext.datapusher_plus.pii_review_threshold",
        0,
    )
    if threshold <= 0 or not runtime.pii_found:
        return

    pii_count = runtime.pii_candidate_count
    # Quick-screen mode only detects PII *presence* — pii_candidate_count
    # is a degenerate 1 that no numeric threshold could ever exceed, so
    # there the threshold acts purely as the feature's on/off switch.
    if not conf.PII_QUICK_SCREEN and pii_count <= threshold:
        return

    raise utils.JobError(
        f"PII review gate crossed: {pii_count} candidate match(es) "
        f"(threshold={threshold}). Human-in-the-loop review requires "
        "Prefect, which is disabled "
        "(ckanext.datapusher_plus.prefect_enabled = false), so the job "
        "was aborted before any datastore write. Re-enable Prefect to "
        "review and approve these runs, or set "
        "ckanext.datapusher_plus.pii_review_threshold = 0 to load them "
        "without review."
    )


def run_job(job_input: Union[JobInput, Dict[str, Any]]) -> Optional[str]:
    """Ingest one CKAN resource into the datastore, in this process.

    The Prefect-free twin of ``datapusher_plus_flow``, and the callable
    RQ invokes for every locally-run job.

    Returns ``None`` on success — matching the flow's return contract —
    and re-raises on failure so RQ records the job as failed after the
    ``Jobs`` row and the CKAN callback have been updated.
    """
    # RQ hands back exactly what was enqueued (a dict); accept the
    # dataclass too so callers and tests can drive this directly.
    if isinstance(job_input, dict):
        job_input = JobInput(**job_input)

    job_id = job_input.task_id
    log.info(
        f"DATAPUSHER+ v{_dpp_version} starting local (no-Prefect) job for "
        f"resource {job_input.resource_id}"
    )

    # Register the job in the DP+ Jobs table at start. This is what
    # ``datapusher_status`` and the CKAN UI read.
    try:
        dph.add_pending_job(job_id, **job_input.input)
    except sa.exc.IntegrityError:
        raise utils.JobError("Job already exists.")
    # Column repurposed for the orchestrator's own run id — the Prefect
    # flow stores its flow_run_id here, we store the RQ job id.
    dph.set_aps_job_id(job_id, _current_rq_job_id() or "local")

    # Validate the input only after the Jobs row exists, so a malformed
    # submission is recorded as an errored job (visible via
    # datapusher_status / the CKAN UI) instead of raising with no trace.
    try:
        validate_input(job_input.input)
    except utils.JobError as e:
        dph.mark_job_as_errored(job_id, str(e))
        raise

    job_timeout_seconds = resolve_int(
        "DATAPUSHER_PLUS_FLOW_TIMEOUT_SECONDS",
        "ckanext.datapusher_plus.flow_timeout",
        7200,
    )
    job_start = time.monotonic()

    def _check_deadline():
        """Raise if the configured deadline has been exceeded.

        Soft enforcement, checked between stages only: a stage hung
        mid-execution is RQ's ``timeout`` to kill (set from the same
        config value at enqueue time), not this check's.
        """
        elapsed = time.monotonic() - job_start
        if elapsed > job_timeout_seconds:
            raise utils.JobError(
                f"Job exceeded configured timeout of "
                f"{job_timeout_seconds}s (elapsed: {elapsed:.0f}s)"
            )

    # Announce running state to CKAN.
    result_url = job_input.input.get("result_url")
    if result_url:
        callback_datapusher_hook(
            result_url=result_url,
            job_dict={
                "metadata": job_input.input.get("metadata", {}),
                "status": "running",
            },
        )

    errored = False
    with tempfile.TemporaryDirectory() as temp_dir:
        # ``runtime`` / ``token`` are built *inside* the try so that a
        # failure in build_runtime_context (e.g. get_resource raising) is
        # caught: mark_job_as_errored runs and the error callback fires,
        # instead of the exception escaping and leaving the job stuck
        # "running" (set by the announce callback above).
        runtime = None
        token = None
        try:
            runtime = build_runtime_context(job_input, temp_dir)
            token = set_runtime_context(runtime)

            if resource_is_datastore_dump(runtime):
                runtime.logger.info("Dump files are managed with the Datastore API")
                dph.mark_job_as_completed(job_id, {"skipped": "datastore-managed"})
                _log_done(runtime, "(skipped: datastore-managed) ")
                return None

            # Read-only / non-destructive stages. Each mutates the one
            # live RuntimeContext, so — unlike the Prefect tasks, whose
            # bodies may be skipped by a cache hit — no result needs to
            # be threaded from stage to stage.
            run_stage(DownloadStage())
            _check_deadline()
            run_stage(FormatConverterStage())
            run_stage(ValidationStage())

            # Enforce the quarantine threshold (raises if exceeded).
            # No-op when no rows were rejected.
            quarantine.apply_quarantine(
                resource_id=runtime.resource_id,
                clean_csv_path=runtime.tmp,
                quarantine_csv_path=runtime.quarantine_csv_path or None,
                quarantined_rows=runtime.quarantined_rows,
                total_rows=runtime.rows_to_copy + runtime.quarantined_rows,
            )

            run_stage(AnalysisStage())
            _check_deadline()

            # Optional AI suggestions, before the datastore writes so a
            # (rare) failure to patch the package can't poison the
            # rollback path below. Gated on
            # ``ckanext.datapusher_plus.enable_ai_suggestions`` (default
            # False) and swallows every internal failure.
            run_stage(AISuggestionsStage())
            _check_deadline()

            _guard_pii_review(runtime)

            # Datastore-mutating group. A failure *after* the database
            # stage means the table is half-built: drop it (and restore
            # any stashed Data Dictionary), which is what the Prefect
            # flow's transaction rollback does. A failure *in* the
            # database stage is deliberately left alone — see the module
            # docstring.
            run_stage(DatabaseStage())
            _check_deadline()
            try:
                run_stage(IndexingStage())
                run_stage(FormulaStage())
                run_stage(MetadataStage())
            except Exception:
                # Catches ``StageAbort`` too — one of these stages
                # returning ``None`` after the load has committed leaves
                # the same half-finished table as a raise, and under
                # Prefect it would likewise fail the transaction and
                # fire the rollback hooks. The outer handler still marks
                # the job complete-with-skip.
                rollback_datastore_writes(runtime)
                raise

            if job_input.dry_run:
                dph.mark_job_as_completed(job_id, {"headers": runtime.headers_dicts})
                _log_done(runtime, "(dry-run) ")
                return None

            if runtime.quarantined_rows > 0:
                runtime.logger.warning(
                    f"{runtime.quarantined_rows} row(s) quarantined to "
                    f"{runtime.quarantine_csv_path}"
                )

            dph.mark_job_as_completed(
                job_id,
                {
                    "rows": runtime.copied_count,
                    "headers": runtime.headers_dicts,
                },
            )
            _log_done(runtime)
            return None

        except StageAbort as e:
            # A stage signalled "nothing to do" by returning None (e.g.
            # the Analysis stage on a zero-record file). v2 stopped the
            # pipeline here and the job *completed* — there was simply
            # nothing to load. Match that: complete-with-skip, not an
            # error. ``errored`` stays False so the finally block fires
            # the "complete" callback.
            if runtime is not None:
                runtime.logger.info(str(e))
            log.info(str(e))
            dph.mark_job_as_completed(job_id, {"skipped": e.stage_name})
            _log_done(runtime, f"(skipped: {e.stage_name}) ")
            return None
        except utils.JobError as e:
            errored = True
            dph.mark_job_as_errored(job_id, str(e))
            if runtime is not None:
                runtime.logger.error(f"DataPusher Plus error: {e}")
            log.error(f"DataPusher Plus error: {e}")
            raise
        except Exception as e:
            errored = True
            tb = traceback.format_tb(sys.exc_info()[2])[-1] + repr(e)
            dph.mark_job_as_errored(job_id, tb)
            if runtime is not None:
                runtime.logger.error(
                    f"DataPusher Plus error: {e}, {traceback.format_exc()}"
                )
            log.error(f"DataPusher Plus error: {e}")
            raise
        finally:
            if token is not None:
                reset_runtime_context(token)
            # Issue #265: on any successful exit (including the
            # ``StageAbort`` complete-with-skip), drop the Data
            # Dictionary stash — the run reached its natural end. On
            # failure, leave the stash for the rollback path (and a
            # possible resubmission).
            if not errored:
                try:
                    dict_stash.clear(job_input.resource_id)
                except Exception as e:  # noqa: BLE001 — never block teardown on stash cleanup
                    log.warning(
                        f"Could not clear dictionary stash for "
                        f"{job_input.resource_id}: {e}"
                    )
            if result_url:
                status = "error" if errored else "complete"
                saved_ok = callback_datapusher_hook(
                    result_url=result_url,
                    job_dict={
                        "metadata": job_input.input.get("metadata", {}),
                        "status": status,
                    },
                )
                if not saved_ok and not errored:
                    dph.mark_job_as_failed_to_post_result(job_id)


def _log_done(runtime, marker: str = "") -> None:
    """Emit the "JOB DONE!" capstone (issue #111) on every success path."""
    if runtime is None:
        return
    total_elapsed = time.time() - runtime.timer_start
    runtime.logger.info(
        f"DATAPUSHER+ v{_dpp_version} JOB DONE! {marker}"
        f"Total elapsed time: {total_elapsed:,.2f} seconds."
    )
