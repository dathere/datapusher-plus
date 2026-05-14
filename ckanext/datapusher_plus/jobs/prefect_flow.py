# -*- coding: utf-8 -*-
"""
Prefect-orchestrated data ingestion flow for DataPusher+.

This module is the v3.0 replacement for the v2 ``DataProcessingPipeline``
loop in ``jobs/pipeline.py``. It exposes:

* Eight module-level ``@task`` functions, one per ingestion stage. Each
  delegates to the unchanged ``BaseStage.process()`` body so that the
  hundreds of lines of stage logic carry across without rewrite. Per-task
  retries, timeouts, and tags are declared on the decorator — the
  resilience contract is visible at a glance.

* ``datapusher_plus_flow`` — the entry-point ``@flow``. Builds the
  ``RuntimeContext`` once, binds it via a ``ContextVar``, runs the tasks
  in order, wraps the datastore-mutating group in ``with transaction()``
  for atomic rollback, and owns the DP+ ``Jobs`` row's state transitions
  in a ``try/finally``. The HTTP callback to ``datapusher_hook`` fires in
  the ``finally`` so post-completion CKAN logic (default views, plugin
  hooks, auto-resubmit) keeps working identically to v2.

Operators can register a custom flow via
``ckanext.datapusher_plus.prefect_flow``; this file's ``@task`` functions
are the public composable primitives for that customization.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import tempfile
import time
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# CKAN app-context bootstrap (runs at module import)
# ---------------------------------------------------------------------------
#
# When this module is imported inside a Prefect worker subprocess (the
# ``process`` worker spawns a fresh Python interpreter for each flow run),
# CKAN's normal startup hasn't happened — no Flask app, no ``tk.config``.
# DP+'s ``config.py`` reads its module-level constants via ``tk.config.get``,
# which returns ``None`` for everything in that context, and ``Path(None)``
# crashes the import.
#
# Bootstrap CKAN here by loading the ini pointed to by ``CKAN_INI``, building
# a Flask app stack, and pushing its application context. After this,
# subsequent ``tk.config.get(...)`` calls return the real values.
#
# When ``CKAN_INI`` is unset or we're already inside an app context (e.g.,
# this module is being imported from the CKAN web process), the bootstrap
# is a no-op.


def _bootstrap_ckan_app_context() -> None:
    ini = os.environ.get("CKAN_INI")
    if not ini or not os.path.exists(ini):
        return
    try:
        from flask import has_app_context

        if has_app_context():
            return
    except Exception:
        # If flask isn't importable we have bigger problems; let the
        # downstream DP+ imports surface the real error.
        return
    try:
        from ckan.cli import load_config
        from ckan.config.middleware import make_flask_stack

        cfg = load_config(ini)
        app = make_flask_stack(cfg)
        app.app_context().push()
    except Exception as e:
        logging.getLogger(__name__).warning(
            "CKAN config bootstrap failed in Prefect worker subprocess: %s. "
            "DP+ will fall back to env-var defaults where available.",
            e,
        )


_bootstrap_ckan_app_context()


# ---------------------------------------------------------------------------
# DP+ imports (CKAN context is now available)
# ---------------------------------------------------------------------------


import requests
import sqlalchemy as sa
from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.transactions import transaction

import ckanext.datapusher_plus.config as conf
import ckanext.datapusher_plus.datastore_utils as dsu
import ckanext.datapusher_plus.helpers as dph
import ckanext.datapusher_plus.prefect_client as prefect_client
import ckanext.datapusher_plus.utils as utils
from ckanext.datapusher_plus.jobs import artifacts, events, quarantine
from ckanext.datapusher_plus.jobs.caching import (
    CONTENT_CACHE_POLICY,
    DEFAULT_CACHE_EXPIRATION,
    DEFAULT_RESULT_STORAGE,
    DOWNLOAD_CACHE_POLICY,
    content_cache_key,
    download_cache_key,
)
from ckanext.datapusher_plus.jobs.context import ProcessingContext
from ckanext.datapusher_plus.jobs.runtime_context import (
    AnalyzeResult,
    ConvertResult,
    DatabaseResult,
    DownloadResult,
    FormulaResult,
    IndexingResult,
    JobInput,
    MetadataResult,
    RuntimeContext,
    ValidateResult,
    get_runtime_context,
    reset_runtime_context,
    set_runtime_context,
)
from ckanext.datapusher_plus.jobs.stages.analysis import AnalysisStage
from ckanext.datapusher_plus.jobs.stages.database import DatabaseStage
from ckanext.datapusher_plus.jobs.stages.download import DownloadStage
from ckanext.datapusher_plus.jobs.stages.format_converter import FormatConverterStage
from ckanext.datapusher_plus.jobs.stages.formula import FormulaStage
from ckanext.datapusher_plus.jobs.stages.indexing import IndexingStage
from ckanext.datapusher_plus.jobs.stages.metadata import MetadataStage
from ckanext.datapusher_plus.jobs.stages.validation import ValidationStage
from ckanext.datapusher_plus.logging_utils import TRACE
from ckanext.datapusher_plus.qsv_utils import QSVCommand


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
#
# Worker processes may run in environments where CKAN's config object is
# not yet initialized. We read tunables from environment variables with
# CKAN config as a fallback so the flow is launchable from a pure
# ``prefect worker`` process.


def _env_int(name: str, default: int) -> int:
    """Read an env-var-only int. Used for env-only knobs like retry counts."""
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _resolve_int(env_name: str, config_key: str, default: int) -> int:
    """Resolve an int tunable from env var → CKAN config → default.

    Env var wins so operators and CI can override per-process without
    touching ``ckan.ini``. When the env var is unset, fall back to the
    CKAN config key — useful for operators who manage all settings via
    ``ckan.ini``. When neither is set, return ``default``.
    """
    env_value = os.environ.get(env_name)
    if env_value is not None and env_value != "":
        try:
            return int(env_value)
        except ValueError:
            pass
    try:
        import ckan.plugins.toolkit as tk

        v = tk.config.get(config_key)
        if v is not None and v != "":
            return int(v)
    except Exception:
        # CKAN config not loaded (e.g., when running in a bare
        # ``prefect worker`` process) — fall through to the default.
        pass
    return default


_FLOW_TIMEOUT_SECONDS = _resolve_int(
    "DATAPUSHER_PLUS_FLOW_TIMEOUT_SECONDS",
    "ckanext.datapusher_plus.flow_timeout",
    7200,
)
_TASK_RETRY_DOWNLOAD = _resolve_int(
    "DATAPUSHER_PLUS_DOWNLOAD_RETRIES",
    "ckanext.datapusher_plus.download_retries",
    3,
)
_TASK_RETRY_DATABASE = _resolve_int(
    "DATAPUSHER_PLUS_DATABASE_RETRIES",
    "ckanext.datapusher_plus.database_retries",
    2,
)


# ---------------------------------------------------------------------------
# Callback helper (moved from pipeline.py)
# ---------------------------------------------------------------------------


def callback_datapusher_hook(result_url: str, job_dict: Dict[str, Any]) -> bool:
    """
    POST a status update to CKAN's ``datapusher_hook`` endpoint.

    Preserves the v2 contract: the worker reports running/complete/error
    state by POSTing here, which drives default-view creation, plugin
    ``IDataPusher.after_upload`` hooks, and auto-resubmit on file change.
    """
    api_token = utils.get_dp_plus_user_apitoken()
    headers = {
        "Content-Type": "application/json",
        "Authorization": api_token,
    }
    try:
        response = requests.post(
            result_url,
            data=json.dumps(job_dict, cls=utils.DatetimeJsonEncoder),
            verify=conf.SSL_VERIFY,
            headers=headers,
            timeout=30,
        )
    except requests.ConnectionError:
        return False
    return response.status_code == requests.codes.ok


# ---------------------------------------------------------------------------
# Stage tasks
# ---------------------------------------------------------------------------
#
# Each task:
#   * Reads shared mutable state from ``RuntimeContext`` (the v2
#     ``ProcessingContext``) via the ContextVar binding.
#   * Delegates the actual work to the unchanged stage class.
#   * Returns a small typed result so the Prefect run graph is meaningful
#     and downstream tasks declare their dependencies explicitly.
#
# Retries are tuned per failure mode: I/O-bound tasks retry with backoff;
# deterministic ones (validation, formula) have ``retries=0`` because a
# retry would fail identically.


def _stage_run(stage) -> RuntimeContext:
    """Invoke a stage on the bound RuntimeContext, propagating None-aborts."""
    ctx = get_runtime_context()
    result = stage(ctx)
    if result is None:
        raise utils.JobError(f"Stage {stage.name} aborted the pipeline")
    return result


@task(
    name="download",
    retries=_TASK_RETRY_DOWNLOAD,
    retry_delay_seconds=[10, 60, 300],
    tags=["datapusher-plus", "io-bound"],
    # Persistence: every output is checkpointed so a re-run from the
    # Prefect UI replays only the failed and downstream tasks.
    persist_result=True,
    result_storage=DEFAULT_RESULT_STORAGE,
    # Cross-run caching keyed on (resource_id, URL) when ignore_hash is
    # False. ``DOWNLOAD_CACHE_POLICY`` composes our custom key with
    # ``TASK_SOURCE`` so a DP+ upgrade that changes this task's body
    # invalidates stale caches automatically.
    cache_policy=DOWNLOAD_CACHE_POLICY,
    cache_expiration=DEFAULT_CACHE_EXPIRATION,
)
def download_task(job_input: JobInput) -> DownloadResult:
    """Fetch the resource file. Idempotent w.r.t. (URL, file_hash)."""
    ctx = _stage_run(DownloadStage())
    return DownloadResult(
        resource=ctx.resource,
        resource_url=ctx.resource_url,
        file_hash=ctx.file_hash,
        content_length=ctx.content_length,
        downloaded_path=ctx.tmp,
    )


@task(
    name="format-convert",
    retries=1,
    retry_delay_seconds=30,
    tags=["datapusher-plus", "qsv-subprocess"],
    persist_result=True,
    result_storage=DEFAULT_RESULT_STORAGE,
    # Content-based: same input file_hash + same task source = same output.
    cache_policy=CONTENT_CACHE_POLICY,
    cache_expiration=DEFAULT_CACHE_EXPIRATION,
)
def format_convert_task(prev: DownloadResult) -> ConvertResult:
    """Excel/ODS/Shapefile/GeoJSON/ZIP → CSV via qsv."""
    ctx = _stage_run(FormatConverterStage())
    return ConvertResult(
        csv_path=ctx.tmp,
        converted_from=ctx.resource.get("format"),
        file_hash=prev.file_hash,
    )


@task(
    name="validate",
    retries=0,
    tags=["datapusher-plus", "qsv-subprocess"],
    persist_result=True,
    result_storage=DEFAULT_RESULT_STORAGE,
    cache_policy=CONTENT_CACHE_POLICY,
    cache_expiration=DEFAULT_CACHE_EXPIRATION,
)
def validate_task(prev: ConvertResult) -> ValidateResult:
    """RFC-4180 validation with quarantine, encoding normalization, dedup."""
    ctx = _stage_run(ValidationStage())

    # Enforce the quarantine threshold (raises if exceeded) and emit the
    # row.quarantined event. ``apply_quarantine`` is a no-op when no rows
    # were rejected.
    quarantine.apply_quarantine(
        resource_id=ctx.resource_id,
        clean_csv_path=ctx.tmp,
        quarantine_csv_path=ctx.quarantine_csv_path or None,
        quarantined_rows=ctx.quarantined_rows,
        total_rows=ctx.rows_to_copy + ctx.quarantined_rows,
    )

    return ValidateResult(
        csv_path=ctx.tmp,
        rows_after_dedup=ctx.rows_to_copy,
        quarantined_rows=ctx.quarantined_rows,
        quarantine_csv_path=ctx.quarantine_csv_path or None,
        file_hash=prev.file_hash,
    )


@task(
    name="analyze",
    retries=1,
    retry_delay_seconds=60,
    tags=["datapusher-plus", "qsv-subprocess", "cpu-bound"],
    persist_result=True,
    result_storage=DEFAULT_RESULT_STORAGE,
    cache_policy=CONTENT_CACHE_POLICY,
    cache_expiration=DEFAULT_CACHE_EXPIRATION,
)
def analyze_task(prev: ValidateResult) -> AnalyzeResult:
    """qsv stats + frequency + type inference + PII screening."""
    ctx = _stage_run(AnalysisStage())
    if ctx.pii_found:
        # Operators wire Prefect Automations to this event for alerting.
        pii_fields = [
            h.get("id") or h.get("name")
            for h in ctx.headers_dicts
            if isinstance(h, dict) and h.get("pii")
        ]
        events.emit_pii_detected(
            resource_id=ctx.resource_id, fields=[f for f in pii_fields if f]
        )
    return AnalyzeResult(
        headers=list(ctx.headers),
        headers_dicts=list(ctx.headers_dicts),
        original_header_dict=dict(ctx.original_header_dict),
        dataset_stats=dict(ctx.dataset_stats),
        resource_fields_stats=dict(ctx.resource_fields_stats),
        resource_fields_freqs=dict(ctx.resource_fields_freqs),
        pii_found=ctx.pii_found,
        file_hash=prev.file_hash,
    )


@task(
    name="database-load",
    retries=_TASK_RETRY_DATABASE,
    retry_delay_seconds=30,
    tags=["datapusher-plus", "datastore-copy"],
    # Persist so operators can inspect the row counts and existing_info
    # snapshot after a run. No cache_key_fn — the task's value is the
    # Postgres side effect; caching it would skip the actual load.
    persist_result=True,
    result_storage=DEFAULT_RESULT_STORAGE,
)
def database_task(prev: AnalyzeResult) -> DatabaseResult:
    """Postgres COPY into the datastore."""
    ctx = _stage_run(DatabaseStage())
    return DatabaseResult(
        rows_to_copy=ctx.rows_to_copy,
        copied_count=ctx.copied_count,
        existing_info=dict(ctx.existing_info) if ctx.existing_info else None,
    )


@task(
    name="auto-index",
    retries=1,
    retry_delay_seconds=30,
    tags=["datapusher-plus", "datastore-copy"],
    # Destructive (creates Postgres indexes); persisted but not cached.
    persist_result=True,
    result_storage=DEFAULT_RESULT_STORAGE,
)
def indexing_task(prev: DatabaseResult) -> IndexingResult:
    """Create indexes based on cardinality / date columns."""
    _stage_run(IndexingStage())
    return IndexingResult()


@task(
    name="formula",
    retries=0,
    tags=["datapusher-plus"],
    # Writes Jinja2-derived metadata back to the resource; persisted but
    # not cached (the act of writing is the value).
    persist_result=True,
    result_storage=DEFAULT_RESULT_STORAGE,
)
def formula_task(prev: IndexingResult) -> FormulaResult:
    """Jinja2 formula evaluation against dpps/dppf/dpp namespaces."""
    _stage_run(FormulaStage())
    return FormulaResult()


@task(
    name="metadata",
    retries=1,
    retry_delay_seconds=15,
    tags=["datapusher-plus"],
    # Writes datastore alias + dpp_suggestions back to CKAN; persisted
    # for observability, not cached.
    persist_result=True,
    result_storage=DEFAULT_RESULT_STORAGE,
)
def metadata_task(prev: FormulaResult) -> MetadataResult:
    """Final datastore resource_show updates + dpp_suggestions write-back."""
    _stage_run(MetadataStage())
    return MetadataResult()



# ---------------------------------------------------------------------------
# Rollback hooks
# ---------------------------------------------------------------------------
#
# The four destructive tasks (database, indexing, formula, metadata) run
# inside ``with transaction():`` in the flow body. When any task in that
# group raises, Prefect invokes the registered ``on_rollback`` hooks
# *in reverse order* so the most-recently-committed task cleans up first.
#
# The v2 pipeline had no rollback story: a failure between COPY and
# CREATE INDEX left the datastore with a half-built resource. In v3.0
# the database hook drops the datastore table when this run created it
# from empty; if the table had pre-existing content, the hook logs the
# inconsistency and leaves the data in place — restoring a prior snapshot
# is out of scope. The remaining hooks log only: dropping the datastore
# table sweeps their writes (indexes live on the table; formula and
# alias writes are surfaced for operator review).


def _runtime_or_none() -> Optional[RuntimeContext]:
    """Fetch the bound RuntimeContext without raising outside a flow."""
    try:
        return get_runtime_context()
    except LookupError:
        return None


@database_task.on_rollback
def _rollback_database(txn) -> None:
    """Drop the datastore table when this run created it from empty."""
    runtime = _runtime_or_none()
    if runtime is None:
        return
    resource_id = runtime.resource_id
    logger = runtime.logger
    existing = runtime.existing_info
    if not existing:
        try:
            dsu.delete_datastore_resource(resource_id)
            logger.info(
                f"Rollback: dropped datastore resource {resource_id} "
                "after transactional failure"
            )
        except Exception as e:
            logger.warning(
                f"Rollback: could not drop datastore {resource_id}: {e}"
            )
    else:
        # The resource already had datastore content before this run. We
        # cannot safely revert without a snapshot — log and leave it.
        logger.warning(
            f"Rollback: datastore {resource_id} had pre-existing content "
            "before this run; leaving partial writes in place for operator "
            "review (see Prefect UI for the failed flow run)"
        )


@indexing_task.on_rollback
def _rollback_indexing(txn) -> None:
    """No-op: the database rollback drops the table, taking indexes with it."""
    runtime = _runtime_or_none()
    if runtime is not None:
        runtime.logger.info(
            "Rollback: indexes will be dropped with the datastore table "
            "(see database rollback)"
        )


@formula_task.on_rollback
def _rollback_formula(txn) -> None:
    """Log: the dpp_suggestions write lives on the resource record.

    Reverting it requires a pre-state snapshot we do not capture in v3.0.
    Operators inspect the resource and reset if needed.
    """
    runtime = _runtime_or_none()
    if runtime is not None:
        runtime.logger.warning(
            f"Rollback: dpp_suggestions for resource {runtime.resource_id} "
            "may have been partially written; verify the resource record"
        )


@metadata_task.on_rollback
def _rollback_metadata(txn) -> None:
    """Log: the datastore alias (if any) lives in Postgres and is dropped
    together with the table by the database rollback. Any resource-record
    updates are flagged for operator review."""
    runtime = _runtime_or_none()
    if runtime is not None:
        runtime.logger.warning(
            f"Rollback: metadata updates for resource {runtime.resource_id} "
            "may have been partially applied; verify the resource record"
        )


# ---------------------------------------------------------------------------
# Pre-flight helpers
# ---------------------------------------------------------------------------


def _validate_input(input_payload: Dict[str, Any]) -> None:
    """Mirror of v2 ``pipeline.validate_input``."""
    if "metadata" not in input_payload:
        raise utils.JobError("Metadata missing")
    if "resource_id" not in input_payload["metadata"]:
        raise utils.JobError("No id provided.")


def _build_runtime_context(
    job_input: JobInput, temp_dir: str
) -> RuntimeContext:
    """
    Construct the per-run ``RuntimeContext`` (== legacy ``ProcessingContext``).

    Sets up the task-scoped logger with both the v2 ``StoringHandler`` (so
    the DP+ ``Logs`` table continues to populate, and the CKAN UI's job
    detail view keeps working) and a stream handler for the worker's
    stdout.
    """
    task_id = job_input.task_id
    input_payload = job_input.input

    # Task-scoped logger — same approach as v2 ``_push_to_datastore``.
    handler = utils.StoringHandler(task_id, input_payload)
    logger = logging.getLogger(task_id)
    logger.addHandler(handler)
    logger.addHandler(logging.StreamHandler())
    try:
        log_level = getattr(logging, conf.UPLOAD_LOG_LEVEL.upper())
    except AttributeError:
        log_level = TRACE
    logger.setLevel(log_level)
    logger.info(f"Setting log level to {logging.getLevelName(int(log_level))}")

    if not Path(conf.QSV_BIN).is_file():
        raise utils.JobError(f"{conf.QSV_BIN} not found.")

    qsv = QSVCommand(logger=logger)

    # Fetch the resource (one retry, as in v2).
    resource_id = job_input.resource_id
    try:
        resource = dsu.get_resource(resource_id)
    except utils.JobError:
        time.sleep(5)
        resource = dsu.get_resource(resource_id)

    ctx = ProcessingContext(
        task_id=task_id,
        input=input_payload,
        dry_run=job_input.dry_run,
        temp_dir=temp_dir,
        logger=logger,
        qsv=qsv,
        resource=resource,
        resource_id=resource_id,
        ckan_url=job_input.ckan_url,
        # Stamp now so the duration-since-start computed in the success
        # event (``time.time() - timer_start``) is meaningful.
        timer_start=time.time(),
    )
    return ctx


def _resource_is_datastore_dump(ctx: RuntimeContext) -> bool:
    """v2 early-exit: ``url_type == 'datastore'`` resources are not re-ingested."""
    return ctx.resource.get("url_type") == "datastore"



# ---------------------------------------------------------------------------
# PII review suspension
# ---------------------------------------------------------------------------
#
# When PII screening flags more fields than ``pii_review_threshold``, the
# flow suspends via Prefect's ``suspend_flow_run`` and waits for an
# operator to approve or reject via a typed form in the Prefect UI. The
# worker shuts down during the wait — important because review may take
# hours or days. On resume, persisted task results replay the upstream
# stages from cache; only the suspension point and downstream tasks
# actually re-execute.
#
# The feature is off by default (``pii_review_threshold = 0``). Operators
# turn it on by raising the threshold in CKAN config.


try:
    # Lazy: keep these imports near their use site so the module still
    # imports when Prefect isn't fully installed (e.g., during Alembic).
    from prefect.input import RunInput  # type: ignore

    class PIIReviewApproval(RunInput):
        """Operator-supplied decision for a PII-flagged ingestion run.

        Surfaces as a typed form on the suspended flow run's page in the
        Prefect UI. Resuming the flow with this input drives the post-
        suspend branch: ``approve=True`` continues into the transactional
        datastore writes; ``approve=False`` raises a ``JobError`` so the
        flow ends cleanly before any datastore mutation.
        """

        approve: bool = False
        reviewer: str = ""
        notes: str = ""
except Exception:  # pragma: no cover - Prefect import edge cases
    PIIReviewApproval = None  # type: ignore


def _pii_review_threshold() -> int:
    """Read the configured PII review threshold.

    ``0`` (default) disables the feature entirely.
    """
    try:
        import ckan.plugins.toolkit as tk

        v = tk.config.get("ckanext.datapusher_plus.pii_review_threshold")
        if v is not None and v != "":
            return int(v)
    except Exception:
        pass
    return _env_int("DATAPUSHER_PLUS_PII_REVIEW_THRESHOLD", 0)


def _count_pii_fields(headers_dicts: List[Dict[str, Any]]) -> int:
    """Count columns flagged as PII in the analysis output."""
    return sum(
        1
        for h in headers_dicts
        if isinstance(h, dict) and h.get("pii")
    )


def _maybe_suspend_for_pii_review(
    runtime: RuntimeContext, analysis: AnalyzeResult, job_input: JobInput
) -> None:
    """Suspend the flow run for human review when PII threshold is exceeded.

    Raises ``utils.JobError`` if the reviewer rejects. Returns normally
    (and lets the flow proceed into the transactional write group) when
    the reviewer approves or the threshold is not crossed.
    """
    threshold = _pii_review_threshold()
    if threshold <= 0 or PIIReviewApproval is None:
        return  # feature off
    if not analysis.pii_found:
        return
    pii_count = _count_pii_fields(analysis.headers_dicts)
    if pii_count <= threshold:
        return

    runtime.logger.warning(
        f"PII review threshold exceeded: {pii_count} fields flagged "
        f"(threshold={threshold}). Suspending flow for operator review "
        "via the Prefect UI."
    )

    from prefect.flow_runs import suspend_flow_run

    # Stable key so that on resume, ``suspend_flow_run`` returns the
    # provided input instead of suspending again. Tying the key to the
    # flow_run_id ensures one suspension per run — independent of any
    # cache hits on upstream tasks.
    flow_run_id = prefect_client.get_current_flow_run_id()

    approval = suspend_flow_run(
        wait_for_input=PIIReviewApproval.with_initial_data(
            notes=(
                f"DataPusher+ flagged {pii_count} PII fields in resource "
                f"{job_input.resource_id}. Review the analysis artifact "
                "and approve/reject before any datastore writes happen."
            )
        ),
        key=f"pii-review-{flow_run_id}",
    )

    if not approval.approve:
        reason = approval.notes or "(no reason given)"
        runtime.logger.error(
            f"PII review rejected by {approval.reviewer or '(unspecified)'}: "
            f"{reason}"
        )
        raise utils.JobError(f"PII review rejected: {reason}")

    runtime.logger.info(
        f"PII review approved by {approval.reviewer or '(unspecified)'}: "
        f"{approval.notes or '(no notes)'}"
    )


# ---------------------------------------------------------------------------
# Flow state-change hooks
# ---------------------------------------------------------------------------


def _cleanup_temp_dir(flow_run_context) -> None:
    """Best-effort cleanup hook invoked on failure / crash."""
    try:
        ctx = get_runtime_context()
        td = ctx.temp_dir
        if td and Path(td).exists():
            # The flow body's tempfile.TemporaryDirectory normally handles
            # cleanup; this hook is a safety net for crashes where the
            # context manager didn't get to run.
            import shutil

            shutil.rmtree(td, ignore_errors=True)
    except LookupError:
        # Runtime context never bound — nothing to clean.
        pass


def _on_flow_failure(flow, flow_run, state) -> None:
    """Logged for observability. State ownership stays in the flow body."""
    logger = get_run_logger()
    logger.error(f"Flow {flow_run.name} failed: {state.message}")
    _cleanup_temp_dir(flow_run)


def _on_flow_crashed(flow, flow_run, state) -> None:
    logger = get_run_logger()
    logger.error(f"Flow {flow_run.name} crashed: {state.message}")
    _cleanup_temp_dir(flow_run)


# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------


@flow(
    name="datapusher-plus",
    log_prints=True,
    timeout_seconds=_FLOW_TIMEOUT_SECONDS,
    on_failure=[_on_flow_failure],
    on_crashed=[_on_flow_crashed],
)
def datapusher_plus_flow(job_input: JobInput) -> Optional[str]:
    """
    Ingest one CKAN resource into the datastore.

    Returns ``"error"`` on failure, ``None`` on success — matching v2's
    ``datapusher_plus_to_datastore`` return contract so any external
    callers continue to work.
    """
    # Accept a plain dict (Prefect's parameter deserialization may not
    # reconstruct the frozen dataclass) and coerce.
    if isinstance(job_input, dict):
        job_input = JobInput(**job_input)

    prefect_logger = get_run_logger()
    prefect_logger.info(
        f"Starting datapusher-plus flow for resource {job_input.resource_id}"
    )

    _validate_input(job_input.input)

    flow_run_id = prefect_client.get_current_flow_run_id()
    job_id = job_input.task_id

    # Register the job in the DP+ Jobs table at flow start. This is what
    # ``datapusher_status`` and the CKAN UI read.
    try:
        dph.add_pending_job(job_id, **job_input.input)
    except sa.exc.IntegrityError:
        raise utils.JobError("Job already exists.")
    dph.set_aps_job_id(job_id, flow_run_id)  # column repurposed for flow_run_id

    # Announce running state to CKAN.
    result_url = job_input.input.get("result_url")
    if result_url:
        callback_datapusher_hook(
            result_url=result_url,
            job_dict={"metadata": job_input.input.get("metadata", {}), "status": "running"},
        )

    errored = False
    with tempfile.TemporaryDirectory() as temp_dir:
        runtime = _build_runtime_context(job_input, temp_dir)
        token = set_runtime_context(runtime)
        try:
            if _resource_is_datastore_dump(runtime):
                runtime.logger.info("Dump files are managed with the Datastore API")
                dph.mark_job_as_completed(job_id, {"skipped": "datastore-managed"})
                return None

            # Read-only / non-destructive stages.
            dl = download_task(job_input)
            cv = format_convert_task(dl)
            vl = validate_task(cv)
            an = analyze_task(vl)

            # Human-in-the-loop gate. When PII screening flags more
            # fields than ``pii_review_threshold``, the flow suspends
            # here for operator review via the Prefect UI. Approval
            # continues; rejection raises JobError before any datastore
            # writes happen.
            _maybe_suspend_for_pii_review(runtime, an, job_input)

            # Datastore-mutating group — atomic under transaction(). If
            # any task here fails, the @on_rollback hooks registered just
            # below the task definitions clean up partial Postgres writes
            # (the database hook drops a newly-created datastore table;
            # indexes, alias, and formula writes are swept by the table
            # drop or flagged for review when the table pre-existed).
            with transaction():
                db = database_task(an)
                idx = indexing_task(db)
                fm = formula_task(idx)
                md = metadata_task(fm)

            if job_input.dry_run:
                dph.mark_job_as_completed(
                    job_id, {"headers": runtime.headers_dicts}
                )
                return None

            # Observability surface for a successful run: a Data Quality
            # Markdown artifact (visible inline on the Prefect flow-run
            # page), a one-click CKAN-resource link artifact, an optional
            # Quarantine Markdown artifact when validate_task rejected
            # some rows, and a ``datapusher.resource.ingested`` event that
            # operators wire into Automations for downstream side effects.
            artifacts.create_data_quality_artifact(
                resource_id=job_input.resource_id,
                rows=runtime.copied_count,
                headers=runtime.headers_dicts,
                pii_found=runtime.pii_found,
                quarantined_rows=runtime.quarantined_rows,
            )
            artifacts.create_resource_link_artifact(
                ckan_url=job_input.ckan_url, resource_id=job_input.resource_id
            )
            if runtime.quarantined_rows > 0 and runtime.quarantine_csv_path:
                artifacts.create_quarantine_artifact(
                    resource_id=job_input.resource_id,
                    quarantined_rows=runtime.quarantined_rows,
                    total_rows=runtime.copied_count + runtime.quarantined_rows,
                    csv_path=runtime.quarantine_csv_path,
                )
            events.emit_resource_ingested(
                resource_id=job_input.resource_id,
                rows=runtime.copied_count,
                file_hash=runtime.file_hash,
                duration_seconds=time.time() - runtime.timer_start,
            )

            dph.mark_job_as_completed(
                job_id,
                {
                    "rows": runtime.copied_count,
                    "headers": runtime.headers_dicts,
                },
            )
            return None

        except utils.JobError as e:
            errored = True
            dph.mark_job_as_errored(job_id, str(e))
            runtime.logger.error(f"DataPusher Plus error: {e}")
            prefect_logger.error(f"DataPusher Plus error: {e}")
            raise
        except Exception as e:
            errored = True
            tb = traceback.format_tb(sys.exc_info()[2])[-1] + repr(e)
            dph.mark_job_as_errored(job_id, tb)
            runtime.logger.error(
                f"DataPusher Plus error: {e}, {traceback.format_exc()}"
            )
            prefect_logger.error(f"DataPusher Plus error: {e}")
            raise
        finally:
            reset_runtime_context(token)
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
