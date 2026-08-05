# -*- coding: utf-8 -*-
# flake8: noqa: E501
"""
Prefect-free core shared by both DataPusher+ job runners.

DP+ v3 can execute the same nine ingestion stages two ways:

* ``jobs/prefect_flow.py`` — the default, Prefect-orchestrated flow
  (per-task retries, result caching, run artifacts, transactional
  rollback, PII suspend-for-review).
* ``jobs/local_runner.py`` — the in-process fallback selected by
  ``ckanext.datapusher_plus.prefect_enabled = false``. Runs on CKAN's own
  RQ background-job worker with no Prefect server, worker, or import
  anywhere in the path.

Everything both runners need — the CKAN status callback, input
validation, ``RuntimeContext`` construction, the stage invoker and its
"nothing to do" signal, and the datastore rollback body — lives here so
there is exactly one implementation of each.

**Nothing in this module (or anything it imports) may import
``prefect``.** Avoiding that import is the whole point of the disabled
mode: on a deployment whose ``$PREFECT_HOME`` is not writable (a CKAN
process running with ``HOME=/root``, say) even ``import prefect`` raises
``PermissionError`` while it tries to create ``profiles.toml``.
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional

import requests

import ckanext.datapusher_plus.config as conf
import ckanext.datapusher_plus.datastore_utils as dsu
import ckanext.datapusher_plus.dictionary_stash as dict_stash
import ckanext.datapusher_plus.utils as utils
from ckanext.datapusher_plus.jobs.context import ProcessingContext
from ckanext.datapusher_plus.jobs.runtime_context import (
    JobInput,
    RuntimeContext,
    get_runtime_context,
    rehydrate,
)
from ckanext.datapusher_plus.logging_utils import TRACE
from ckanext.datapusher_plus.qsv_utils import QSVCommand

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tunable resolution
# ---------------------------------------------------------------------------


def resolve_int(env_name: str, config_key: str, default: int) -> int:
    """Resolve an int tunable from env → ckan.ini → default.

    Env wins so operators and CI can override per-process without
    touching ``ckan.ini``. When the env var is unset, fall back to the
    CKAN config key. When nothing is set, return ``default``.

    ``prefect_flow._resolve_int`` layers a Prefect Variable lookup on
    top of this and then delegates here; the local runner uses it
    directly (there is no Prefect server to hold Variables).
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
        # CKAN config not loaded (e.g., when running in a bare worker
        # process) — fall through to the default.
        pass
    return default


# ---------------------------------------------------------------------------
# Callback helper
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
# Stage invocation
# ---------------------------------------------------------------------------


class StageAbort(Exception):
    """A stage returned ``None`` — the BaseStage "nothing to do" signal.

    Per the ``BaseStage`` contract, ``process()`` may return ``None`` to
    stop the rest of the pipeline gracefully (e.g. the Analysis stage on
    a zero-record file logs "Upload skipped as there are zero records"
    and returns ``None``). The v2 pipeline stopped there and the job
    *completed* — nothing was wrong, there was simply nothing to load.

    Raised by ``run_stage`` and caught distinctly from ``JobError`` by
    both runners so the job is marked complete-with-skip, not errored.
    """

    def __init__(self, stage_name: str):
        self.stage_name = stage_name
        super().__init__(
            f"Stage {stage_name} stopped the pipeline (nothing to do)"
        )


def run_stage(stage, prev: Any = None) -> RuntimeContext:
    """Invoke a stage on the bound RuntimeContext.

    ``prev`` is the upstream task's result. When given, the bound
    ``RuntimeContext`` is rehydrated from it first, so the stage sees
    correct ``ctx`` state even if the upstream task's body never ran (a
    Prefect cache hit, or a persisted-result replay on a flow re-run) —
    that body is what would otherwise have mutated the shared context.
    The root task (``download_task``) passes no ``prev``, and neither
    does the local runner: there, every stage mutates one live context
    in a single process, so there is nothing to rehydrate from.

    A stage returning ``None`` is the BaseStage "skip / nothing to do"
    signal (per its docstring) — surfaced here as ``StageAbort`` so the
    caller can stop cleanly and mark the job *complete*, not errored.
    """
    ctx = get_runtime_context()
    if prev is not None:
        rehydrate(ctx, prev)
    result = stage(ctx)
    if result is None:
        raise StageAbort(stage.name)
    return result


# ---------------------------------------------------------------------------
# Pre-flight helpers
# ---------------------------------------------------------------------------


def validate_input(input_payload: Dict[str, Any]) -> None:
    """Mirror of v2 ``pipeline.validate_input``."""
    if "metadata" not in input_payload:
        raise utils.JobError("Metadata missing")
    if "resource_id" not in input_payload["metadata"]:
        raise utils.JobError("No id provided.")


def build_runtime_context(job_input: JobInput, temp_dir: str) -> RuntimeContext:
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


def resource_is_datastore_dump(ctx: RuntimeContext) -> bool:
    """v2 early-exit: ``url_type == 'datastore'`` resources are not re-ingested."""
    return ctx.resource.get("url_type") == "datastore"


# ---------------------------------------------------------------------------
# Datastore rollback
# ---------------------------------------------------------------------------


def rollback_datastore_writes(runtime: RuntimeContext) -> None:
    """Drop the datastore table after a failed write group, restoring the
    stashed Data Dictionary if the analysis stage saved one.

    The Prefect flow calls this from ``database_task.on_rollback`` when
    its transaction unwinds; the local runner calls it directly when a
    stage after the database stage raises. Both mean the same thing: the
    datastore table this run built is not trustworthy.

    The database stage's path is: delete any pre-existing table, create
    an empty one, then COPY into it. So by the time a later stage fails,
    the original content is already gone in *both* the "created from
    empty" and "had pre-existing content" cases — what is on disk is a
    half-written *new* table, not recoverable original data. Dropping it
    unconditionally is strictly better than leaving polluted contents an
    operator may not notice.
    """
    resource_id = runtime.resource_id
    try:
        dsu.delete_datastore_resource(resource_id)
        runtime.logger.info(
            f"Rollback: dropped datastore resource {resource_id} "
            "after transactional failure"
        )
    except Exception as e:
        runtime.logger.warning(
            f"Rollback: could not drop datastore {resource_id}: {e}"
        )

    # Issue #265: if the analysis stage stashed a Data Dictionary
    # before the original delete, restore it now by re-creating the
    # datastore resource with the stashed per-field ``info`` dicts and
    # zero rows. The *data* is unrecoverable (it never landed), but the
    # operator's annotations (labels, descriptions, type_overrides)
    # are preserved across the failed run. The stash file is left in
    # place for inspection if restore itself fails — a future
    # successful run will overwrite it.
    stashed = dict_stash.load(resource_id)
    if not stashed:
        return
    try:
        # Derive each field's Postgres ``type`` from the stashed
        # ``info["type_override"]`` (mapped through ``conf.TYPE_MAPPING``
        # values, e.g. ``numeric`` / ``timestamp`` / ``text``). This
        # mirrors the analysis stage's ``_build_headers_dicts`` merge:
        # otherwise CKAN's ``datastore_create`` falls back to ``text``
        # for every column, and a column the operator originally
        # annotated as numeric or timestamp would be restored as text —
        # silently inconsistent with the stashed dictionary's intent.
        valid_types = set(conf.TYPE_MAPPING.values())
        fields = []
        for fid, info in stashed.items():
            field: Dict[str, Any] = {"id": fid, "info": info}
            type_override = (info or {}).get("type_override")
            if type_override in valid_types:
                field["type"] = type_override
            else:
                field["type"] = "text"
            fields.append(field)
        dsu.send_resource_to_datastore(
            resource=None,
            resource_id=resource_id,
            headers=fields,
            records=[],
            aliases=[],
            calculate_record_count=False,
        )
        runtime.logger.info(
            f"Rollback: restored Data Dictionary for {resource_id} "
            f"({len(fields)} field(s)) from stash"
        )
        dict_stash.clear(resource_id)
    except Exception as e:
        runtime.logger.warning(
            f"Rollback: could not restore Data Dictionary for "
            f"{resource_id}: {e}. Stash file retained at "
            f"{dict_stash.stash_path(resource_id)} for inspection."
        )
