# -*- coding: utf-8 -*-
"""
Thin wrapper around the Prefect 3 client for DataPusher+.

Every orchestrator-specific call in DP+ goes through this module so that
the rest of the codebase does not import from ``prefect.*`` directly.
That gives us one place to mock for unit tests, one place to evolve when
the Prefect API changes, and one place that knows how DP+ maps its data
contract (resource_id, flow_run_id) onto Prefect concepts.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Set

import ckan.plugins.toolkit as tk

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration accessors
# ---------------------------------------------------------------------------


def _deployment_name() -> str:
    """
    Fully-qualified Prefect deployment name in the form ``<flow>/<deployment>``.

    Operators set this with ``ckanext.datapusher_plus.prefect_deployment_name``.
    Default matches the deployment we ship in ``prefect.yaml``.
    """
    return tk.config.get(
        "ckanext.datapusher_plus.prefect_deployment_name",
        "datapusher-plus/datapusher-plus",
    )


def _work_pool() -> str:
    """Operator-overridable work-pool name."""
    return tk.config.get(
        "ckanext.datapusher_plus.prefect_work_pool", "datapusher-plus"
    )


# ---------------------------------------------------------------------------
# Submit
# ---------------------------------------------------------------------------


def submit_flow_run(payload: Dict[str, Any], *, timeout: Optional[int] = None) -> str:
    """
    Submit a DP+ flow run via the configured Prefect deployment.

    The call is non-blocking on the flow itself — it returns as soon as the
    Prefect server has accepted the run. A Prefect worker subscribed to the
    work pool picks up the run and executes it.

    Args:
        payload: dict-form ``JobInput`` (use ``dataclasses.asdict``). Will be
            passed to ``datapusher_plus_flow`` as its ``job_input`` parameter.
        timeout: outer flow timeout in seconds. Forwarded to Prefect as a job
            variable so the flow envelope picks it up; ``None`` means no
            outer timeout (the per-task timeouts still apply).

    Returns:
        The new ``flow_run.id`` as a string. Callers store this in
        ``task_status.value`` alongside the DP+ ``job_id``.

    Raises:
        Whatever Prefect raises if the server is unreachable, the deployment
        does not exist, etc. Callers should let these propagate so the
        CKAN UI surfaces them — there is no useful retry from inside a
        web request.
    """
    # Imported lazily so DP+ modules can be imported in tooling contexts
    # (e.g. Alembic migrations) without requiring Prefect to be installed.
    from prefect.deployments import run_deployment

    parameters: Dict[str, Any] = {"job_input": payload}
    job_variables: Dict[str, Any] = {}
    if timeout is not None:
        # Surfaces on the flow run so the @flow(timeout_seconds=...) honors it
        # at run start. The flow reads ``ckanext.datapusher_plus.flow_timeout``
        # from config but a per-run override wins.
        job_variables["env"] = {
            "DATAPUSHER_PLUS_FLOW_TIMEOUT_SECONDS": str(timeout),
        }

    flow_run = run_deployment(
        name=_deployment_name(),
        parameters=parameters,
        job_variables=job_variables or None,
        timeout=0,  # fire-and-forget: return as soon as run is created
        as_subflow=False,
    )
    return str(flow_run.id)


# ---------------------------------------------------------------------------
# Stale / stillborn detection
# ---------------------------------------------------------------------------


# States that mean "this flow run still owns the resource_id"; used by
# ``datapusher_submit`` to decide whether to skip a duplicate submission.
_ACTIVE_STATE_TYPES = (
    "SCHEDULED",
    "PENDING",
    "RUNNING",
    "PAUSED",
    "CANCELLING",
)


def get_running_resource_ids() -> Set[str]:
    """
    Return the set of CKAN resource_ids currently being ingested.

    Replaces the v2 RQ-queue regex scan at ``logic/action.py:117-165``. Uses
    the Prefect API to query flow runs in non-terminal states for the
    configured deployment, then pulls ``resource_id`` out of each run's
    ``parameters["job_input"]``.

    Network or API errors are logged and the function returns an empty set
    — the caller treats "unknown" the same as "nothing running", which is
    the same behavior the v2 RQ scan exhibits when Redis is offline.
    """
    try:
        from prefect.client.orchestration import get_client
        from prefect.client.schemas.filters import (
            DeploymentFilter,
            DeploymentFilterName,
            FlowRunFilter,
            FlowRunFilterState,
            FlowRunFilterStateType,
        )
        from prefect.client.schemas.objects import StateType
    except Exception as e:  # pragma: no cover - import-time issues
        log.warning("Prefect client unavailable for stale-job check: %s", e)
        return set()

    deployment_name = _deployment_name().split("/", 1)[-1]
    state_types = [getattr(StateType, name) for name in _ACTIVE_STATE_TYPES]

    async def _fetch() -> Set[str]:
        async with get_client() as client:
            runs = await client.read_flow_runs(
                flow_run_filter=FlowRunFilter(
                    state=FlowRunFilterState(
                        type=FlowRunFilterStateType(any_=state_types)
                    )
                ),
                deployment_filter=DeploymentFilter(
                    name=DeploymentFilterName(any_=[deployment_name])
                ),
            )
            ids: Set[str] = set()
            for run in runs:
                params = run.parameters or {}
                job_input = params.get("job_input") or {}
                rid = job_input.get("resource_id") if isinstance(job_input, dict) else None
                if rid:
                    ids.add(rid)
            return ids

    try:
        # ``run_coro_sync`` would be nicer but is internal; asyncio.run is
        # fine here because this function is called from CKAN's web request
        # handler which is not running an event loop.
        import asyncio

        return asyncio.run(_fetch())
    except Exception as e:
        log.warning("Failed to query Prefect for running resource_ids: %s", e)
        return set()


# ---------------------------------------------------------------------------
# In-flow accessors
# ---------------------------------------------------------------------------


def get_current_flow_run_id() -> str:
    """
    Return the current flow run's id. Must be called from inside a flow.

    Replaces ``rq.get_current_job().id`` at ``jobs/pipeline.py:103``.
    """
    from prefect.runtime import flow_run

    return str(flow_run.id)


def get_current_flow_run_name() -> Optional[str]:
    """Return the current flow run's name, if available."""
    from prefect.runtime import flow_run

    return getattr(flow_run, "name", None)



# ---------------------------------------------------------------------------
# Custom flow entrypoint
# ---------------------------------------------------------------------------


def resolve_flow():
    """Return the Prefect ``@flow`` object to deploy.

    Reads ``ckanext.datapusher_plus.prefect_flow`` config. When set, it
    must be an importable entrypoint of the form ``module.path:flow_name``
    pointing at a custom ``@flow``-decorated function — typically one the
    operator wrote in their own plugin that composes DP+ tasks
    differently. When unset (the default), the built-in
    ``datapusher_plus_flow`` is returned.

    The custom flow takes the same ``JobInput`` parameter so submissions
    via ``submit_flow_run`` work without changes.

    Raises ``ImportError`` (with a helpful message) if the entrypoint
    string is set but does not resolve.
    """
    entrypoint = tk.config.get("ckanext.datapusher_plus.prefect_flow", "")
    if not entrypoint:
        from ckanext.datapusher_plus.jobs.prefect_flow import datapusher_plus_flow

        return datapusher_plus_flow

    if ":" not in entrypoint:
        raise ImportError(
            f"ckanext.datapusher_plus.prefect_flow must be in the form "
            f"'module.path:flow_name'; got {entrypoint!r}"
        )

    module_path, flow_name = entrypoint.split(":", 1)
    try:
        import importlib

        module = importlib.import_module(module_path)
    except ImportError as e:
        raise ImportError(
            f"Could not import custom flow module {module_path!r}: {e}"
        ) from e

    try:
        return getattr(module, flow_name)
    except AttributeError as e:
        raise ImportError(
            f"Module {module_path!r} has no attribute {flow_name!r} "
            f"(expected a @flow-decorated function)"
        ) from e
