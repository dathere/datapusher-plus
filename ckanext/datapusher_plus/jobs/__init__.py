# -*- coding: utf-8 -*-
"""
DataPusher Plus Jobs Module

The v3.0 release replaces the v2 ``DataProcessingPipeline`` loop with a
Prefect flow. The public composable primitives — module-level ``@task``
functions per ingestion stage and the entry-point ``@flow`` — live in
``prefect_flow``. Custom flows registered via
``ckanext.datapusher_plus.prefect_flow`` should import from there.

This module exposes a small public surface (``datapusher_plus_flow``,
``push_to_datastore``, ``datapusher_plus_to_datastore``,
``callback_datapusher_hook``) but does NOT import Prefect at module
load. Eager imports here would pull in the Prefect runtime every time
CKAN loads the DP+ plugin (during ``ckan db init``, ``ckan plugins
info``, etc.) — and Prefect spins up an ephemeral server when no
``PREFECT_API_URL`` is configured, polluting stdout with log lines and
breaking shell pipelines like ``ckan datastore set-permissions | psql``.
PEP 562 ``__getattr__`` defers the Prefect import until something
actually accesses the flow.
"""

from typing import Any, Dict, Optional


def push_to_datastore(
    task_id: str, input: Dict[str, Any], dry_run: bool = False
) -> Optional[str]:
    """Backward-compat shim for the v2 ``push_to_datastore`` callable.

    Constructs a ``JobInput`` from the legacy arg shape and invokes the
    Prefect flow. Useful for tests that drive the flow as a plain Python
    function without going through a Prefect worker.
    """
    # Lazy: avoid pulling Prefect into CKAN admin commands that import
    # this module but never run a job.
    from ckanext.datapusher_plus.jobs.prefect_flow import datapusher_plus_flow
    from ckanext.datapusher_plus.jobs.runtime_context import JobInput

    metadata = input.get("metadata", {})
    job_input = JobInput(
        task_id=task_id,
        resource_id=metadata.get("resource_id", ""),
        ckan_url=metadata.get("ckan_url", ""),
        input=input,
        dry_run=dry_run,
    )
    return datapusher_plus_flow(job_input)


def __getattr__(name: str):
    """Lazy access for the flow and its callback helper (PEP 562).

    ``from ckanext.datapusher_plus.jobs import datapusher_plus_flow``
    triggers this hook, which imports Prefect on demand instead of at
    plugin-load time.
    """
    if name in {"datapusher_plus_flow", "datapusher_plus_to_datastore"}:
        from ckanext.datapusher_plus.jobs.prefect_flow import datapusher_plus_flow

        return datapusher_plus_flow
    if name == "callback_datapusher_hook":
        from ckanext.datapusher_plus.jobs.prefect_flow import callback_datapusher_hook

        return callback_datapusher_hook
    raise AttributeError(
        f"module 'ckanext.datapusher_plus.jobs' has no attribute {name!r}"
    )


def __dir__():
    """Surface the PEP 562 lazy names in introspection.

    Without this, ``dir(...)`` / REPL tab-completion / reflection-based
    discovery miss the lazily-resolved names even though they are in
    ``__all__`` — and listing them here does not force the Prefect
    import (only attribute *access* does).
    """
    return sorted({*globals(), *__all__})


__all__ = [
    "datapusher_plus_flow",
    "datapusher_plus_to_datastore",
    "push_to_datastore",
    "callback_datapusher_hook",
]
