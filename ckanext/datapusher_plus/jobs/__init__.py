# -*- coding: utf-8 -*-
"""
DataPusher Plus Jobs Module

The v3.0 release replaces the v2 ``DataProcessingPipeline`` loop with a
Prefect flow. The public composable primitives — module-level ``@task``
functions per ingestion stage and the entry-point ``@flow`` — live in
``prefect_flow``. Custom flows registered via
``ckanext.datapusher_plus.prefect_flow`` should import from there.

This module also exports backward-compatible shims so v2 callers that
referenced ``push_to_datastore`` / ``datapusher_plus_to_datastore`` keep
working: both names now route through the new Prefect flow.
"""

from typing import Any, Dict, Optional

from ckanext.datapusher_plus.jobs.prefect_flow import (
    callback_datapusher_hook,
    datapusher_plus_flow,
)
from ckanext.datapusher_plus.jobs.runtime_context import JobInput


def push_to_datastore(
    task_id: str, input: Dict[str, Any], dry_run: bool = False
) -> Optional[str]:
    """Backward-compat shim for the v2 ``push_to_datastore`` callable.

    Constructs a ``JobInput`` from the legacy arg shape and invokes the
    Prefect flow. Useful for tests that drive the flow as a plain Python
    function without going through a Prefect worker.
    """
    metadata = input.get("metadata", {})
    job_input = JobInput(
        task_id=task_id,
        resource_id=metadata.get("resource_id", ""),
        ckan_url=metadata.get("ckan_url", ""),
        input=input,
        dry_run=dry_run,
    )
    return datapusher_plus_flow(job_input)


# v2 RQ entry-point name. v3 callers should use ``datapusher_plus_flow``.
datapusher_plus_to_datastore = datapusher_plus_flow

__all__ = [
    "datapusher_plus_flow",
    "datapusher_plus_to_datastore",
    "push_to_datastore",
    "callback_datapusher_hook",
]
