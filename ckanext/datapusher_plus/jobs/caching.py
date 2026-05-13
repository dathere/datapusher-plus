# -*- coding: utf-8 -*-
"""
Result-persistence and cache-key configuration for the Prefect flow.

The v3.0 flow turns on two distinct Prefect features:

1. **Result persistence** (``persist_result=True`` +
   ``result_storage=...``) — every task output is checkpointed to the
   configured storage block. Two payoffs:

   * "Re-run from failed task" works in the Prefect UI: only the failed
     and downstream tasks re-execute; completed work is replayed from
     the persisted result.
   * Operators can open any task's output from the Prefect UI to debug
     bad data.

2. **Content-based caching** (``cache_key_fn=...``) on the read-only
   stages (download / format-convert / validate / analyze). Same
   resource URL + same file content = reuse the previous task's output.
   A re-submission of an unchanged resource skips the expensive work.

   The destructive stages — database load, indexing, formula evaluation,
   metadata write-back — are **intentionally not cached**: their value
   is the side effect on the datastore, not the return value. Caching
   them would skip the actual ingestion.
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta
from typing import Any, Optional

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result storage
# ---------------------------------------------------------------------------
#
# Loaded once at module import. ``None`` is a valid value — when the block
# is unavailable (no Prefect server, block not yet registered) tasks fall
# back to Prefect's in-process result cache. The CLI's ``prefect-deploy``
# command calls ``blocks.ensure_result_storage_block`` so the block exists
# before workers start polling.


def _load_default_result_storage() -> Optional[Any]:
    try:
        from ckanext.datapusher_plus.jobs.blocks import load_result_storage_block

        return load_result_storage_block()
    except Exception as e:
        log.debug("Result storage block not available at module load: %s", e)
        return None


DEFAULT_RESULT_STORAGE = _load_default_result_storage()


# ---------------------------------------------------------------------------
# Cache expiration
# ---------------------------------------------------------------------------
#
# 24 hours by default — captures the "operator re-submits the same
# resource within a day after fixing a downstream issue" pattern while
# guaranteeing that stale cached files do not silently outlive a real
# data change. Operators can override via env var.


def _default_cache_ttl() -> timedelta:
    hours = int(os.environ.get("DATAPUSHER_PLUS_CACHE_TTL_HOURS", "24"))
    return timedelta(hours=hours)


DEFAULT_CACHE_EXPIRATION = _default_cache_ttl()


# ---------------------------------------------------------------------------
# Cache-key functions
# ---------------------------------------------------------------------------
#
# Prefect calls these with ``(context: TaskRunContext, parameters: dict)``.
# Returning ``None`` skips caching for that invocation; returning a string
# uses it as the cache key. We avoid hashing arguments that include the
# per-run tempdir path (``downloaded_path``, ``csv_path``) because they
# differ across runs even for identical content, which would defeat
# caching entirely.


def download_cache_key(context, parameters) -> Optional[str]:
    """Cache the download task by resource id, URL, and ignore-hash flag.

    Caveat: this cache key does not include the file's content hash
    (we have not downloaded it yet). A resource whose URL did not change
    but whose file content did will hit a stale cache until the TTL
    expires. Operators who need strict freshness set ``ignore_hash=True``
    on submit or shorten ``DATAPUSHER_PLUS_CACHE_TTL_HOURS``.
    """
    job_input = parameters.get("job_input")
    if job_input is None:
        return None
    # ``JobInput`` may arrive as a dict (Prefect serialization) or as the
    # dataclass when called in-process. Handle both.
    if hasattr(job_input, "input"):
        metadata = job_input.metadata
        resource_id = job_input.resource_id
    else:
        metadata = (job_input.get("input") or {}).get("metadata", {}) or {}
        resource_id = job_input.get("resource_id", "")
    ignore_hash = bool(metadata.get("ignore_hash", False))
    if ignore_hash:
        # Operator explicitly asked for a fresh fetch; do not cache.
        return None
    url = metadata.get("original_url") or metadata.get("ckan_url", "")
    return f"dpp:download:{resource_id}:{url}"


def content_cache_key(context, parameters) -> Optional[str]:
    """Cache by the content fingerprint propagated through the chain.

    Each downstream result dataclass exposes a ``file_hash`` field that
    each task copies forward from its input. When the upstream content is
    identical across runs, every read-only stage finds its cached output
    and skips the qsv subprocess.

    Returns ``None`` (no cache) when the propagated hash is missing —
    safer than caching by path, which would silently miss across runs.
    """
    prev = parameters.get("prev")
    if prev is None:
        return None
    fh = getattr(prev, "file_hash", None)
    if not fh:
        return None
    task_name = getattr(context.task, "name", "task")
    return f"dpp:{task_name}:{fh}"
