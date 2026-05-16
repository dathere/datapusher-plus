# -*- coding: utf-8 -*-
"""
Result-persistence and cache-key configuration for the Prefect flow.

**Result persistence** (``persist_result=True`` + ``result_storage=...``)
is active on every task: outputs are checkpointed so operators can
open a task's output from the Prefect UI to debug bad data.

**Content-based caching is enabled** on the read-only stages
(``download_task``, ``format_convert_task``, ``validate_task``,
``analyze_task``); the cache-key functions below build keys from the
content fingerprint propagated through the result chain, so identical
content across runs hits the cache and skips the qsv subprocess. The
mutating stages (``database_task``, ``indexing_task``,
``formula_task``, ``metadata_task``) intentionally do NOT cache —
caching the result would skip the side effect.

The original blocker (each per-stage result dataclass needs to be
self-contained so a cache *hit* — which skips the task body —
doesn't leave a downstream stage reading empty state) was cleared in
PR #280: every task ``rehydrate``-s the ``RuntimeContext`` from its
input result before running its stage.

The second blocker (cached results carried tempdir paths that didn't
survive to the next run) was cleared in PR #286: each file-carrying
result dataclass now stores a stable ``*_path_key`` alongside its
local path; ``file_persistence.persist_file`` writes the working file
to the result-storage block at task completion, and the rehydration
path (``runtime_context._resolve_or_restore``) fetches it back into
the current run's tempdir on a cross-run cache hit.
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


# Default result-storage configuration is left to Prefect — it will use its
# built-in local filesystem path (``~/.prefect/storage``) when this is None.
#
# Deliberately NOT loaded at module import: calling ``LocalFileSystem.load(...)``
# here would trigger a Prefect API call, and Prefect spins up a temporary
# server when ``PREFECT_API_URL`` is unset (which happens during
# ``ckan db init`` and other CKAN admin commands that import DP+'s plugin).
# That temp-server bootstrap pollutes stdout with log lines and breaks any
# tool that pipes CKAN output (e.g., ``ckan datastore set-permissions | psql``).
#
# Operators wanting a specific block (S3/GCS for multi-host pools) wire it in
# via ``prefect.yaml`` deployment job_variables or a custom ``@task`` decorator
# in their custom flow.
DEFAULT_RESULT_STORAGE: Optional[Any] = None


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

    The read-only-stage results (``ConvertResult``, ``ValidateResult``,
    ``AnalyzeResult``) expose a ``file_hash`` property that walks the
    ``upstream`` chain to the root ``DownloadResult`` — these are the
    candidates for content caching. ``DatabaseResult`` exposes it too for
    chain-traversal symmetry, though the database task is side-effecting
    and not itself cached. The terminal side-effecting results
    (``IndexingResult`` / ``FormulaResult`` / ``MetadataResult``)
    intentionally don't expose ``file_hash``: caching their outputs would
    skip the actual side effect.

    When the upstream content is identical across runs, every read-only
    stage finds its cached output and skips the qsv subprocess.

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


# ---------------------------------------------------------------------------
# Composed cache policies
# ---------------------------------------------------------------------------
#
# Prefect 3.4+ recommends ``cache_policy=...`` over the legacy
# ``cache_key_fn=`` shorthand. Wrapping our custom keys in
# ``CacheKeyFnPolicy`` lets us compose them with ``TASK_SOURCE`` — when
# operators upgrade DP+ and a task body changes, the source-hash
# component invalidates stale caches automatically. Without this, a
# resubmit after upgrading would happily reuse output produced by the
# old code.
#
# Loaded lazily so the module still imports when Prefect 3.4's
# ``cache_policies`` module isn't available (older Prefect 3.x or
# tooling contexts).

# We deliberately do NOT compose with ``TASK_SOURCE`` here. A naive
# ``CacheKeyFnPolicy(cache_key_fn=key_fn) + TASK_SOURCE`` has a
# correctness hazard: Prefect 3's ``CompoundCachePolicy.compute_key``
# drops ``None`` contributions and hashes the rest, so when ``key_fn``
# returns ``None`` (no file_hash propagated yet, or operator passed
# ``ignore_hash=True``) the compound key collapses to just the
# TASK_SOURCE hash — identical for every invocation of that task with
# the same task source, regardless of parameters. Two unrelated runs
# with no content key would hit each other's cache. Verified
# empirically against Prefect 3.7.
#
# Without TASK_SOURCE, a DP+ upgrade that changes a task body does NOT
# automatically invalidate older cached output. Operators relying on
# fresh output after an upgrade should either delete the result-storage
# Block contents or shorten ``DATAPUSHER_PLUS_CACHE_TTL_HOURS``. The
# default 24h expiration bounds the staleness window.
try:
    from prefect.cache_policies import CacheKeyFnPolicy

    DOWNLOAD_CACHE_POLICY = CacheKeyFnPolicy(cache_key_fn=download_cache_key)
    CONTENT_CACHE_POLICY = CacheKeyFnPolicy(cache_key_fn=content_cache_key)
except ImportError as e:  # pragma: no cover - older Prefect 3.x
    # Only a missing module is an expected fallback. A narrower catch
    # than bare ``Exception`` so a real bug (e.g. a bad CacheKeyFnPolicy
    # call, an incompatible ``+`` override) propagates instead of being
    # silently swallowed into "caching disabled".
    log.debug("Falling back to bare cache_key_fn (cache_policies unavailable): %s", e)
    DOWNLOAD_CACHE_POLICY = None  # type: ignore
    CONTENT_CACHE_POLICY = None  # type: ignore
