# flake8: noqa: E501
"""Persist a resource's Data Dictionary across a DP+ job so it can be
restored on failure.

Why this exists
---------------
The analysis stage captures a resource's existing per-field ``info``
dicts (label / description / type_override — the "Data Dictionary") into
``ProcessingContext.existing_info``, then deletes the existing datastore
resource so the database stage can re-create + COPY into it. If any
later stage fails, the in-memory snapshot dies with the worker and the
operator's annotations are gone forever — see the docstring on
``_rollback_database`` in ``jobs/prefect_flow.py``, which explicitly
calls out this hole:

    (The earlier ``existing_info`` branch claimed to "preserve" the
     original, but the delete had already destroyed it.)

This module closes it. On entry to the database stage, the dictionary is
written to a small JSON file keyed by ``resource_id``. On a flow
rollback, ``_rollback_database`` looks for that file and restores the
dictionary by re-creating the datastore resource with the original
``info`` dicts (and zero rows — the data is gone, but the *annotations*
the operator carefully built up are preserved). On flow success the
file is deleted.

Why a file (not the DP+ DB)
---------------------------
The ``Metadata`` model would also work, but adds a transaction inside
the Prefect task body for a write that is only ever read by the rollback
on the *same* worker. A file in a configurable directory keeps the path
simple, supports inspection by ops, and survives the worker-crash case
(/tmp is per-host, persists across container restarts unless the host
itself reboots — the failure mode #265 targets). For deployments where
/tmp is wiped on restart, set ``ckanext.datapusher_plus.dictionary_stash_dir``
to a persistent location.

Public API: ``save``, ``load``, ``clear``, ``stash_path``.
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
from typing import Any, Dict, Optional

import ckan.plugins.toolkit as tk

_LOG = logging.getLogger(__name__)


def _stash_dir() -> str:
    """Resolve the stash directory, creating it if missing.

    Lazy (per-call) rather than module-level so tests can set the config
    via ``ckan.plugins.toolkit.config`` without re-importing the module.
    """
    configured = tk.config.get("ckanext.datapusher_plus.dictionary_stash_dir")
    base = configured or os.path.join(tempfile.gettempdir(), "dpp_dict_stash")
    os.makedirs(base, exist_ok=True)
    return base


def stash_path(resource_id: str) -> str:
    """Return the absolute path to the stash file for ``resource_id``.

    Does not check existence — callers use ``load`` for that.
    """
    if not resource_id:
        raise ValueError("resource_id is required")
    # Guard against path traversal via maliciously crafted resource_id —
    # CKAN resource ids are UUIDs in practice, but the contract here is
    # "any string", so reject separators rather than silently writing to
    # an unexpected location.
    if os.sep in resource_id or (os.altsep and os.altsep in resource_id):
        raise ValueError(f"resource_id contains a path separator: {resource_id!r}")
    return os.path.join(_stash_dir(), f"{resource_id}.json")


def save(resource_id: str, info_by_field: Dict[str, Dict[str, Any]]) -> None:
    """Write the per-field ``info`` dicts for ``resource_id`` to the stash.

    ``info_by_field`` is the shape produced by the analysis stage:
    ``{field_id: {"label": ..., "type_override": ..., ...}}``. Writing
    an empty dict is allowed (no-op semantically; explicit "no
    dictionary to stash") and produces a file containing ``{}``.

    Overwrites any pre-existing stash for the same ``resource_id`` —
    a previous failed run's stash is superseded by the latest attempt's
    snapshot.
    """
    path = stash_path(resource_id)
    # Write to a temp file in the same directory, then rename. atomic on
    # POSIX; on Windows the rename is best-effort but the failure mode
    # (partial JSON on disk) is detected by load() returning None on
    # JSONDecodeError.
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(info_by_field, f)
    os.replace(tmp_path, path)
    _LOG.debug(
        "dictionary_stash.save: wrote %d field(s) for resource %s to %s",
        len(info_by_field),
        resource_id,
        path,
    )


def load(resource_id: str) -> Optional[Dict[str, Dict[str, Any]]]:
    """Read the stash for ``resource_id``; return ``None`` if absent.

    A corrupt stash (malformed JSON) is treated as absent and logged at
    warning level — restoring nothing is strictly better than crashing
    the rollback hook on a bad file we wrote ourselves.
    """
    path = stash_path(resource_id)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        _LOG.warning(
            "dictionary_stash.load: corrupt stash for resource %s at %s (%s); "
            "treating as absent",
            resource_id,
            path,
            e,
        )
        return None


def clear(resource_id: str) -> None:
    """Delete the stash for ``resource_id`` if present.

    Idempotent — clearing a non-existent stash is a no-op, not an
    error. The on-success path calls this; we want it to be safe to
    call even when ``save`` was never called for this run.
    """
    path = stash_path(resource_id)
    try:
        os.remove(path)
        _LOG.debug(
            "dictionary_stash.clear: removed stash for resource %s at %s",
            resource_id,
            path,
        )
    except FileNotFoundError:
        return
    except OSError as e:
        # Surface but do not raise — the worst case is a stale stash
        # file that the next run will overwrite anyway.
        _LOG.warning(
            "dictionary_stash.clear: could not remove %s: %s",
            path,
            e,
        )
