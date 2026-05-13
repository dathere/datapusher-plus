# -*- coding: utf-8 -*-
"""
Runtime context types for the Prefect-orchestrated pipeline.

The v3.0 cutover splits the v2 monolithic ``ProcessingContext`` into three
families of types, each with a clear role in a Prefect flow:

1. ``JobInput`` — frozen, JSON-serializable dataclass passed to
   ``datapusher_plus_flow`` as its parameter. This is what shows up in the
   Prefect UI and what Prefect uses to identify, cache, and re-run flow runs.

2. ``RuntimeContext`` — the per-flow-run mutable state holding non-
   serializable handles (logger, ``QSVCommand``, temp dir). Built inside the
   flow before any task executes and bound to a ``ContextVar`` so tasks
   access it without it crossing a serialization boundary.

   For the v3.0 release we deliberately keep ``RuntimeContext`` as an alias
   for the v2 ``ProcessingContext`` so the eight existing stage bodies can
   be reused unchanged — they already accept exactly this shape. The
   alias makes the *role* of the type explicit at use sites; future PRs
   can progressively retire the legacy fields as stages adopt the typed-
   result pattern below.

3. Per-stage result dataclasses — small typed payloads returned by each
   ``@task``. They give the Prefect run graph meaningful, inspectable
   outputs and serve as the persisted artifacts that power
   "re-run from failed task".
"""

from __future__ import annotations

import contextvars
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from ckanext.datapusher_plus.jobs.context import ProcessingContext

# ``RuntimeContext`` is the role-name for the mutable per-run state. We
# alias it to ``ProcessingContext`` so stage signatures (``process(ctx:
# ProcessingContext)``) continue to type-check without changes.
RuntimeContext = ProcessingContext


# ---------------------------------------------------------------------------
# Flow parameter
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class JobInput:
    """
    Serializable payload that ``datapusher_plus_flow`` receives as its
    ``job_input`` parameter.

    Keep this small and JSON-friendly: Prefect persists it in the flow-run
    record so it is visible in the UI and reusable for re-runs and
    automations.
    """

    task_id: str
    resource_id: str
    ckan_url: str
    input: Dict[str, Any]
    dry_run: bool = False

    @property
    def metadata(self) -> Dict[str, Any]:
        """Convenience accessor matching the legacy ``ProcessingContext`` API."""
        return self.input.get("metadata", {})


# ---------------------------------------------------------------------------
# ContextVar binding for the per-run RuntimeContext
# ---------------------------------------------------------------------------
#
# Using a ContextVar instead of a module-global is what makes concurrent
# flow runs in the same process (the default ``ThreadPoolTaskRunner``)
# work correctly: each flow run gets its own context binding.

_runtime_var: contextvars.ContextVar[RuntimeContext] = contextvars.ContextVar(
    "datapusher_plus_runtime"
)


def set_runtime_context(ctx: RuntimeContext) -> contextvars.Token:
    """Bind a ``RuntimeContext`` to the current flow run. Returns a reset token."""
    return _runtime_var.set(ctx)


def get_runtime_context() -> RuntimeContext:
    """
    Retrieve the current flow run's ``RuntimeContext``.

    Raises ``LookupError`` if called outside a flow run — tasks must be
    invoked through the flow so the binding exists.
    """
    return _runtime_var.get()


def reset_runtime_context(token: contextvars.Token) -> None:
    """Pair with ``set_runtime_context``; restores the previous binding."""
    _runtime_var.reset(token)


# ---------------------------------------------------------------------------
# Per-stage result types
# ---------------------------------------------------------------------------
#
# Each ``@task`` in ``prefect_flow.py`` returns one of these. They are:
#
#   * Small — only data downstream tasks actually need.
#   * Typed — so the run graph in the UI is meaningful.
#   * Serializable — primitives, dicts, lists. No file handles, no loggers,
#     no subprocess wrappers.
#   * Cache-friendly — result hashes drive Prefect's caching, so equal
#     inputs across runs produce equal cached outputs.


@dataclass(frozen=True)
class DownloadResult:
    """Output of the download task."""

    resource: Dict[str, Any]
    resource_url: str
    file_hash: str
    content_length: int
    downloaded_path: str


@dataclass(frozen=True)
class ConvertResult:
    """Output of the format-converter task (Excel/ODS/Shapefile/GeoJSON → CSV)."""

    csv_path: str
    # e.g. "xlsx", "shp", or None for pass-through.
    converted_from: Optional[str] = None
    # Content fingerprint propagated from DownloadResult so downstream
    # tasks can cache by file identity rather than by tempdir path.
    file_hash: str = ""  # e.g. "xlsx", "shp", or None for pass-through


@dataclass(frozen=True)
class ValidateResult:
    """Output of the validation task, including quarantine info."""

    csv_path: str
    rows_after_dedup: int
    quarantined_rows: int = 0
    quarantine_csv_path: Optional[str] = None
    # Propagated content fingerprint for downstream caching.
    file_hash: str = ""


@dataclass(frozen=True)
class AnalyzeResult:
    """Output of the qsv-driven analysis task."""

    headers: List[str]
    headers_dicts: List[Dict[str, Any]]
    original_header_dict: Dict[int, str]
    dataset_stats: Dict[str, Any]
    resource_fields_stats: Dict[str, Any]
    resource_fields_freqs: Dict[str, Any]
    pii_found: bool
    # Propagated content fingerprint for downstream caching.
    file_hash: str = ""


@dataclass(frozen=True)
class DatabaseResult:
    """Output of the database-load (COPY) task."""

    rows_to_copy: int
    copied_count: int
    existing_info: Optional[Dict[str, Any]] = None


@dataclass(frozen=True)
class IndexingResult:
    """Output of the auto-indexing task."""

    indexes_created: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class FormulaResult:
    """Output of the Jinja2 formula-evaluation task."""

    formula_outputs: Dict[str, Any] = field(default_factory=dict)
    suggestions: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MetadataResult:
    """Output of the final metadata-update task. Terminal."""

    alias_created: Optional[str] = None
    updated_fields: List[str] = field(default_factory=list)
