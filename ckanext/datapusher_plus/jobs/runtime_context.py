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
#   * Self-contained — every result nests the result of the stage before
#     it (``upstream``), so the whole chain of cross-task state is reachable
#     from any single result. This is what lets ``rehydrate`` (below)
#     reconstitute the ``RuntimeContext`` from a result alone: a cache hit
#     or a persisted-result replay skips a task body, so the body's
#     mutations to the shared context are *not* something a downstream
#     stage can rely on — the result chain is.
#   * Typed — so the run graph in the UI is meaningful.
#   * Serializable — primitives, dicts, lists. No file handles, no loggers,
#     no subprocess wrappers.


@dataclass(frozen=True)
class DownloadResult:
    """Output of the download task. Root of the result chain."""

    resource: Dict[str, Any]
    resource_url: str
    file_hash: str
    content_length: int
    downloaded_path: str


@dataclass(frozen=True)
class ConvertResult:
    """Output of the format-converter task (Excel/ODS/Shapefile/GeoJSON → CSV)."""

    upstream: DownloadResult
    csv_path: str
    # e.g. "xlsx", "shp", or None for pass-through.
    converted_from: Optional[str] = None

    @property
    def file_hash(self) -> str:
        """Content fingerprint, carried from the root ``DownloadResult``."""
        return self.upstream.file_hash


@dataclass(frozen=True)
class ValidateResult:
    """Output of the validation task, including quarantine info."""

    upstream: ConvertResult
    csv_path: str
    rows_after_dedup: int
    quarantined_rows: int = 0
    quarantine_csv_path: Optional[str] = None

    @property
    def file_hash(self) -> str:
        """Content fingerprint, carried from the root ``DownloadResult``."""
        return self.upstream.file_hash


@dataclass(frozen=True)
class AnalyzeResult:
    """Output of the qsv-driven analysis task."""

    upstream: ValidateResult
    # Working CSV path as analysis left it — what the database COPY reads.
    csv_path: str
    headers: List[str]
    headers_dicts: List[Dict[str, Any]]
    original_header_dict: Dict[int, str]
    dataset_stats: Dict[str, Any]
    resource_fields_stats: Dict[str, Any]
    resource_fields_freqs: Dict[str, Any]
    pii_found: bool
    # Count of PII candidate matches — what the PII-review suspend gate
    # thresholds on.
    pii_candidate_count: int = 0

    @property
    def file_hash(self) -> str:
        """Content fingerprint, carried from the root ``DownloadResult``."""
        return self.upstream.file_hash


@dataclass(frozen=True)
class DatabaseResult:
    """Output of the database-load (COPY) task."""

    upstream: AnalyzeResult
    rows_to_copy: int
    copied_count: int
    existing_info: Optional[Dict[str, Any]] = None

    @property
    def file_hash(self) -> str:
        """Content fingerprint, carried from the root ``DownloadResult``."""
        return self.upstream.file_hash


@dataclass(frozen=True)
class IndexingResult:
    """Output of the auto-indexing task."""

    upstream: DatabaseResult
    indexes_created: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class FormulaResult:
    """Output of the Jinja2 formula-evaluation task."""

    upstream: IndexingResult
    formula_outputs: Dict[str, Any] = field(default_factory=dict)
    suggestions: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MetadataResult:
    """Output of the final metadata-update task. Terminal."""

    upstream: FormulaResult
    alias_created: Optional[str] = None
    updated_fields: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Rehydration: result chain -> RuntimeContext
# ---------------------------------------------------------------------------
#
# The per-stage result dataclasses above are the *source of truth* for
# state that crosses a task boundary. ``rehydrate`` walks a nested result
# chain root-first and writes each layer's fields back onto the
# ContextVar-bound ``RuntimeContext``, so a stage sees correct ``ctx``
# state even when an upstream task's body never ran (a Prefect cache hit,
# or a persisted-result replay on a flow re-run). The stage bodies in
# ``jobs/stages/*.py`` are reused unmodified — they still read and write
# ``ctx``; they just no longer *depend* on a previous task body having
# mutated the shared context in this process.


def _apply_result(ctx: RuntimeContext, result: Any) -> None:
    """Apply one result layer's fields onto ``ctx``.

    Linear ``isinstance`` dispatch keeps every result-to-context mapping
    visible in one place. Only stages whose output a later stage reads
    back from ``ctx`` need a branch here; ``IndexingResult`` /
    ``FormulaResult`` / ``MetadataResult`` carry nothing a downstream
    stage consumes via the context, so they have none.
    """
    if isinstance(result, DownloadResult):
        # Defensive copy: later stages mutate ``ctx.resource`` in place
        # (e.g. ``FormulaStage`` adds ``dpp_suggestions``), so without a
        # copy here that mutation would also mutate the ``DownloadResult``
        # stored in the result chain — making it no longer a true snapshot
        # of the download stage's output.
        ctx.resource = dict(result.resource)
        ctx.resource_url = result.resource_url
        ctx.file_hash = result.file_hash
        ctx.content_length = result.content_length
        ctx.tmp = result.downloaded_path
    elif isinstance(result, ConvertResult):
        ctx.tmp = result.csv_path
    elif isinstance(result, ValidateResult):
        ctx.tmp = result.csv_path
        ctx.rows_to_copy = result.rows_after_dedup
        ctx.quarantined_rows = result.quarantined_rows
        ctx.quarantine_csv_path = result.quarantine_csv_path or ""
    elif isinstance(result, AnalyzeResult):
        ctx.tmp = result.csv_path
        ctx.headers = list(result.headers)
        ctx.headers_dicts = list(result.headers_dicts)
        ctx.original_header_dict = dict(result.original_header_dict)
        ctx.dataset_stats = dict(result.dataset_stats)
        ctx.resource_fields_stats = dict(result.resource_fields_stats)
        ctx.resource_fields_freqs = dict(result.resource_fields_freqs)
        ctx.pii_found = result.pii_found
        ctx.pii_candidate_count = result.pii_candidate_count
    elif isinstance(result, DatabaseResult):
        ctx.rows_to_copy = result.rows_to_copy
        ctx.copied_count = result.copied_count
        # Defensive copy — same rationale as ``DownloadResult.resource``.
        ctx.existing_info = (
            dict(result.existing_info) if result.existing_info else None
        )


def rehydrate(ctx: RuntimeContext, result: Any) -> None:
    """Reconstitute ``ctx`` from a (possibly nested) result chain.

    Walks ``result.upstream`` to the root, then applies each layer
    root-first so later stages' values win (e.g. each stage's ``csv_path``
    overwrites the previous ``ctx.tmp``). A task calls this before running
    its stage; see the module note above.
    """
    chain: List[Any] = []
    node: Any = result
    while node is not None:
        chain.append(node)
        node = getattr(node, "upstream", None)
    for node in reversed(chain):
        _apply_result(ctx, node)
