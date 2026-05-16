# -*- coding: utf-8 -*-
"""
Per-domain Prefect subflows for custom flow composition.

These wrap DP+'s existing in-process helpers (`screen_for_pii`,
`process_spatial_file`) in `@flow`-decorated entry points so a custom
flow can compose them as **subflows** of the main ingestion flow.

What a subflow buys you over inlining the helper:

* **Independent observability** — each subflow run gets its own row
  in the Prefect UI run tree (visible under the parent flow run),
  with its own logs, runtime, and final state. Operators can see at
  a glance whether the spatial-conversion or PII-screening step is
  what's slow / failing.
* **Independent retries** — Prefect's `@flow(retries=N)` applies to
  each subflow run separately, so a transient failure in spatial
  conversion can retry without re-running the whole ingestion.
* **Independent concurrency limits** — operators can tag the
  subflow's deployment with a Prefect concurrency limit (e.g.,
  "no more than 2 GeoJSON simplifications in parallel") without
  capping the rest of the pipeline.
* **Independent versioning** — when a custom flow author wants to
  swap in their own PII-screening logic, they replace the call to
  `pii_screening_subflow` with their own subflow; the rest of the
  pipeline (and DP+ upgrades) stay decoupled from that choice.

The **default** DP+ flow does NOT call these subflows; it inlines the
underlying helpers inside its existing task bodies (kept that way to
avoid the regression risk of restructuring the working pipeline).
Custom-flow authors who want the benefits above import these from
their own flow module — see the "Custom flows" section of README.md.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Optional, Union

from prefect import flow, get_run_logger

from ckanext.datapusher_plus.pii_screening import screen_for_pii
from ckanext.datapusher_plus.qsv_utils import QSVCommand
from ckanext.datapusher_plus.spatial_helpers import process_spatial_file


def _runtime_logger() -> logging.Logger:
    """Return Prefect's run-scoped logger when invoked inside a flow,
    falling back to a module logger when imported into a context with
    no Prefect runtime (e.g., a tooling import, a doctest)."""
    try:
        return get_run_logger()
    except Exception:
        return logging.getLogger(__name__)


@flow(
    name="dpp-pii-screening",
    log_prints=True,
)
def pii_screening_subflow(
    csv_path: Union[str, Path],
    resource: Dict[str, Any],
    temp_dir: Union[str, Path],
    qsv_bin: Optional[str] = None,
) -> Dict[str, Any]:
    """Screen a CSV for PII via ``screen_for_pii``, as an independently
    observable Prefect subflow.

    Args:
        csv_path: Path to the CSV to screen.
        resource: CKAN resource dict (``screen_for_pii`` reads
            ``resource['url']`` / formats from this).
        temp_dir: Per-run scratch directory the screening logic can
            write its intermediate files into. Typically the parent
            flow's ``TemporaryDirectory``; passing it through avoids
            the subflow inventing a new one and losing artifacts on
            success.
        qsv_bin: Optional explicit path to the qsv binary. When ``None``,
            ``QSVCommand`` resolves it from ``ckanext.datapusher_plus
            .qsv_bin`` / ``$QSV_BIN``.

    Returns:
        Dict with two keys:

        * ``pii_found`` (bool): whether any PII candidate matched.
        * ``pii_candidate_count`` (int): exact match count under full
          screening; degenerate ``1`` under quick-screen mode (the
          inner helper only knows presence, not count, in that mode).

        Dict (rather than a dataclass) for Prefect serialization
        simplicity at the subflow boundary — Prefect's result encoding
        handles built-ins without extra schema work.
    """
    log = _runtime_logger()
    qsv = QSVCommand(logger=log)
    pii_found, pii_count = screen_for_pii(
        str(csv_path), resource, qsv, str(temp_dir), log
    )
    log.info(
        f"PII screening complete: pii_found={pii_found}, "
        f"candidate_count={pii_count}"
    )
    return {"pii_found": pii_found, "pii_candidate_count": pii_count}


@flow(
    name="dpp-spatial-processing",
    log_prints=True,
)
def spatial_processing_subflow(
    input_path: Union[str, Path],
    resource_format: str,
    output_csv_path: Optional[Union[str, Path]] = None,
    tolerance: float = 0.001,
) -> Dict[str, Any]:
    """Convert a spatial file (zipped Shapefile, GeoJSON, etc.) to CSV
    via ``process_spatial_file``, as an independently observable
    Prefect subflow.

    Args:
        input_path: Path to the input spatial file. May be a zipped
            Shapefile, a ``.shp``, or a GeoJSON.
        resource_format: Format string from the CKAN resource (``"SHP"``,
            ``"GEOJSON"``, ``"QGIS"``, etc.) — used to dispatch within
            the underlying helper.
        output_csv_path: Destination CSV path. When ``None``, the
            helper writes alongside ``input_path`` with a ``.csv``
            extension.
        tolerance: Geometry simplification tolerance, as a fraction
            of the geometry's diagonal (``0.001`` = 0.1%). Lower
            values preserve detail at the cost of CSV size.

    Returns:
        Dict with:

        * ``success`` (bool)
        * ``error_message`` (Optional[str]): set when ``success`` is
          ``False``.
        * ``bounds`` (Optional[List[float]]): ``[minx, miny, maxx,
          maxy]`` when ``success`` is ``True``, else ``None``. List
          (not tuple) for Prefect-serialization compatibility — JSON
          has no tuples.
    """
    log = _runtime_logger()
    success, error_message, bounds = process_spatial_file(
        input_path,
        resource_format,
        output_csv_path,
        tolerance,
        log,
    )
    if success:
        log.info(
            f"Spatial conversion complete: bounds={bounds}, "
            f"output={output_csv_path}"
        )
    else:
        log.error(f"Spatial conversion failed: {error_message}")
    return {
        "success": success,
        "error_message": error_message,
        # Tuple -> list for JSON-encodable result. ``None`` passes
        # through unchanged.
        "bounds": list(bounds) if bounds is not None else None,
    }
