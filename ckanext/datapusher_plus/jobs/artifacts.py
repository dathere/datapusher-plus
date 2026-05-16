# -*- coding: utf-8 -*-
"""
Prefect artifacts for DataPusher+ flow runs.

Artifacts attach human-readable summaries to a Prefect flow run page.
They serve two audiences:

* Operators investigating a specific run see the data-quality snapshot
  and any quarantined-row details without leaving the Prefect UI.
* Dataset owners get a one-click link back to the resource page in CKAN.

Helpers fail soft: if Prefect's artifact API is unavailable (e.g. running
the flow directly as a Python function in unit tests), they log a warning
and continue rather than failing the flow.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)


def _safe(call):
    """Decorator: swallow artifact API errors so a failed artifact never fails the flow."""

    def wrapper(*args, **kwargs):
        try:
            return call(*args, **kwargs)
        except Exception as e:
            log.warning("Failed to create Prefect artifact in %s: %s", call.__name__, e)
            return None

    wrapper.__name__ = call.__name__
    return wrapper


@_safe
def create_data_quality_artifact(
    *,
    resource_id: str,
    rows: int,
    headers: List[Dict[str, Any]],
    pii_found: bool,
    quarantined_rows: int,
    formula_outputs: Optional[Dict[str, Any]] = None,
) -> str:
    """Attach a Markdown summary of what the flow ingested."""
    from prefect.artifacts import create_markdown_artifact

    lines = [
        f"# Data Quality Report — `{resource_id}`",
        "",
        f"- **Rows ingested:** {rows:,}",
        f"- **Columns:** {len(headers)}",
        f"- **PII detected:** {'yes' if pii_found else 'no'}",
        f"- **Rows quarantined:** {quarantined_rows:,}",
    ]
    if formula_outputs:
        lines += ["", "## Formula outputs", ""]
        for key, value in formula_outputs.items():
            lines.append(f"- `{key}`: {value!r}")

    if headers:
        lines += ["", "## Inferred schema", "", "| Column | Type |", "|---|---|"]
        for h in headers:
            name = h.get("id") or h.get("name") or "?"
            typ = h.get("type", "?")
            lines.append(f"| `{name}` | `{typ}` |")

    return create_markdown_artifact(
        key=f"dpp-data-quality-{resource_id}",
        markdown="\n".join(lines),
        description=f"DataPusher+ ingestion summary for resource {resource_id}",
    )


@_safe
def create_quarantine_artifact(
    *,
    resource_id: str,
    quarantined_rows: int,
    total_rows: int,
    csv_path: str,
) -> str:
    """Attach a Markdown summary of rows that failed validation."""
    from prefect.artifacts import create_markdown_artifact

    pct = (quarantined_rows / total_rows * 100) if total_rows else 0.0
    md = (
        f"# Quarantine Report — `{resource_id}`\n\n"
        f"- **Rows quarantined:** {quarantined_rows:,} of {total_rows:,} "
        f"({pct:.2f}%)\n"
        f"- **Quarantine CSV (worker-local path):** `{csv_path}`\n\n"
        "Rejected rows are not loaded into the datastore. Inspect the "
        "quarantine CSV on the worker host to see the original lines and "
        "their parse errors."
    )
    return create_markdown_artifact(
        key=f"dpp-quarantine-{resource_id}",
        markdown=md,
        description=f"Quarantined rows for resource {resource_id}",
    )


@_safe
def create_resource_link_artifact(*, ckan_url: str, resource_id: str) -> str:
    """One-click link back to the CKAN resource page."""
    from prefect.artifacts import create_link_artifact

    href = ckan_url.rstrip("/") + f"/dataset/resource_data/{resource_id}"
    return create_link_artifact(
        key=f"dpp-resource-{resource_id}",
        link=href,
        description=f"View resource {resource_id} in CKAN",
    )
