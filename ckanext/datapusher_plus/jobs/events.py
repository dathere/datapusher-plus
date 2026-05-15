# -*- coding: utf-8 -*-
"""
Custom Prefect events emitted by DataPusher+ flows.

These events are the contract between DP+ and downstream Prefect
Automations. Operators wire them up in the Prefect UI to drive Slack
alerts, search reindexes, DCAT refreshes, etc., without DP+ hard-coding
any specific alerting backend.

Event names follow the ``<namespace>.<noun>.<verb>`` convention Prefect
recommends. The DP+ namespace is ``datapusher``.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

log = logging.getLogger(__name__)


def _safe_emit(event: str, resource_id: str, payload: Dict[str, Any]) -> None:
    """Best-effort emit; never let a failed event fail the flow."""
    try:
        from prefect.events import emit_event

        emit_event(
            event=event,
            resource={
                "prefect.resource.id": f"datapusher.resource.{resource_id}",
                "ckan.resource.id": resource_id,
            },
            payload=payload,
        )
    except Exception as e:  # pragma: no cover - observability is best-effort
        log.warning("Failed to emit Prefect event %s: %s", event, e)


def emit_resource_ingested(
    *, resource_id: str, rows: int, file_hash: str, duration_seconds: float
) -> None:
    """Fires once on successful flow completion."""
    _safe_emit(
        "datapusher.resource.ingested",
        resource_id,
        {
            "rows": rows,
            "file_hash": file_hash,
            "duration_seconds": duration_seconds,
        },
    )


def emit_rows_quarantined(
    *, resource_id: str, quarantined: int, total: int
) -> None:
    """Fires when the validation task captures bad rows above 0."""
    _safe_emit(
        "datapusher.row.quarantined",
        resource_id,
        {"quarantined": quarantined, "total": total},
    )


def emit_pii_detected(
    *, resource_id: str, fields: list[str], details: Optional[Dict[str, Any]] = None
) -> None:
    """Fires when PII screening finds candidate fields."""
    _safe_emit(
        "datapusher.pii.detected",
        resource_id,
        {"fields": fields, "details": details or {}},
    )
