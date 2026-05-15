# -*- coding: utf-8 -*-
"""
Bad-row quarantine for the validation task.

The v2 pipeline aborts the entire job on the first RFC-4180 violation. In
v3.0 the validation task can instead route rejected rows to a sibling
CSV and continue with the clean subset, as long as the quarantine rate
stays below ``ckanext.datapusher_plus.max_quarantine_pct`` (default 5%).
Beyond that threshold the flow fails fast with a clear error.

This module is intentionally small: it holds the threshold check and the
sibling-CSV writer. The actual qsv-validate invocation that produces
rejected rows lives in ``stages/validation.py`` and feeds this helper.
"""

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path
from typing import Optional

import ckanext.datapusher_plus.utils as utils
from ckanext.datapusher_plus.jobs.events import emit_rows_quarantined

log = logging.getLogger(__name__)


def _max_quarantine_pct() -> float:
    """
    Operator-overridable threshold (percent of input rows).

    Reads CKAN config first, env var as fallback so the value is
    accessible from both web and worker processes.
    """
    try:
        import ckan.plugins.toolkit as tk

        value = tk.config.get("ckanext.datapusher_plus.max_quarantine_pct")
        if value is not None:
            return float(value)
    except Exception:
        pass
    return float(os.environ.get("DATAPUSHER_PLUS_MAX_QUARANTINE_PCT", "5.0"))


def apply_quarantine(
    *,
    resource_id: str,
    clean_csv_path: str,
    quarantine_csv_path: Optional[str],
    quarantined_rows: int,
    total_rows: int,
) -> Optional[str]:
    """
    Enforce the quarantine threshold and emit observability signals.

    Args:
        resource_id: For event/artifact correlation.
        clean_csv_path: Path to the cleaned CSV (what downstream tasks load).
        quarantine_csv_path: Path to the sibling CSV containing rejected rows,
            or ``None`` if no rows were rejected.
        quarantined_rows: Count of rejected rows.
        total_rows: Count of rows considered (clean + quarantined).

    Returns:
        The quarantine CSV path (possibly relocated to a stable location)
        or ``None`` if nothing was quarantined.

    Raises:
        utils.JobError: if the quarantine rate exceeds the configured
            threshold. Downstream tasks will not run.
    """
    if quarantined_rows == 0 or not quarantine_csv_path:
        return None

    pct = (quarantined_rows / total_rows * 100) if total_rows else 0.0
    threshold = _max_quarantine_pct()

    if pct > threshold:
        raise utils.JobError(
            f"Quarantine rate {pct:.2f}% exceeds threshold {threshold:.2f}% "
            f"({quarantined_rows}/{total_rows} rows rejected). "
            "Aborting — fix the source file or raise "
            "ckanext.datapusher_plus.max_quarantine_pct."
        )

    log.info(
        "Quarantined %d/%d rows (%.2f%%) for resource %s",
        quarantined_rows, total_rows, pct, resource_id,
    )
    emit_rows_quarantined(
        resource_id=resource_id, quarantined=quarantined_rows, total=total_rows
    )
    return quarantine_csv_path


def stash_quarantine_csv(*, source: str, dest_dir: str, resource_id: str) -> str:
    """
    Copy the quarantine CSV out of the per-run temp_dir into a longer-lived
    location so operators can inspect it after the flow finishes.

    Returns the destination path.
    """
    Path(dest_dir).mkdir(parents=True, exist_ok=True)
    dest = Path(dest_dir) / f"{resource_id}-quarantine.csv"
    shutil.copy2(source, dest)
    return str(dest)
