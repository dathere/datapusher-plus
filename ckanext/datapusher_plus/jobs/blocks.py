# -*- coding: utf-8 -*-
"""
Prefect Block registration for DataPusher+.

Prefect Blocks hold typed, named, optionally-encrypted configuration that
survives across flow runs and worker restarts. DP+ uses one Block at
v3.0 launch:

* **Result storage** — a ``LocalFileSystem`` (default) or S3/GCS block
  pointing at where Prefect persists each task's result for "re-run from
  failed task". Operators can swap to S3 for multi-host worker pools
  without touching DP+ code.

Operators run ``ckan datapusher_plus prefect-deploy`` once during setup;
that command calls :func:`ensure_result_storage_block` to register the
default block if it does not already exist.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Optional

log = logging.getLogger(__name__)

DEFAULT_BLOCK_NAME = "datapusher-plus-results"
DEFAULT_LOCAL_PATH = "~/.prefect/datapusher-plus-results"


def _block_name() -> str:
    try:
        import ckan.plugins.toolkit as tk

        return tk.config.get(
            "ckanext.datapusher_plus.result_storage_block",
            f"local-file-system/{DEFAULT_BLOCK_NAME}",
        )
    except Exception:
        return os.environ.get(
            "DATAPUSHER_PLUS_RESULT_STORAGE_BLOCK",
            f"local-file-system/{DEFAULT_BLOCK_NAME}",
        )


def ensure_result_storage_block(local_path: Optional[str] = None) -> str:
    """
    Idempotently register a ``LocalFileSystem`` Block for result persistence.

    Returns the block identifier (``<type-slug>/<name>``) that callers can
    pass to ``@task(result_storage=...)``.
    """
    name = _block_name()
    try:
        from prefect.filesystems import LocalFileSystem

        if "/" in name:
            _, bare = name.split("/", 1)
        else:
            bare = name
        path = Path(local_path or DEFAULT_LOCAL_PATH).expanduser()
        path.mkdir(parents=True, exist_ok=True)
        LocalFileSystem(basepath=str(path)).save(name=bare, overwrite=False)
        log.info("Registered Prefect result storage block: %s", name)
    except ValueError:
        # Block already exists — Prefect raises ValueError when overwrite=False.
        log.debug("Result storage block %s already exists", name)
    except Exception as e:
        log.warning("Could not register result storage block %s: %s", name, e)
    return name


def load_result_storage_block() -> Any:
    """
    Load the configured result-storage block for use as
    ``@task(result_storage=...)``. Returns ``None`` if unavailable so the
    flow falls back to Prefect's default (in-memory) result handling.
    """
    name = _block_name()
    try:
        from prefect.filesystems import LocalFileSystem

        return LocalFileSystem.load(name.split("/", 1)[-1])
    except Exception as e:
        log.debug("Result storage block %s not available: %s", name, e)
        return None
