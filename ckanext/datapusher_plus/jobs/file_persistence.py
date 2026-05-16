# -*- coding: utf-8 -*-
"""
Persist task working files to the Prefect result-storage block so
cached task results stay valid across runs.

The result dataclasses in ``runtime_context`` carry working file paths
(``downloaded_path``, ``csv_path``, ``quarantine_csv_path``) that point
into a per-run ``TemporaryDirectory``. When a downstream stage hits a
*cached* result from an earlier run, those paths reference a tempdir
that no longer exists — so the consuming stage cannot read the file.

This module's two helpers solve that:

* ``persist_file`` writes a local file to the configured result-storage
  block under a stable, content-hash-keyed name at task completion.
* ``restore_file`` fetches that file back into the current run's
  tempdir when ``_apply_result`` rehydrates a cached result whose
  recorded tempdir path no longer exists.

Both helpers degrade gracefully: when the storage block isn't
available (no Prefect server, block not yet registered, tooling
context), ``persist_file`` returns ``None`` and ``restore_file``
returns ``False``. Callers treat that as "no persistence happened"
and continue with the in-tempdir copy — same behaviour as before
caching was re-enabled.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

from ckanext.datapusher_plus.jobs.blocks import load_result_storage_block

log = logging.getLogger(__name__)


# Skip persistence for files larger than this many MiB. ``persist_file``
# reads the whole file into RAM before handing it to the Prefect block
# (Prefect 3's ``LocalFileSystem.write_path`` / ``read_path`` are
# bytes-in / bytes-out — no streaming variant), so on multi-GB CSV
# ingestions a single task completion could OOM the worker. Skipping
# above the threshold falls back to the pre-caching behaviour for
# those files (same-run chains still work; cross-run cache hits don't
# rehydrate the file and the downstream stage will surface a clear
# ``FileNotFoundError`` instead of running out of memory).
#
# Operators override via ``DATAPUSHER_PLUS_MAX_PERSIST_FILE_MB``; set
# to ``0`` to disable the cap entirely (only sensible on workers with
# RAM to spare). The 512 MiB default is chosen so a CSV that fits in
# Postgres' default ``work_mem`` × a typical worker box also fits in
# RAM for one round-trip.
def _max_persist_bytes() -> int:
    raw = os.environ.get("DATAPUSHER_PLUS_MAX_PERSIST_FILE_MB", "512")
    try:
        mb = int(raw)
    except ValueError:
        mb = 512
    if mb <= 0:
        return 0  # disabled — persist everything
    return mb * 1024 * 1024


def persist_file(local_path: str, key: str) -> Optional[str]:
    """Write a local file's contents to the result-storage block.

    Args:
        local_path: Filesystem path to the file to persist.
        key: Stable identifier to store it under (typically derived
            from the content hash + stage name).

    Returns:
        ``key`` on success, or ``None`` if the block is unavailable,
        the file exceeds the configured size cap, or the write failed.
        Callers should treat ``None`` as "no persistence recorded" —
        the result dataclass leaves its ``*_path_key`` field at
        ``None`` and cross-run rehydration falls back to the in-tempdir
        copy (which works for same-run chains, just not cache hits).
    """
    block = load_result_storage_block()
    if block is None:
        return None
    try:
        size = os.path.getsize(local_path)
    except OSError:
        log.warning("Could not persist file %s: not found", local_path)
        return None
    cap = _max_persist_bytes()
    if cap and size > cap:
        log.debug(
            "Skipping persistence of %s (%d bytes > %d-byte cap); set "
            "DATAPUSHER_PLUS_MAX_PERSIST_FILE_MB=0 to disable the cap.",
            local_path,
            size,
            cap,
        )
        return None
    try:
        with open(local_path, "rb") as fh:
            data = fh.read()
        block.write_path(key, data)
        log.debug("Persisted file %s to result storage as %s", local_path, key)
        return key
    except FileNotFoundError:
        # Race: file existed at getsize() but was removed before open().
        # Rare in practice but possible if a parallel tempdir cleanup
        # fires; treat as "not persisted" rather than crashing the task.
        log.warning("Could not persist file %s: not found", local_path)
        return None
    except Exception as e:
        # ``write_path`` can fail on a permission / disk / network
        # error. Log and degrade — the caller still gets a working
        # result for the same-run case; only cross-run cache hits
        # lose their persisted file.
        log.warning("Could not persist file %s as %s: %s", local_path, key, e)
        return None


def restore_file(key: str, dest_path: str) -> bool:
    """Fetch a previously-persisted file from result storage.

    Args:
        key: Storage key returned by an earlier ``persist_file`` call.
        dest_path: Local filesystem path to write the contents to.
            Parent directory must already exist (typically the current
            run's tempdir, which always exists at rehydration time).

    Returns:
        ``True`` if the file was restored, ``False`` if the storage
        block is unavailable, the key is missing, or the write failed.
        Callers treat ``False`` as "rehydration skipped" — the cached
        result's path field is left as-is and the downstream stage
        will fail with a clear ``FileNotFoundError`` rather than
        appearing to succeed on stale data.
    """
    block = load_result_storage_block()
    if block is None:
        return False
    try:
        data = block.read_path(key)
        with open(dest_path, "wb") as fh:
            fh.write(data)
        log.debug("Restored file %s from result storage key %s", dest_path, key)
        return True
    except Exception as e:
        # ``read_path`` raises a generic exception for "not found" in
        # most Prefect block backends. Treat any failure here as
        # "not restorable" — the caller falls back to letting the
        # downstream stage surface a clear missing-file error.
        log.debug("Could not restore file %s from %s: %s", dest_path, key, e)
        return False
