# -*- coding: utf-8 -*-
"""
Unit coverage for the file-persistence layer that backs the re-enabled
result caching: ``file_persistence.persist_file`` /
``file_persistence.restore_file`` and the ``_resolve_or_restore``
rehydration helper in ``runtime_context``.

These tests stub the result-storage block with an in-memory fake so
they run anywhere CKAN is importable (the ``dpp-test`` container, CI)
without needing a real Prefect server.
"""

from __future__ import annotations

from pathlib import Path
from unittest import mock

import pytest


# ---------------------------------------------------------------------------
# Fake result-storage block — drop-in for what ``load_result_storage_block``
# returns. Mimics Prefect's ``LocalFileSystem.{read_path,write_path}`` API.
# ---------------------------------------------------------------------------


class _FakeBlock:
    """Minimal in-memory stand-in for a Prefect storage block."""

    def __init__(self):
        self.store: dict = {}

    def write_path(self, key: str, content: bytes) -> None:
        self.store[key] = content

    def read_path(self, key: str) -> bytes:
        if key not in self.store:
            raise FileNotFoundError(key)
        return self.store[key]


# ---------------------------------------------------------------------------
# persist_file / restore_file
# ---------------------------------------------------------------------------


def test_persist_then_restore_round_trips_file_contents(tmp_path):
    """Happy path: write contents, key returned, restore reads them back
    identically into a new path."""
    pytest.importorskip("ckan")
    from ckanext.datapusher_plus.jobs import file_persistence

    src = tmp_path / "input.csv"
    src.write_bytes(b"col1,col2\n1,2\n3,4\n")

    block = _FakeBlock()
    with mock.patch.object(
        file_persistence, "load_result_storage_block", return_value=block
    ):
        key = file_persistence.persist_file(str(src), "dpp:files:abc:download:downloaded")
        assert key == "dpp:files:abc:download:downloaded"
        assert "dpp:files:abc:download:downloaded" in block.store

        dst = tmp_path / "restored.csv"
        ok = file_persistence.restore_file(key, str(dst))
        assert ok is True
        assert dst.read_bytes() == src.read_bytes()


def test_persist_returns_none_when_block_unavailable(tmp_path):
    """When ``load_result_storage_block`` returns ``None`` (no Prefect
    server / unregistered block / tooling context), persist must
    degrade silently with ``None`` so the caller can leave the result's
    storage-key field at ``None`` and same-run chains still work."""
    pytest.importorskip("ckan")
    from ckanext.datapusher_plus.jobs import file_persistence

    src = tmp_path / "x.csv"
    src.write_bytes(b"...")

    with mock.patch.object(
        file_persistence, "load_result_storage_block", return_value=None
    ):
        assert file_persistence.persist_file(str(src), "any-key") is None


def test_persist_returns_none_when_file_missing(tmp_path):
    """A missing local file is handled gracefully (logged, returns
    ``None``) — never raises."""
    pytest.importorskip("ckan")
    from ckanext.datapusher_plus.jobs import file_persistence

    block = _FakeBlock()
    with mock.patch.object(
        file_persistence, "load_result_storage_block", return_value=block
    ):
        result = file_persistence.persist_file(
            str(tmp_path / "does-not-exist.csv"), "k"
        )
    assert result is None
    assert block.store == {}


def test_restore_returns_false_for_missing_key(tmp_path):
    """A missing storage key is handled gracefully (returns ``False``)
    so the caller can fall back to letting the downstream stage raise
    a clear ``FileNotFoundError``."""
    pytest.importorskip("ckan")
    from ckanext.datapusher_plus.jobs import file_persistence

    block = _FakeBlock()
    with mock.patch.object(
        file_persistence, "load_result_storage_block", return_value=block
    ):
        ok = file_persistence.restore_file(
            "nonexistent-key", str(tmp_path / "dest.csv")
        )
    assert ok is False


# ---------------------------------------------------------------------------
# _resolve_or_restore (the rehydration helper in runtime_context)
# ---------------------------------------------------------------------------


def test_resolve_returns_local_path_when_file_exists(tmp_path):
    """Same-run case: the recorded path still exists, no restore needed."""
    pytest.importorskip("ckan")
    from ckanext.datapusher_plus.jobs.runtime_context import _resolve_or_restore

    existing = tmp_path / "in_tempdir.csv"
    existing.write_bytes(b"still here")

    result = _resolve_or_restore(str(existing), "some-key", str(tmp_path))
    assert result == str(existing)


def test_resolve_restores_from_storage_when_path_missing(tmp_path):
    """Cross-run cache-hit case: the recorded path is gone (old tempdir
    cleaned up), but the storage key resolves — restore into the
    current tempdir and return the new path."""
    pytest.importorskip("ckan")
    from ckanext.datapusher_plus.jobs import file_persistence
    from ckanext.datapusher_plus.jobs.runtime_context import _resolve_or_restore

    # Seed the fake block with a saved version of the file.
    block = _FakeBlock()
    block.store["k1"] = b"persisted contents"

    # The "old" tempdir path doesn't exist on this machine.
    old_path = str(tmp_path / "old_tempdir" / "stage_out.csv")
    assert not Path(old_path).exists()

    # The "current" tempdir does — that's where the restored file lands.
    current_tempdir = tmp_path / "current_tempdir"
    current_tempdir.mkdir()

    with mock.patch.object(
        file_persistence, "load_result_storage_block", return_value=block
    ):
        result = _resolve_or_restore(old_path, "k1", str(current_tempdir))

    expected = current_tempdir / "stage_out.csv"
    assert result == str(expected)
    assert expected.read_bytes() == b"persisted contents"


def test_resolve_returns_original_path_when_neither_local_nor_storage(tmp_path):
    """No-fallback case: tempdir path gone AND storage key missing /
    unavailable. Return the original path so the downstream stage
    raises a clear ``FileNotFoundError`` instead of silently appearing
    to succeed on stale data."""
    pytest.importorskip("ckan")
    from ckanext.datapusher_plus.jobs import file_persistence
    from ckanext.datapusher_plus.jobs.runtime_context import _resolve_or_restore

    block = _FakeBlock()  # empty
    missing_path = str(tmp_path / "gone.csv")

    with mock.patch.object(
        file_persistence, "load_result_storage_block", return_value=block
    ):
        result = _resolve_or_restore(missing_path, "absent-key", str(tmp_path))

    assert result == missing_path  # unchanged


def test_resolve_passes_through_none(tmp_path):
    """``None`` / empty inputs pass through unchanged — common for
    optional fields like ``quarantine_csv_path`` when validation
    rejected zero rows."""
    pytest.importorskip("ckan")
    from ckanext.datapusher_plus.jobs.runtime_context import _resolve_or_restore

    assert _resolve_or_restore(None, "any", str(tmp_path)) is None
    assert _resolve_or_restore("", "any", str(tmp_path)) == ""
