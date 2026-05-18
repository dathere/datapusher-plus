# -*- coding: utf-8 -*-
"""
Regression coverage for issue #310:
``MetadataStage._update_resource_metadata`` was dropping the per-file
hash that ``DownloadStage`` computed because the re-fetch added by
``d653129c`` ("persist preview/preview_rows after datastore_create")
wholesale replaced ``context.resource`` with the on-disk CKAN copy —
which still had an empty hash at that point in the flow.

The bug was briefly masked because the resulting empty hash looked
the same regardless of which algorithm DP+ used internally; PR
#309's BLAKE3-default smoke test caught it via the
"CKAN persists a 64-hex digest" assertion.

These tests pin the contract:

1. When ``context.file_hash`` is set (the normal path), the hash
   survives the re-fetch and appears on the resource dict passed to
   ``dsu.update_resource``.
2. When ``context.file_hash`` is empty (corner case — e.g. dedup
   skipped the download, or an upstream bug), the re-fetched
   resource's hash is left alone — i.e. we don't overwrite a real
   stored hash with an empty string.
3. Other fields the method sets (``datastore_active``,
   ``total_record_count``) still land on the persisted resource —
   proving the fix didn't break the original
   "persist preview/preview_rows after datastore_create" intent.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest import mock

import pytest


@pytest.fixture
def metadata_stage():
    pytest.importorskip("ckan")
    from ckanext.datapusher_plus.jobs.stages.metadata import MetadataStage

    return MetadataStage()


@pytest.fixture
def build_context():
    """Build a minimal ProcessingContext the method actually reads.

    ``_update_resource_metadata`` touches: ``context.resource``
    (re-fetched, then mutated), ``context.resource_id``,
    ``context.dataset_stats`` (for RECORD_COUNT), ``context.copied_count``,
    ``context.file_hash``, ``context.logger``, and calls
    ``_maybe_write_csv_spatial_extent(context)`` (which we no-op via
    patch). ``SimpleNamespace`` is enough — the real ProcessingContext
    would pull in Prefect runtime scaffolding the test doesn't need.
    """

    def _make(file_hash="abc123", record_count=5):
        return SimpleNamespace(
            resource_id="res-aaa",
            # The pre-flow resource dict the test fixture would
            # produce; replaced wholesale by ``dsu.get_resource``
            # inside the method.
            resource={},
            dataset_stats={"RECORD_COUNT": record_count},
            copied_count=record_count,
            file_hash=file_hash,
            logger=mock.Mock(),
        )

    return _make


# ---------------------------------------------------------------------------
# Issue #310 — hash survives the re-fetch
# ---------------------------------------------------------------------------


def test_hash_from_context_file_hash_lands_on_persisted_resource(
    metadata_stage, build_context, monkeypatch
):
    # The central #310 contract: a DownloadStage that set
    # ``context.file_hash`` MUST end up with that hash on the
    # CKAN-facing resource record. The re-fetch above the fix
    # discards whatever was on ``context.resource``, so the fix has
    # to restore the hash from ``context.file_hash``.
    from ckanext.datapusher_plus.jobs.stages import metadata as md_mod

    ctx = build_context(file_hash="blake3-deadbeef")

    # The re-fetch returns a dict with NO hash — exactly the state on
    # CKAN before our update fires.
    refetched = {
        "id": "res-aaa",
        "package_id": "pkg-xxx",
        "name": "smoke.csv",
        # No ``hash`` key — simulates the regression scenario.
    }
    monkeypatch.setattr(md_mod.dsu, "get_resource", lambda rid: dict(refetched))

    # Capture what ``update_resource`` receives.
    captured = {}
    monkeypatch.setattr(
        md_mod.dsu, "update_resource", lambda r: captured.update({"r": dict(r)})
    )
    # Stub the spatial-extent helper — out of scope here.
    monkeypatch.setattr(
        metadata_stage,
        "_maybe_write_csv_spatial_extent",
        lambda ctx: None,
    )
    # ``conf.PREVIEW_ROWS`` is read inside the method — patch it to a
    # known value rather than depending on ckan.ini.
    monkeypatch.setattr(md_mod.conf, "PREVIEW_ROWS", 1000)

    metadata_stage._update_resource_metadata(ctx)

    assert captured["r"]["hash"] == "blake3-deadbeef"
    # And the original "fix preview/preview_rows" intent still holds.
    assert captured["r"]["datastore_active"] is True
    assert captured["r"]["total_record_count"] == 5


def test_empty_context_file_hash_does_not_clobber_refetched_hash(
    metadata_stage, build_context, monkeypatch
):
    # Corner case: dedup skipped the download (so ``context.file_hash``
    # is empty), but CKAN already has a real hash on the resource
    # from a previous run. The fix must NOT overwrite that stored
    # hash with an empty string — leaving the existing hash alone
    # is strictly better than wiping it.
    from ckanext.datapusher_plus.jobs.stages import metadata as md_mod

    ctx = build_context(file_hash="")

    refetched = {
        "id": "res-aaa",
        "package_id": "pkg-xxx",
        "name": "smoke.csv",
        "hash": "previously-stored-hash",
    }
    monkeypatch.setattr(md_mod.dsu, "get_resource", lambda rid: dict(refetched))

    captured = {}
    monkeypatch.setattr(
        md_mod.dsu, "update_resource", lambda r: captured.update({"r": dict(r)})
    )
    monkeypatch.setattr(
        metadata_stage, "_maybe_write_csv_spatial_extent", lambda ctx: None
    )
    monkeypatch.setattr(md_mod.conf, "PREVIEW_ROWS", 1000)

    metadata_stage._update_resource_metadata(ctx)

    # Pre-existing hash is preserved (NOT overwritten with the empty
    # context.file_hash).
    assert captured["r"]["hash"] == "previously-stored-hash"


def test_context_file_hash_overrides_stale_refetched_hash(
    metadata_stage, build_context, monkeypatch
):
    # Normal-but-overlapping case: CKAN's stored hash is from a prior
    # ingest of different content, and the current run computed a
    # fresh hash. The current run's hash MUST win — that's what the
    # whole DownloadStage→MetadataStage chain is for.
    from ckanext.datapusher_plus.jobs.stages import metadata as md_mod

    ctx = build_context(file_hash="fresh-blake3-digest")

    refetched = {
        "id": "res-aaa",
        "package_id": "pkg-xxx",
        "name": "smoke.csv",
        "hash": "stale-md5-from-previous-ingest",
    }
    monkeypatch.setattr(md_mod.dsu, "get_resource", lambda rid: dict(refetched))

    captured = {}
    monkeypatch.setattr(
        md_mod.dsu, "update_resource", lambda r: captured.update({"r": dict(r)})
    )
    monkeypatch.setattr(
        metadata_stage, "_maybe_write_csv_spatial_extent", lambda ctx: None
    )
    monkeypatch.setattr(md_mod.conf, "PREVIEW_ROWS", 1000)

    metadata_stage._update_resource_metadata(ctx)

    assert captured["r"]["hash"] == "fresh-blake3-digest"
