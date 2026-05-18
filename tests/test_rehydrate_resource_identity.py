# -*- coding: utf-8 -*-
"""
Regression coverage for issue #311.

When ``analyze_task`` / ``validate_task`` / ``format_convert_task``
cache-hit by file content across resources (same CSV bytes uploaded
as two different CKAN resources), the cached upstream chain points
to the FIRST flow's ``DownloadResult`` — which carries the FIRST
flow's resource dict. Pre-#311, ``_apply_result`` for ``DownloadResult``
did ``ctx.resource = dict(result.resource)``, wholesale replacing the
current flow's resource dict (set by ``_build_runtime_context`` from
CKAN) with the cached one.

Symptom: ``DatabaseStage._create_datastore_table`` then uses
``context.resource["id"]`` (the OLD resource_id, from the cache) for
``datastore_create`` — a no-op since that table already exists from
the previous run. ``_copy_data`` uses ``context.resource_id`` (the
NEW resource_id, set fresh by ``_build_runtime_context``) for
TRUNCATE, which fails with ``Postgres COPY failed: relation
"<new_resource_id>" does not exist``.

The fix: don't apply ``result.resource`` on rehydrate. Resource
identity is per-flow-run state, not per-file-content state.

These tests pin three properties:

1. ``ctx.resource`` is NOT overwritten by a ``DownloadResult``
   rehydrate (the central #311 contract).
2. The content-related fields ``ctx.file_hash``, ``ctx.resource_url``,
   ``ctx.content_length``, ``ctx.tmp`` ARE applied — those are
   genuinely content-derived and need to flow through the chain.
3. A multi-layer chain (e.g. ``AnalyzeResult → ValidateResult →
   ConvertResult → DownloadResult``) still leaves ``ctx.resource``
   untouched — this is the bug case where the cache hit happens on
   a downstream stage.
"""

from __future__ import annotations

import os
from typing import Any, Dict, List
from unittest import mock

import pytest


@pytest.fixture
def context_factory(tmp_path):
    """Build a minimal ProcessingContext with a known resource dict.

    Mirrors what ``_build_runtime_context`` produces: the resource
    dict for the CURRENT flow's resource, fetched from CKAN.
    """
    pytest.importorskip("ckan")
    from ckanext.datapusher_plus.jobs.context import ProcessingContext

    def _make(resource_id: str = "new-resource-id") -> ProcessingContext:
        return ProcessingContext(
            task_id="task-1",
            input={},
            dry_run=False,
            temp_dir=str(tmp_path),
            logger=mock.Mock(),
            qsv=mock.Mock(),
            resource={
                "id": resource_id,
                "package_id": "new-package",
                "name": "current.csv",
                "url": "http://current/current.csv",
            },
            resource_id=resource_id,
            ckan_url="http://current",
        )

    return _make


def _make_download_result(resource_id: str, file_hash: str, downloaded_path: str):
    """Build a DownloadResult as if from a different flow.

    The ``resource`` dict here intentionally has a DIFFERENT id than
    the context fixture's — this is the cross-resource cache-hit
    scenario the bug surfaced.
    """
    from ckanext.datapusher_plus.jobs.runtime_context import DownloadResult

    return DownloadResult(
        resource={
            "id": resource_id,
            "package_id": "old-package",
            "name": "old.csv",
            "url": "http://old/old.csv",
            "hash": file_hash,
        },
        resource_url="http://old/old.csv",
        file_hash=file_hash,
        content_length=64,
        downloaded_path=downloaded_path,
        downloaded_path_key=None,
    )


# ---------------------------------------------------------------------------
# Central #311 contract: ctx.resource is NOT clobbered by rehydrate.
# ---------------------------------------------------------------------------


def test_rehydrate_does_not_overwrite_ctx_resource(context_factory, tmp_path):
    # The bug case in 4 lines: ctx starts with the CURRENT flow's
    # resource ("new-resource-id"); rehydrate is given a cached
    # DownloadResult from a different flow ("old-resource-id");
    # afterward ctx.resource MUST still be the current flow's.
    from ckanext.datapusher_plus.jobs.runtime_context import rehydrate

    ctx = context_factory(resource_id="new-resource-id")
    original_resource = dict(ctx.resource)

    downloaded_path = str(tmp_path / "file.csv")
    with open(downloaded_path, "wb") as f:
        f.write(b"id,name\n1,Alice\n")
    cached_dl = _make_download_result(
        resource_id="old-resource-id",
        file_hash="blake3-deadbeef",
        downloaded_path=downloaded_path,
    )

    rehydrate(ctx, cached_dl)

    # The whole point: ctx.resource is untouched.
    assert ctx.resource == original_resource
    assert ctx.resource["id"] == "new-resource-id"
    assert ctx.resource["package_id"] == "new-package"


def test_rehydrate_applies_content_fields_from_download_result(
    context_factory, tmp_path
):
    # Belt-and-braces: the content-derived fields MUST still flow
    # through the chain. Without this we'd lose the ability to do
    # cross-run cache hits on the same resource (which is the whole
    # reason the cache exists).
    from ckanext.datapusher_plus.jobs.runtime_context import rehydrate

    ctx = context_factory()
    downloaded_path = str(tmp_path / "file.csv")
    with open(downloaded_path, "wb") as f:
        f.write(b"id,name\n1,Alice\n")
    cached_dl = _make_download_result(
        resource_id="any-resource",
        file_hash="blake3-deadbeef",
        downloaded_path=downloaded_path,
    )

    rehydrate(ctx, cached_dl)

    assert ctx.file_hash == "blake3-deadbeef"
    assert ctx.resource_url == "http://old/old.csv"
    assert ctx.content_length == 64
    # tmp gets resolved from the persisted file (or the original path
    # when no key — the fixture passes None for the key).
    assert ctx.tmp == downloaded_path


# ---------------------------------------------------------------------------
# Multi-layer chain: the bug case (cache hit on analyze, walks chain).
# ---------------------------------------------------------------------------


def test_rehydrate_through_multi_layer_chain_preserves_ctx_resource(
    context_factory, tmp_path
):
    # The realistic bug case: ``database_task`` calls _stage_run with
    # a cached AnalyzeResult; rehydrate walks the chain
    # AnalyzeResult → ValidateResult → ConvertResult → DownloadResult
    # and at the root applies the cached DownloadResult.
    #
    # Without the fix: ctx.resource would be replaced with the
    # cached resource (old_resource_id). With the fix: ctx.resource
    # stays as the current flow's (new_resource_id).
    from ckanext.datapusher_plus.jobs.runtime_context import (
        AnalyzeResult,
        ConvertResult,
        ValidateResult,
        rehydrate,
    )

    ctx = context_factory(resource_id="new-resource-id")
    original_resource = dict(ctx.resource)

    # Build the cached chain from a previous flow with a different
    # resource_id but identical content.
    downloaded_path = str(tmp_path / "file.csv")
    csv_path = str(tmp_path / "converted.csv")
    for p in (downloaded_path, csv_path):
        with open(p, "wb") as f:
            f.write(b"id,name\n1,Alice\n")

    dl = _make_download_result(
        resource_id="old-resource-id",
        file_hash="blake3-deadbeef",
        downloaded_path=downloaded_path,
    )
    conv = ConvertResult(
        upstream=dl,
        csv_path=csv_path,
        converted_from=None,
        csv_path_key=None,
    )
    val = ValidateResult(
        upstream=conv,
        csv_path=csv_path,
        rows_after_dedup=1,
        quarantined_rows=0,
        quarantine_csv_path=None,
        csv_path_key=None,
        quarantine_csv_path_key=None,
    )
    ana = AnalyzeResult(
        upstream=val,
        csv_path=csv_path,
        headers=["id", "name"],
        headers_dicts=[
            {"id": "id", "type": "numeric", "info": {}},
            {"id": "name", "type": "text", "info": {}},
        ],
        original_header_dict={0: "id", 1: "name"},
        dataset_stats={"RECORD_COUNT": 1},
        resource_fields_stats={},
        resource_fields_freqs={},
        pii_found=False,
        pii_candidate_count=0,
        csv_path_key=None,
    )

    rehydrate(ctx, ana)

    # The whole #311 contract: ctx.resource stays as the current
    # flow's resource, NOT the cached one from upstream.
    assert ctx.resource == original_resource
    assert ctx.resource["id"] == "new-resource-id"
    # And the content-derived fields propagated through the chain.
    assert ctx.file_hash == "blake3-deadbeef"
    assert ctx.headers == ["id", "name"]
    assert len(ctx.headers_dicts) == 2
    # ValidateResult set rows_to_copy.
    assert ctx.rows_to_copy == 1


# ---------------------------------------------------------------------------
# Hash field still ends up on ctx.resource via the metadata stage path.
# ---------------------------------------------------------------------------


def test_file_hash_propagates_via_ctx_file_hash_not_via_resource_dict(
    context_factory, tmp_path
):
    # Belt-and-braces for the safety story: ``ctx.file_hash`` is the
    # source of truth for "what hash should land on the resource".
    # The #310 fix in MetadataStage reads it from there and writes
    # to ``ctx.resource["hash"]`` AFTER the re-fetch — so even
    # though we no longer carry the hash through ctx.resource via
    # rehydrate, the persisted resource record still gets the right
    # hash.
    from ckanext.datapusher_plus.jobs.runtime_context import rehydrate

    ctx = context_factory()
    # ctx.resource starts without a hash field (mirrors what
    # ``_build_runtime_context`` produces on a brand-new upload).
    assert "hash" not in ctx.resource

    downloaded_path = str(tmp_path / "file.csv")
    with open(downloaded_path, "wb") as f:
        f.write(b"id,name\n1,Alice\n")

    cached_dl = _make_download_result(
        resource_id="any-resource",
        file_hash="blake3-cafebabe",
        downloaded_path=downloaded_path,
    )

    rehydrate(ctx, cached_dl)

    # ctx.file_hash is set (the metadata stage will read it).
    assert ctx.file_hash == "blake3-cafebabe"
    # ctx.resource still doesn't have a hash — the metadata stage's
    # #310 fix copies ctx.file_hash to ctx.resource["hash"] later in
    # the flow, after its own re-fetch.
    assert "hash" not in ctx.resource
