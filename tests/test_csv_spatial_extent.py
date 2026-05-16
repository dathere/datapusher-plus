# -*- coding: utf-8 -*-
"""
Unit coverage for the CSV ``dpp_spatial_extent`` persistence path.

Re-implements the CSV slice of PR #253 against the current Prefect
pipeline. For shapefile / GeoJSON inputs ``FormatConverterStage``
already writes ``dpp_spatial_extent`` on the simplified resource it
uploads. For plain-CSV resources with lat/lon columns the bbox was
previously only derived at jinja2 render-time
(``spatial_extent_wkt``); ``MetadataStage._maybe_write_csv_spatial_extent``
now persists it on the resource dict.

Co-authored with @minhajuddin2510 (original feature from PR #253).
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest import mock

import pytest


def _stats_row(field_type: str, min_v, max_v):
    return {"stats": {"type": field_type, "min": min_v, "max": max_v}}


@pytest.fixture
def helpers():
    pytest.importorskip("ckan")
    from ckanext.datapusher_plus import jinja2_helpers as j2h

    return j2h


@pytest.fixture
def stage():
    pytest.importorskip("ckan")
    pytest.importorskip("psycopg2")
    from ckanext.datapusher_plus.jobs.stages.metadata import MetadataStage

    return MetadataStage()


@pytest.fixture
def context_factory():
    """Build a minimal ProcessingContext stand-in.

    Methods called by ``_maybe_write_csv_spatial_extent`` are limited
    to ``context.resource`` (dict), ``context.resource_fields_stats``
    (dict), and ``context.logger`` (warning / info). A ``SimpleNamespace``
    suffices — pulling in the real ProcessingContext (or a Prefect
    runtime) would balloon this unit test into an integration test.
    """

    def _make(resource=None, stats=None):
        return SimpleNamespace(
            resource=resource if resource is not None else {},
            resource_fields_stats=stats if stats is not None else {},
            logger=mock.Mock(),
        )

    return _make


# ---------- detect_lat_lon_fields ----------------------------------


def test_detect_lat_lon_fields_matches_canonical_names(helpers):
    stats = {
        "latitude": _stats_row("Float", -45.0, 45.0),
        "longitude": _stats_row("Float", -120.0, 120.0),
        "name": _stats_row("String", "a", "z"),
    }
    assert helpers.detect_lat_lon_fields(stats) == ("latitude", "longitude")


def test_detect_lat_lon_fields_case_insensitive(helpers):
    stats = {
        "Lat": _stats_row("Float", -10.0, 10.0),
        "Lon": _stats_row("Float", -10.0, 10.0),
    }
    # Original case preserved in the return values.
    assert helpers.detect_lat_lon_fields(stats) == ("Lat", "Lon")


def test_detect_lat_lon_rejects_out_of_range_values(helpers):
    """A Float column called ``latitude`` whose values run from 0–1000 is
    not real-world latitude; refuse to misidentify it."""
    stats = {
        "latitude": _stats_row("Float", 0.0, 1000.0),
        "longitude": _stats_row("Float", -120.0, 120.0),
    }
    assert helpers.detect_lat_lon_fields(stats) == (None, "longitude")


def test_detect_lat_lon_rejects_non_float_type(helpers):
    stats = {
        "latitude": _stats_row("Integer", -45, 45),
        "longitude": _stats_row("Float", -120.0, 120.0),
    }
    assert helpers.detect_lat_lon_fields(stats) == (None, "longitude")


def test_detect_lat_lon_returns_none_for_missing(helpers):
    assert helpers.detect_lat_lon_fields({}) == (None, None)


def test_detect_lat_lon_handles_bad_min_max(helpers):
    """Bad / missing min/max in the stats dict shouldn't raise — just
    fail to match. Guards against partial qsv stats output."""
    stats = {
        "latitude": {"stats": {"type": "Float"}},  # no min/max keys
        "longitude": _stats_row("Float", "not-a-number", "also-not"),
    }
    assert helpers.detect_lat_lon_fields(stats) == (None, None)


# ---------- MetadataStage._maybe_write_csv_spatial_extent -----------


def test_maybe_write_csv_spatial_extent_writes_bbox(stage, context_factory):
    """Happy path: lat/lon columns detected, valid stats, no prior
    extent. Resource dict gains ``dpp_spatial_extent`` in the same
    BoundingBox shape as the FormatConverterStage path."""
    ctx = context_factory(
        stats={
            "lat": _stats_row("Float", 40.5, 41.5),
            "lon": _stats_row("Float", -74.5, -73.5),
        }
    )
    with mock.patch.object(
        __import__(
            "ckanext.datapusher_plus.config", fromlist=["AUTO_CSV_SPATIAL_EXTENT"]
        ),
        "AUTO_CSV_SPATIAL_EXTENT",
        True,
    ):
        stage._maybe_write_csv_spatial_extent(ctx)

    assert ctx.resource["dpp_spatial_extent"] == {
        "type": "BoundingBox",
        "coordinates": [[-74.5, 40.5], [-73.5, 41.5]],
    }
    ctx.logger.info.assert_called()


def test_maybe_write_csv_spatial_extent_respects_disable_flag(
    stage, context_factory
):
    ctx = context_factory(
        stats={
            "lat": _stats_row("Float", 40.5, 41.5),
            "lon": _stats_row("Float", -74.5, -73.5),
        }
    )
    with mock.patch.object(
        __import__(
            "ckanext.datapusher_plus.config", fromlist=["AUTO_CSV_SPATIAL_EXTENT"]
        ),
        "AUTO_CSV_SPATIAL_EXTENT",
        False,
    ):
        stage._maybe_write_csv_spatial_extent(ctx)

    assert "dpp_spatial_extent" not in ctx.resource


def test_maybe_write_csv_spatial_extent_preserves_existing(stage, context_factory):
    """If the resource already has ``dpp_spatial_extent`` (e.g. a
    shapefile-simplified resource), we must not overwrite it."""
    existing = {"type": "BoundingBox", "coordinates": [[0, 0], [1, 1]]}
    ctx = context_factory(
        resource={"dpp_spatial_extent": existing},
        stats={
            "lat": _stats_row("Float", 40.5, 41.5),
            "lon": _stats_row("Float", -74.5, -73.5),
        },
    )
    with mock.patch.object(
        __import__(
            "ckanext.datapusher_plus.config", fromlist=["AUTO_CSV_SPATIAL_EXTENT"]
        ),
        "AUTO_CSV_SPATIAL_EXTENT",
        True,
    ):
        stage._maybe_write_csv_spatial_extent(ctx)

    assert ctx.resource["dpp_spatial_extent"] is existing


def test_maybe_write_csv_spatial_extent_no_lat_lon_columns(stage, context_factory):
    """CSV without lat/lon columns → no key added (not present, not None)."""
    ctx = context_factory(
        stats={"name": _stats_row("String", "a", "z")},
    )
    with mock.patch.object(
        __import__(
            "ckanext.datapusher_plus.config", fromlist=["AUTO_CSV_SPATIAL_EXTENT"]
        ),
        "AUTO_CSV_SPATIAL_EXTENT",
        True,
    ):
        stage._maybe_write_csv_spatial_extent(ctx)

    assert "dpp_spatial_extent" not in ctx.resource


def test_maybe_write_csv_spatial_extent_empty_stats(stage, context_factory):
    """No stats yet → no-op (don't raise)."""
    ctx = context_factory(stats={})
    with mock.patch.object(
        __import__(
            "ckanext.datapusher_plus.config", fromlist=["AUTO_CSV_SPATIAL_EXTENT"]
        ),
        "AUTO_CSV_SPATIAL_EXTENT",
        True,
    ):
        stage._maybe_write_csv_spatial_extent(ctx)

    assert "dpp_spatial_extent" not in ctx.resource
