# -*- coding: utf-8 -*-
"""
Unit coverage for ``MetadataStage._blank_date_means``.

Regression test for the bug originally reported in #254 by
@avdata99: qsv reports a Date/DateTime column's ``mean`` as an
ISO date string (e.g. ``"2025-03-01"``), but the summary-stats
table declares ``mean FLOAT`` so a direct COPY trips
``invalid input syntax for type double precision``.
"""

from __future__ import annotations

import csv
from unittest import mock

import pytest


@pytest.fixture
def stage():
    """Import the metadata stage lazily — its top-level imports need
    psycopg2 and the CKAN-dependent helpers."""
    pytest.importorskip("ckan")
    pytest.importorskip("psycopg2")
    from ckanext.datapusher_plus.jobs.stages.metadata import MetadataStage

    return MetadataStage()


def _write_stats_csv(path, rows):
    """Write a minimal qsv-style stats CSV with the columns the
    helper looks at."""
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["field", "type", "mean"])
        w.writeheader()
        w.writerows(rows)


def _read_csv(path):
    with open(path) as f:
        return list(csv.DictReader(f))


def test_blank_date_means_blanks_date_and_datetime_rows(stage, tmp_path):
    """Date and DateTime rows have ``mean`` blanked; numeric rows pass
    through unchanged. Returns the new path so the caller uses the
    cleaned file."""
    src = tmp_path / "qsv_stats.csv"
    _write_stats_csv(
        src,
        [
            {"field": "id", "type": "Integer", "mean": "42"},
            {"field": "score", "type": "Float", "mean": "3.14"},
            {"field": "born_on", "type": "Date", "mean": "1990-01-01"},
            {"field": "logged_at", "type": "DateTime", "mean": "2025-03-01T12:00"},
        ],
    )
    ctx = mock.MagicMock()
    ctx.temp_dir = str(tmp_path)

    result_path = stage._blank_date_means(ctx, str(src))

    assert result_path != str(src)  # a new file was written
    rows = _read_csv(result_path)
    by_field = {r["field"]: r for r in rows}
    assert by_field["id"]["mean"] == "42"
    assert by_field["score"]["mean"] == "3.14"
    assert by_field["born_on"]["mean"] == ""
    assert by_field["logged_at"]["mean"] == ""


def test_blank_date_means_returns_original_when_no_date_rows(stage, tmp_path):
    """No Date/DateTime rows → don't write a new file, return the
    original path. Keeps the file-handling path identical for the
    common all-numeric case (no spurious tempfile creation)."""
    src = tmp_path / "qsv_stats.csv"
    _write_stats_csv(
        src,
        [
            {"field": "id", "type": "Integer", "mean": "42"},
            {"field": "score", "type": "Float", "mean": "3.14"},
        ],
    )
    ctx = mock.MagicMock()
    ctx.temp_dir = str(tmp_path)

    result_path = stage._blank_date_means(ctx, str(src))

    assert result_path == str(src)
    # No leftover cleaned file:
    assert not (tmp_path / "qsv_stats_cleaned.csv").exists()


def test_blank_date_means_handles_empty_mean(stage, tmp_path):
    """A Date row whose ``mean`` is already empty doesn't trigger the
    rewrite (no work to do). Numeric rows with empty mean (rare but
    possible) pass through unchanged."""
    src = tmp_path / "qsv_stats.csv"
    _write_stats_csv(
        src,
        [
            {"field": "born_on", "type": "Date", "mean": ""},
            {"field": "score", "type": "Float", "mean": "3.14"},
        ],
    )
    ctx = mock.MagicMock()
    ctx.temp_dir = str(tmp_path)

    result_path = stage._blank_date_means(ctx, str(src))

    # No date row needed blanking → returns original
    assert result_path == str(src)
