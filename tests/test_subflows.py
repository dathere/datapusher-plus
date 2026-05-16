# -*- coding: utf-8 -*-
"""
Unit coverage for the per-domain subflow primitives in
``ckanext.datapusher_plus.jobs.subflows``.

These tests run the subflows in-process (Prefect 3 lets you call a
``@flow`` directly — it executes the body and tracks the run against
an ephemeral local server when none is configured) and verify:

* the subflow forwards its arguments to the wrapped helper, and
* shapes the helper's return value into the documented JSON-encodable
  dict for downstream consumption.

The underlying helpers (``screen_for_pii`` / ``process_spatial_file``)
have their own coverage in the integration suite; here we patch them
at their import sites in ``subflows`` so the tests do not need a real
PII regex resource or a real shapefile.
"""

from __future__ import annotations

from unittest import mock

import pytest


@pytest.fixture
def subflows():
    """Import the subflows module lazily — its top-level imports need
    Prefect and the CKAN-dependent ``pii_screening`` / ``qsv_utils``
    modules to be available."""
    pytest.importorskip("prefect")
    pytest.importorskip("ckan")
    from ckanext.datapusher_plus.jobs import subflows as mod

    return mod


# ---------------------------------------------------------------------------
# pii_screening_subflow
# ---------------------------------------------------------------------------


def test_pii_subflow_forwards_args_and_shapes_result(subflows, tmp_path):
    """The subflow calls ``screen_for_pii`` with the forwarded args
    and packages the ``(pii_found, count)`` tuple into the documented
    dict shape. The constructed ``QSVCommand`` instance reaches the
    helper as the third positional argument — without this assertion,
    a regression that swapped the call order or dropped the
    ``QSVCommand`` would slip through.
    """
    csv = tmp_path / "x.csv"
    csv.write_text("col1,col2\n1,2\n")

    with mock.patch.object(
        subflows, "screen_for_pii", return_value=(True, 3)
    ) as screen, mock.patch.object(subflows, "QSVCommand") as qsv_class:
        result = subflows.pii_screening_subflow(
            csv_path=str(csv),
            resource={"format": "CSV", "url": "x"},
            temp_dir=str(tmp_path),
        )

    assert result == {"pii_found": True, "pii_candidate_count": 3}
    screen.assert_called_once()
    qsv_class.assert_called_once()
    args, _ = screen.call_args
    assert args[0] == str(csv)
    assert args[1] == {"format": "CSV", "url": "x"}
    # The QSVCommand instance constructed inside the subflow is
    # forwarded as the third positional arg.
    assert args[2] is qsv_class.return_value
    assert args[3] == str(tmp_path)


def test_pii_subflow_returns_no_pii_path(subflows, tmp_path):
    """``screen_for_pii`` reporting ``(False, 0)`` round-trips
    cleanly through the subflow."""
    csv = tmp_path / "clean.csv"
    csv.write_text("a,b\n1,2\n")

    with mock.patch.object(
        subflows, "screen_for_pii", return_value=(False, 0)
    ), mock.patch.object(subflows, "QSVCommand"):
        result = subflows.pii_screening_subflow(
            csv_path=str(csv),
            resource={"format": "CSV", "url": "x"},
            temp_dir=str(tmp_path),
        )

    assert result == {"pii_found": False, "pii_candidate_count": 0}


# ---------------------------------------------------------------------------
# spatial_processing_subflow
# ---------------------------------------------------------------------------


def test_spatial_subflow_success_shapes_bounds_as_list(subflows, tmp_path):
    """A successful conversion returns ``bounds`` as a list (not a
    tuple) so the result is JSON-serializable across the subflow
    boundary."""
    input_path = tmp_path / "in.geojson"
    input_path.write_text('{"type":"FeatureCollection","features":[]}')
    output_path = tmp_path / "out.csv"

    with mock.patch.object(
        subflows,
        "process_spatial_file",
        return_value=(True, None, (-1.0, -2.0, 3.0, 4.0)),
    ) as p:
        result = subflows.spatial_processing_subflow(
            input_path=str(input_path),
            resource_format="GEOJSON",
            output_csv_path=str(output_path),
            tolerance=0.005,
        )

    assert result == {
        "success": True,
        "error_message": None,
        "bounds": [-1.0, -2.0, 3.0, 4.0],
    }
    assert isinstance(result["bounds"], list)
    # Forwarded args reach the helper in the documented order.
    args, _ = p.call_args
    assert args[0] == str(input_path)
    assert args[1] == "GEOJSON"
    assert args[2] == str(output_path)
    assert args[3] == 0.005


def test_spatial_subflow_failure_surfaces_error_message(subflows, tmp_path):
    """A failed conversion surfaces the helper's error message and
    returns ``bounds=None`` (no list cast on ``None``)."""
    input_path = tmp_path / "bad.geojson"
    input_path.write_text("not valid json")

    with mock.patch.object(
        subflows,
        "process_spatial_file",
        return_value=(False, "Invalid GeoJSON", None),
    ):
        result = subflows.spatial_processing_subflow(
            input_path=str(input_path), resource_format="GEOJSON"
        )

    assert result == {
        "success": False,
        "error_message": "Invalid GeoJSON",
        "bounds": None,
    }


def test_spatial_subflow_default_tolerance(subflows, tmp_path):
    """Omitting ``tolerance`` uses the documented 0.001 default
    (0.1% relative to geometry diagonal)."""
    input_path = tmp_path / "x.geojson"
    input_path.write_text("{}")

    with mock.patch.object(
        subflows, "process_spatial_file", return_value=(True, None, (0, 0, 1, 1))
    ) as p:
        subflows.spatial_processing_subflow(
            input_path=str(input_path), resource_format="GEOJSON"
        )

    args, _ = p.call_args
    assert args[3] == 0.001
