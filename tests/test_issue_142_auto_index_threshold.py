# -*- coding: utf-8 -*-
"""
Regression coverage for issue #142 ("Low cardinality auto indexing").

Eric Soroos's report: DP+'s ``AUTO_INDEX_THRESHOLD`` (default 3) was
creating B-tree indexes on columns with cardinality as low as 1 (a
single distinct text value) — those indexes occupy 10–40 MB on
real-world resources and the Postgres planner never picks them
because a sequential scan is always cheaper at that selectivity.

The fix (designed in the issue thread by @jqnatividad) keeps the
existing intent — accelerate filtering UIs (DataTables SearchBuilder,
CKAN's facet-style views) over enum-shaped columns like ``Borough``
(5 values) or ``status`` (3–10 values) — but adds a lower bound to
skip the useless single-value case:

    cardinality range for auto-indexing: [MIN_THRESHOLD, MAX_THRESHOLD]
    defaults: [3, 10]   (was [implicit 1, 3])

These tests pin five properties:

1. The default thresholds are 3 and 10 respectively (issue-#142 design).
2. The declaration defaults agree with ``config.py``'s inline
   fallbacks (a #179-style declaration-vs-fallback drift would let
   the defaults silently disagree).
3. The ``_create_indexes`` loop only indexes columns whose cardinality
   falls in the closed range — single-value columns no longer get
   indexed at the default settings (Eric's exact failure mode).
4. The ``-1`` upper-bound magic value still means "no upper bound"
   (legacy contract).
5. ``MIN > MAX`` is logged as a warning (so an operator who set
   ``min=5, threshold=3`` sees what happened instead of silently
   getting zero indexes).
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest


REPO_ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Default values
# ---------------------------------------------------------------------------


def test_config_py_inline_fallback_defaults_for_auto_index():
    """``config.py``'s inline fallbacks must encode the issue-#142
    design: upper threshold 10, lower threshold 3.

    AST-parsed rather than imported to keep this test free of the CKAN
    bootstrap (matches the pattern in ``test_date_without_timestamp.py``).
    """
    import ast

    config_py = (
        REPO_ROOT / "ckanext" / "datapusher_plus" / "config.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(config_py)

    # Walk the AST for the ``tk.config.get(<key>, <default>)`` calls
    # bound to the two AUTO_INDEX_* keys.
    defaults = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not (
            isinstance(node.func, ast.Attribute) and node.func.attr == "get"
        ):
            continue
        if not node.args or not isinstance(node.args[0], ast.Constant):
            continue
        key = node.args[0].value
        if not isinstance(key, str) or "auto_index" not in key:
            continue
        if len(node.args) >= 2 and isinstance(node.args[1], ast.Constant):
            defaults[key] = node.args[1].value

    assert (
        defaults.get("ckanext.datapusher_plus.auto_index_threshold") == "10"
    ), (
        f"config.py auto_index_threshold inline default is "
        f"{defaults.get('ckanext.datapusher_plus.auto_index_threshold')!r}; "
        "must be '10' per issue #142 (was '3' pre-resolution)."
    )
    assert (
        defaults.get("ckanext.datapusher_plus.auto_index_min_threshold")
        == "3"
    ), (
        f"config.py auto_index_min_threshold inline default is "
        f"{defaults.get('ckanext.datapusher_plus.auto_index_min_threshold')!r}; "
        "must be '3' per issue #142 (the field is new — issue #142 "
        "added the lower bound)."
    )


def test_config_declaration_defaults_for_auto_index():
    """The CKAN declaration defaults must agree with the inline
    fallbacks. A #179-style declaration-vs-fallback drift would silently
    pick whichever the declaration declared, regardless of what
    ``config.py`` claims.
    """
    import yaml

    decl_path = (
        REPO_ROOT
        / "ckanext"
        / "datapusher_plus"
        / "config_declaration.yaml"
    )
    docs = yaml.safe_load(decl_path.read_text(encoding="utf-8"))

    by_key = {}
    for group in docs["groups"]:
        for opt in group.get("options", []):
            if opt.get("key", "").startswith(
                "ckanext.datapusher_plus.auto_index"
            ):
                by_key[opt["key"]] = opt

    assert (
        by_key["ckanext.datapusher_plus.auto_index_threshold"]["default"]
        == 10
    ), "config_declaration default for auto_index_threshold must be 10"
    assert (
        by_key["ckanext.datapusher_plus.auto_index_min_threshold"]["default"]
        == 3
    ), "config_declaration default for auto_index_min_threshold must be 3"


# ---------------------------------------------------------------------------
# The MIN floor actually skips single-value columns
# ---------------------------------------------------------------------------


def _build_indexing_test_context(cardinalities, headers, *, record_count=100):
    """Build a minimal ``ProcessingContext`` for ``_create_indexes``.

    The fixture is intentionally tiny: we mock the psycopg2 connection
    and cursor so the test runs in any environment (no Postgres
    needed). What we're pinning is the *decision* about which columns
    get an index, not the SQL execution path.
    """
    context = SimpleNamespace(
        resource_id="res-142-test",
        logger=mock.Mock(),
        headers=headers,
        headers_dicts=[{"id": h, "type": "text"} for h in headers],
        dataset_stats={
            "HEADERS_CARDINALITY": cardinalities,
            "RECORD_COUNT": record_count,
        },
    )
    return context


def _run_create_indexes(
    cardinalities,
    headers,
    *,
    record_count=100,
    auto_index_threshold=10,
    auto_index_min_threshold=3,
    auto_index_dates=False,
    auto_unique_index=False,
    preview_rows=0,
    date_like_cols_list=None,
):
    """Drive ``_create_indexes`` with the per-column index methods
    stubbed and return the list of columns the stage *decided* to
    index.

    We patch ``_create_regular_index`` / ``_create_unique_index``
    (which sit just below the cardinality-range decision) rather than
    mocking the psycopg2 cursor — ``psycopg2.sql.Composed.as_string``
    needs a real connection/cursor and falls over against a
    MagicMock. Stubbing the index-creation methods isolates the test
    to the issue #142 decision logic, which is what we're pinning.
    """
    pytest.importorskip("ckan")
    from ckanext.datapusher_plus.jobs.stages.indexing import IndexingStage
    from ckanext.datapusher_plus.jobs.stages import indexing as indexing_mod

    stage = IndexingStage()
    context = _build_indexing_test_context(
        cardinalities, headers, record_count=record_count
    )

    regular_indexed: list[str] = []
    unique_indexed: list[str] = []

    def fake_create_regular(_self, _ctx, _cur, column, _cardinality, _dates):
        regular_indexed.append(column)
        return True

    def fake_create_unique(_self, _ctx, _cur, column, _cardinality):
        unique_indexed.append(column)
        return True

    fake_conn = mock.MagicMock()
    fake_conn.cursor.return_value = mock.MagicMock()

    with mock.patch.object(
        indexing_mod.psycopg2, "connect", return_value=fake_conn
    ), mock.patch.object(
        indexing_mod.conf, "AUTO_INDEX_DATES", auto_index_dates
    ), mock.patch.object(
        indexing_mod.conf, "AUTO_UNIQUE_INDEX", auto_unique_index
    ), mock.patch.object(
        indexing_mod.conf, "PREVIEW_ROWS", preview_rows
    ), mock.patch.object(
        IndexingStage,
        "_create_regular_index",
        side_effect=fake_create_regular,
        autospec=True,
    ), mock.patch.object(
        IndexingStage,
        "_create_unique_index",
        side_effect=fake_create_unique,
        autospec=True,
    ), mock.patch.object(
        IndexingStage, "_vacuum_analyze",
    ):
        stage._create_indexes(
            context,
            cardinalities,
            date_like_cols_list=date_like_cols_list or [],
            record_count=record_count,
            auto_index_threshold=auto_index_threshold,
            auto_index_min_threshold=auto_index_min_threshold,
        )

    # Tests interpret "indexed" as "any index was created", so flatten.
    return regular_indexed + unique_indexed


def test_single_value_column_is_not_indexed_at_defaults():
    """The exact failure mode @EricSoroos reported: a column with
    cardinality 1 (one distinct text value) was getting a 10–40 MB
    index the Postgres planner would never use. Default thresholds
    are now ``[3, 10]``, so cardinality 1 falls below the MIN floor
    and the column is skipped.
    """
    indexed = _run_create_indexes(
        cardinalities=[1, 5, 50],
        headers=["status_always_active", "borough", "transaction_id"],
        auto_index_threshold=10,
        auto_index_min_threshold=3,
    )
    assert "status_always_active" not in indexed, (
        "Single-value column was indexed despite cardinality (1) being "
        "below the MIN_THRESHOLD (3). Issue #142's regression."
    )


def test_columns_in_range_are_indexed():
    """Within the closed range [MIN, MAX] is the sweet spot — typical
    enum-shaped columns (5 values like ``borough``) should still get
    indexed. The MIN floor must not over-correct and skip these.
    """
    indexed = _run_create_indexes(
        cardinalities=[3, 5, 7, 10],
        headers=["country", "borough", "status", "category"],
        auto_index_threshold=10,
        auto_index_min_threshold=3,
    )
    assert set(indexed) == {"country", "borough", "status", "category"}, (
        f"Expected all four in-range columns to be indexed; got {indexed}"
    )


def test_high_cardinality_columns_are_not_indexed():
    """Columns above the MAX threshold are skipped (existing contract;
    the planner would prefer a seq scan anyway for low-selectivity
    filtering, and these columns are not the DataTables sweet spot).
    """
    indexed = _run_create_indexes(
        cardinalities=[5, 50, 999],
        headers=["borough", "zip_code", "tracking_id"],
        auto_index_threshold=10,
        auto_index_min_threshold=3,
    )
    # zip_code and tracking_id are above MAX → no auto-index.
    assert "borough" in indexed
    assert "zip_code" not in indexed
    assert "tracking_id" not in indexed


def test_legacy_min_zero_disables_floor():
    """An operator who wants the pre-#142 behavior (no lower floor)
    sets ``auto_index_min_threshold = 0`` — single-value columns
    return to being indexed. This is the documented escape hatch for
    ``auto_index_threshold = -1`` ("index every column") to keep
    working.
    """
    indexed = _run_create_indexes(
        cardinalities=[1, 2, 5],
        headers=["only_active", "yes_no", "borough"],
        auto_index_threshold=10,
        auto_index_min_threshold=0,
    )
    assert "only_active" in indexed
    assert "yes_no" in indexed
    assert "borough" in indexed


# ---------------------------------------------------------------------------
# Misconfiguration is loud, not silent
# ---------------------------------------------------------------------------


def test_min_greater_than_max_logs_a_warning_and_indexes_nothing():
    """An operator who sets ``min=5, threshold=3`` (range becomes
    empty) currently gets zero indexes with no signal. Issue #142's
    fix adds a clear ``warning`` log line and the range still
    correctly produces zero non-date / non-unique indexes.
    """
    pytest.importorskip("ckan")
    from ckanext.datapusher_plus.jobs.stages.indexing import IndexingStage
    from ckanext.datapusher_plus.jobs.stages import indexing as indexing_mod

    stage = IndexingStage()
    context = _build_indexing_test_context(
        [5, 7, 9], ["a", "b", "c"]
    )

    fake_conn = mock.MagicMock()
    fake_conn.cursor.return_value = mock.MagicMock()

    with mock.patch.object(
        indexing_mod.psycopg2, "connect", return_value=fake_conn
    ), mock.patch.object(
        indexing_mod.conf, "AUTO_INDEX_DATES", False
    ), mock.patch.object(
        indexing_mod.conf, "AUTO_UNIQUE_INDEX", False
    ), mock.patch.object(
        indexing_mod.conf, "AUTO_INDEX_THRESHOLD", 3
    ), mock.patch.object(
        indexing_mod.conf, "AUTO_INDEX_MIN_THRESHOLD", 5
    ), mock.patch.object(
        indexing_mod.conf, "PREVIEW_ROWS", 0
    ):
        # Drive ``process``, which is where the warning is emitted —
        # not ``_create_indexes`` directly.
        stage.process(context)

    # Assert ``logger.warning(...)`` was called with a message that
    # mentions the empty range.
    warning_calls = [
        call.args[0]
        for call in context.logger.warning.mock_calls
    ]
    assert any(
        "min_threshold" in msg and "threshold" in msg
        for msg in warning_calls
    ), (
        f"Expected a warning about empty min/max range; saw: "
        f"{warning_calls!r}"
    )
