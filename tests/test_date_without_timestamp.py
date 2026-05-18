# -*- coding: utf-8 -*-
"""
Regression coverage for issue #179: a CSV column containing date-only
values (e.g. ``2023-02-02``) was being stored as Postgres ``timestamp``
and rendered as ``2023-02-02T00:00:00`` instead of as a plain date.

Root cause was a discrepancy between two sources of the same default:

* ``ckanext/datapusher_plus/config.py:144`` inline fallback default
  for ``type_mapping`` had ``"Date": "date"`` (correct).
* ``ckanext/datapusher_plus/config_declaration.yaml`` had
  ``"Date": "timestamp"`` (wrong) — and the CKAN declaration default
  wins over an inline fallback in ``tk.config.get(key, fallback)``
  because the declaration populates ``tk.config`` BEFORE the
  fallback is consulted.

A secondary bug was that the analysis stage's "apply Data Dictionary
type override" path had a Postgres-→-qsv reverse map that was missing
the ``"date"`` entry, so an operator who explicitly set
``type_override = "date"`` in the Data Dictionary would silently fall
through to qsv's original ``DateTime`` inference and be mapped right
back to Postgres ``timestamp``.

These tests pin three properties of the fix:

1. A qsv-inferred ``Date`` type produces a Postgres ``date`` column
   in the analysis stage's headers_dicts (not ``timestamp``).
2. A ``date`` type is NOT added to ``datetimecols_list``, so it
   skips the ``qsv datefmt`` normalization that would add the
   ``T00:00:00`` time component.
3. A Data Dictionary ``type_override = "date"`` survives the
   reverse-mapping logic — the operator's explicit choice wins.

We also pin the declaration default itself, so a future "cleanup"
PR doesn't silently re-introduce the bug by reverting it.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest


@pytest.fixture
def minimal_stats_csv_dates(tmp_path):
    """A stats CSV with a Date column, a DateTime column, and one each
    of String / Integer for control. ``qsv_*`` row is the sentinel
    trailer that ``_parse_stats``' loop breaks on.
    """
    csv_path = tmp_path / "stats.csv"
    csv_path.write_text(
        "field,type,min,max,cardinality\n"
        "name,String,Alice,Zach,42\n"
        "qty,Integer,1,1000,500\n"
        "birthday,Date,2000-01-01,2024-12-31,365\n"
        "logged_at,DateTime,2020-01-01T00:00:00,2024-12-31T23:59:59,500\n"
        "qsv__rowcount,Integer,0,0,0\n",
        encoding="utf-8",
    )
    return str(csv_path)


@pytest.fixture(autouse=True)
def _isolate_dictionary_stash(tmp_path, monkeypatch):
    """Point the dictionary-stash dir at a per-test tmp_path.

    ``AnalysisStage._parse_stats`` writes (and the retry-restore branch
    reads) ``dictionary_stash`` files keyed by ``resource_id``. Without
    this fixture, a stash file from a prior test run for the same
    ``resource_id`` would be silently loaded as ``existing_info`` and
    apply a (stale) type_override on top of the qsv inference —
    making the FIRST date test in this file see ``type_override='date'``
    from the THIRD test's mock setup. Caught while writing the tests
    for #179.
    """
    pytest.importorskip("ckan")
    import ckan.plugins.toolkit as tk

    monkeypatch.setitem(
        tk.config, "ckanext.datapusher_plus.dictionary_stash_dir", str(tmp_path)
    )


@pytest.fixture
def analysis_stage_context(tmp_path):
    """Minimal ProcessingContext stand-in for ``_parse_stats``."""
    pytest.importorskip("ckan")
    from ckanext.datapusher_plus.jobs.stages.analysis import AnalysisStage

    stage = AnalysisStage()
    context = SimpleNamespace(
        resource_id="res-date-test",
        logger=mock.Mock(),
        existing_info=None,
        add_stat=mock.Mock(),
    )
    return stage, context


# ---------------------------------------------------------------------------
# Central #179 contract: a date-only column becomes a Postgres ``date``.
# ---------------------------------------------------------------------------


def test_date_column_becomes_postgres_date_not_timestamp(
    analysis_stage_context, minimal_stats_csv_dates, monkeypatch
):
    # The user-visible bug from #179 in one assertion: a CSV column
    # with date-only values (qsv infers it as "Date") MUST produce a
    # ``date`` Postgres column, not ``timestamp``. Pre-fix, both
    # ``Date`` and ``DateTime`` mapped to ``timestamp``.
    from ckanext.datapusher_plus.jobs.stages import analysis as analysis_mod

    stage, context = analysis_stage_context
    monkeypatch.setattr(
        analysis_mod.dsu, "datastore_resource_exists", lambda rid: None
    )

    headers_dicts, datetimecols_list, _stats = stage._parse_stats(
        context, minimal_stats_csv_dates, {0: "name", 1: "qty", 2: "birthday", 3: "logged_at"}
    )

    by_id = {h["id"]: h for h in headers_dicts}

    # The actual #179 fix: ``Date`` -> Postgres ``date``.
    assert by_id["birthday"]["type"] == "date", (
        "Date-only column was assigned Postgres ``%s`` instead of ``date`` — "
        "the #179 bug is back."
    ) % by_id["birthday"]["type"]

    # And ``DateTime`` still goes to ``timestamp`` (the not-buggy case
    # the fix MUSTN'T break).
    assert by_id["logged_at"]["type"] == "timestamp"


def test_date_column_not_added_to_datetimecols_for_normalization(
    analysis_stage_context, minimal_stats_csv_dates, monkeypatch
):
    # ``datetimecols_list`` is fed to ``qsv datefmt`` which normalizes
    # values to RFC 3339 — adding ``T00:00:00`` for date-only inputs.
    # That's exactly what produces the reporter's "2023-02-02T00:00:00"
    # symptom in the preview pane. A column typed as Postgres ``date``
    # MUST NOT be added to the list — the value passes through
    # unchanged (e.g. ``2023-02-02``) and lands cleanly in a ``date``
    # column.
    from ckanext.datapusher_plus.jobs.stages import analysis as analysis_mod

    stage, context = analysis_stage_context
    monkeypatch.setattr(
        analysis_mod.dsu, "datastore_resource_exists", lambda rid: None
    )

    _headers, datetimecols_list, _stats = stage._parse_stats(
        context, minimal_stats_csv_dates, {0: "name", 1: "qty", 2: "birthday", 3: "logged_at"}
    )

    # Only the DateTime column gets normalized.
    assert "logged_at" in datetimecols_list
    assert "birthday" not in datetimecols_list


# ---------------------------------------------------------------------------
# Type override survives the reverse map.
# ---------------------------------------------------------------------------


def test_type_override_date_wins_over_qsv_datetime_inference(
    analysis_stage_context, minimal_stats_csv_dates, monkeypatch
):
    # Operator wants a column annotated as date-only even though qsv
    # inferred DateTime. They set ``type_override = "date"`` in the
    # Data Dictionary. Pre-#179 reverse-map fix, the "date" key was
    # missing from the Postgres-→-qsv map, so the override silently
    # fell through to qsv's "DateTime" inference and the column ended
    # up Postgres ``timestamp`` — losing the operator's intent.
    from ckanext.datapusher_plus.jobs.stages import analysis as analysis_mod

    stage, context = analysis_stage_context

    # Pretend the resource exists with a Data Dictionary that overrides
    # ``logged_at`` to ``date`` (even though qsv infers DateTime).
    monkeypatch.setattr(
        analysis_mod.dsu,
        "datastore_resource_exists",
        lambda rid: {
            "fields": [
                {
                    "id": "logged_at",
                    "info": {"type_override": "date", "label": "Logged on"},
                }
            ]
        },
    )
    # The branch below the override calls delete_datastore_resource —
    # stub it out, the test isn't about that path.
    monkeypatch.setattr(
        analysis_mod.dsu, "delete_datastore_resource", lambda rid: None
    )

    headers_dicts, datetimecols_list, _stats = stage._parse_stats(
        context, minimal_stats_csv_dates, {0: "name", 1: "qty", 2: "birthday", 3: "logged_at"}
    )

    by_id = {h["id"]: h for h in headers_dicts}
    # Operator's intent wins.
    assert by_id["logged_at"]["type"] == "date", (
        "type_override='date' was silently dropped — the reverse map "
        "in analysis._parse_stats is missing the ``date`` entry."
    )
    # And the column is NOT normalized through qsv datefmt.
    assert "logged_at" not in datetimecols_list


# ---------------------------------------------------------------------------
# Declaration default — catches a future regression that re-introduces
# ``"Date": "timestamp"`` in config_declaration.yaml.
# ---------------------------------------------------------------------------


def test_config_declaration_default_maps_date_to_date_not_timestamp():
    # The historical bug: ``config.py``'s inline fallback default for
    # ``type_mapping`` had ``"Date": "date"`` but the declaration's
    # default had ``"Date": "timestamp"`` — and the declaration wins.
    # Pinning the declaration value here so a future "tidy up" PR
    # doesn't silently regress.
    decl_path = (
        Path(__file__).resolve().parents[1]
        / "ckanext"
        / "datapusher_plus"
        / "config_declaration.yaml"
    )
    text = decl_path.read_text()

    # The declaration's value is JSON-as-string. Grep for the
    # type_mapping section, parse, assert. Doing the parse rather
    # than a substring grep so reordering of keys doesn't false-fail
    # the test.
    import re
    import yaml

    docs = yaml.safe_load(text)
    type_mapping_key = "ckanext.datapusher_plus.type_mapping"
    found = None
    for group in docs["groups"]:
        for opt in group.get("options", []):
            if opt.get("key") == type_mapping_key:
                found = opt
                break
    assert found is not None, "type_mapping not declared in config_declaration.yaml"

    declared_default = json.loads(found["default"])
    assert declared_default.get("Date") == "date", (
        "config_declaration default has ``Date`` mapped to "
        f"{declared_default.get('Date')!r} — must be ``date`` (issue #179)."
    )
    # Belt-and-braces: ensure the inline fallback in config.py agrees.
    # If they ever drift again, this test catches it before a real
    # operator does.
    config_py = (
        Path(__file__).resolve().parents[1]
        / "ckanext"
        / "datapusher_plus"
        / "config.py"
    )
    config_text = config_py.read_text()
    # Find the inline fallback JSON literal in the tk.config.get(...) call.
    match = re.search(
        r'"ckanext\.datapusher_plus\.type_mapping",\s*\'([^\']+)\'',
        config_text,
    )
    assert match, (
        "Could not find inline type_mapping fallback in config.py — "
        "the test grep needs updating."
    )
    inline_default = json.loads(match.group(1))
    assert inline_default.get("Date") == "date", (
        f"config.py inline default has Date={inline_default.get('Date')!r}, "
        "expected 'date'."
    )
