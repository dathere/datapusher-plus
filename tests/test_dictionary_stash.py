# -*- coding: utf-8 -*-
"""
Unit coverage for ``ckanext.datapusher_plus.dictionary_stash``.

The stash is the on-disk persistence layer behind issue #265 ("Stash
existing data dictionary before doing a DP+ job. If DP+job fails, the
old Data Dictionary should be restored.") — it lives between the
analysis stage's capture of ``existing_info`` and the rollback hook
that may need it minutes later, on the *same* worker, after the
in-memory ``ProcessingContext`` is gone.

Round-trip semantics tested here:

* ``save`` → ``load`` returns the identical dict.
* ``load`` on a missing stash returns ``None`` (the rollback hook keys
  off this — "no stash, no restore").
* ``clear`` is idempotent (the success path always calls it; we never
  want a "no such file" to bubble up).
* Corrupt JSON on disk is treated as absent (a half-written stash from
  a worker that died mid-``save`` must NOT crash the rollback).
* Atomic write: a crash mid-``save`` leaves either the *old* stash or
  no stash, never a half-written one. We prove this by checking the
  tempfile is renamed, not opened-and-streamed.
* Path-traversal guard: a maliciously crafted ``resource_id`` cannot
  escape the stash directory.
"""

from __future__ import annotations

import json
import os
from unittest import mock

import pytest


@pytest.fixture
def stash_dir(tmp_path, monkeypatch):
    """Point the stash at a per-test tmpdir via the CKAN config knob."""
    pytest.importorskip("ckan")
    import ckan.plugins.toolkit as tk

    monkeypatch.setitem(
        tk.config, "ckanext.datapusher_plus.dictionary_stash_dir", str(tmp_path)
    )
    return tmp_path


@pytest.fixture
def stash_module(stash_dir):
    """Import the module under test fresh per test (no module-level state to reset)."""
    from ckanext.datapusher_plus import dictionary_stash

    return dictionary_stash


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_save_then_load_roundtrips_the_dictionary(stash_module):
    info = {
        "name": {"label": "Widget name", "type_override": "text"},
        "qty": {"label": "Quantity", "type_override": "numeric"},
    }
    stash_module.save("res-aaa", info)
    assert stash_module.load("res-aaa") == info


def test_load_on_missing_stash_returns_none(stash_module):
    # The rollback hook uses this signal to decide whether to restore.
    # A `None` return MUST be cheap and side-effect free.
    assert stash_module.load("res-never-saved") is None


def test_clear_removes_the_stash(stash_module, stash_dir):
    stash_module.save("res-bbb", {"x": {"label": "X"}})
    stash_module.clear("res-bbb")
    assert stash_module.load("res-bbb") is None
    assert not (stash_dir / "res-bbb.json").exists()


def test_clear_is_idempotent(stash_module):
    # The on-success path in the flow calls clear() unconditionally —
    # if save() was never called (no existing dictionary), clear() must
    # still be a no-op rather than raising.
    stash_module.clear("res-never-saved")  # must not raise


def test_save_with_empty_dict_is_explicit_no_dictionary_signal(stash_module):
    # An empty dict is semantically "the resource had no data dictionary
    # to stash" — distinct from "no stash was ever written". load()
    # should return {}, not None, so the rollback hook can distinguish.
    stash_module.save("res-ccc", {})
    assert stash_module.load("res-ccc") == {}


def test_save_overwrites_previous_stash(stash_module):
    # Re-runs of a failed job should supersede the prior attempt's stash
    # — otherwise stale info from a long-ago failure would be restored.
    stash_module.save("res-ddd", {"v1": {"label": "old"}})
    stash_module.save("res-ddd", {"v2": {"label": "new"}})
    assert stash_module.load("res-ddd") == {"v2": {"label": "new"}}


# ---------------------------------------------------------------------------
# Failure modes
# ---------------------------------------------------------------------------


def test_corrupt_stash_is_treated_as_absent(stash_module, stash_dir, caplog):
    # Simulate a worker that died mid-write: an existing file with
    # invalid JSON. The rollback hook must NOT crash on this — better
    # to skip restore than to lose the rollback entirely.
    corrupt = stash_dir / "res-eee.json"
    corrupt.write_text("{not json", encoding="utf-8")

    with caplog.at_level("WARNING"):
        assert stash_module.load("res-eee") is None
    assert any("corrupt stash" in r.message for r in caplog.records)


def test_save_is_atomic_via_rename(stash_module, stash_dir):
    # Write semantics: write to .tmp then os.replace. A crash before
    # replace() leaves the OLD file (or no file) — never a partially
    # written final file. We verify by checking that no .tmp artefact
    # is left behind after a successful save (catches a regression
    # where someone "simplified" to a direct open(path, 'w')).
    stash_module.save("res-fff", {"k": {"label": "v"}})
    leftover_tmps = list(stash_dir.glob("*.tmp"))
    assert leftover_tmps == [], f"unexpected tempfile leftover: {leftover_tmps}"


def test_save_with_failed_atomic_replace_does_not_leave_corruption(
    stash_module, stash_dir, monkeypatch
):
    # If os.replace fails (rare — disk full, permissions), the existing
    # stash (if any) must remain intact AND the .tmp file is cleaned
    # up so the directory doesn't accumulate leaks across repeated
    # failed writes (roborev #2222 LOW finding).
    stash_module.save("res-ggg", {"existing": {"label": "keep me"}})
    original_path = stash_dir / "res-ggg.json"
    original_content = original_path.read_text()

    def boom(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(os, "replace", boom)
    with pytest.raises(OSError):
        stash_module.save("res-ggg", {"new": {"label": "lost"}})

    # Original stash is intact — the failed write did NOT clobber it.
    assert original_path.read_text() == original_content
    # And the tempfile was cleaned up, not leaked.
    leftover_tmps = list(stash_dir.glob("*.tmp"))
    assert leftover_tmps == [], f"unexpected tempfile leftover: {leftover_tmps}"


# ---------------------------------------------------------------------------
# Path-traversal guard
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "bad_id",
    [
        "../etc/passwd",
        "foo/bar",
        # On Windows, os.altsep is '/' — but the regular separator '\'
        # is the primary concern. We test both behaviors via os.sep
        # to be portable.
        "a" + os.sep + "b",
    ],
)
def test_path_traversal_rejected(stash_module, bad_id):
    # CKAN resource ids are UUIDs in practice; this guard is paranoia
    # against a future caller passing a user-supplied string. The
    # stash file should never end up outside the configured directory.
    with pytest.raises(ValueError):
        stash_module.stash_path(bad_id)


def test_empty_resource_id_rejected(stash_module):
    with pytest.raises(ValueError):
        stash_module.stash_path("")


# ---------------------------------------------------------------------------
# Directory bootstrap
# ---------------------------------------------------------------------------


def test_stash_dir_is_created_on_first_use(tmp_path, monkeypatch):
    pytest.importorskip("ckan")
    import ckan.plugins.toolkit as tk
    from ckanext.datapusher_plus import dictionary_stash

    # Point at a non-existent subdir; the module should mkdir it.
    target = tmp_path / "deep" / "nested" / "stash"
    monkeypatch.setitem(
        tk.config, "ckanext.datapusher_plus.dictionary_stash_dir", str(target)
    )
    assert not target.exists()

    dictionary_stash.save("res-hhh", {"k": {"label": "v"}})
    assert target.exists()
    assert (target / "res-hhh.json").exists()


def test_default_stash_dir_falls_back_to_tempfile(monkeypatch):
    # When the config knob is unset, stash files go under tempfile's
    # default location — this is the documented fallback.
    pytest.importorskip("ckan")
    import tempfile

    import ckan.plugins.toolkit as tk
    from ckanext.datapusher_plus import dictionary_stash

    # Use monkeypatch.delitem (not tk.config.pop) so any prior value
    # is restored at teardown — pop() would silently swallow it and
    # create a hidden ordering dependency for subsequent tests
    # (roborev #2222 LOW finding).
    monkeypatch.delitem(
        tk.config,
        "ckanext.datapusher_plus.dictionary_stash_dir",
        raising=False,
    )

    path = dictionary_stash.stash_path("res-iii")
    assert path.startswith(os.path.join(tempfile.gettempdir(), "dpp_dict_stash"))


def test_load_does_not_create_stash_dir(tmp_path, monkeypatch):
    # ``load`` and ``clear`` must NOT bootstrap the stash directory —
    # the flow's success ``finally`` calls ``clear`` unconditionally
    # and we don't want it to create an empty directory for jobs that
    # never stashed anything (roborev #2222 LOW finding).
    pytest.importorskip("ckan")
    import ckan.plugins.toolkit as tk
    from ckanext.datapusher_plus import dictionary_stash

    target = tmp_path / "not-yet-created"
    monkeypatch.setitem(
        tk.config, "ckanext.datapusher_plus.dictionary_stash_dir", str(target)
    )

    # load on a missing-stash path → no side effects.
    assert dictionary_stash.load("res-no-mkdir") is None
    assert not target.exists()

    # clear on a missing-stash path → no side effects.
    dictionary_stash.clear("res-no-mkdir")
    assert not target.exists()

    # Only save bootstraps the directory.
    dictionary_stash.save("res-mkdir", {"k": {"label": "v"}})
    assert target.exists()


# ---------------------------------------------------------------------------
# Retry-after-failure: AnalysisStage._parse_stats picks up the stash
# when no live datastore resource exists.
#
# This is the substantive fix from roborev #2222 MEDIUM. Without these
# tests, a silent regression that drops the stash-load branch would
# go unnoticed by the rest of the suite (the analysis stage's stash
# wiring is otherwise only exercised by the integration suite).
# ---------------------------------------------------------------------------


@pytest.fixture
def minimal_stats_csv(tmp_path):
    """A 2-column stats CSV that ``_parse_stats`` can parse end-to-end.

    Only the columns the method actually reads are populated: ``field``,
    ``type``, ``min``, ``max``, ``cardinality``. ``qsv_*`` rows are
    sentinel trailers in qsv stats output — including one ensures the
    parser's break condition is exercised.
    """
    csv_path = tmp_path / "stats.csv"
    csv_path.write_text(
        "field,type,min,max,cardinality\n"
        "name,String,Alice,Zach,42\n"
        "qty,Integer,1,1000,500\n"
        "qsv__rowcount,Integer,0,0,0\n",
        encoding="utf-8",
    )
    return str(csv_path)


@pytest.fixture
def analysis_stage_context(stash_module, tmp_path):
    """A minimal-but-real wiring of ``AnalysisStage._parse_stats``.

    Why the heavy fixture rather than a SimpleNamespace stand-in:
    ``_parse_stats`` itself is the seam where the retry-restore lives,
    so the test has to exercise that real method. Everything around
    it that *isn't* the focus (qsv, frequency tables, type inference)
    is upstream of this call and doesn't run here.
    """
    pytest.importorskip("ckan")
    from types import SimpleNamespace
    from unittest import mock

    from ckanext.datapusher_plus.jobs.stages.analysis import AnalysisStage

    stage = AnalysisStage()
    context = SimpleNamespace(
        resource_id="res-retry-restore",
        logger=mock.Mock(),
        existing_info=None,
        add_stat=mock.Mock(),
    )
    return stage, context


def test_retry_restore_loads_stash_when_no_live_datastore(
    analysis_stage_context, minimal_stats_csv, monkeypatch
):
    # The failure-then-retry scenario from roborev #2222 MEDIUM:
    # a previous attempt deleted the datastore resource and stashed
    # the dictionary before crashing. On retry, no live datastore is
    # found, but the stash IS — and ``_parse_stats`` must load it
    # into ``existing_info`` so the merge logic propagates it onto
    # the rebuilt datastore.
    from ckanext.datapusher_plus.jobs.stages import analysis as analysis_mod

    stage, context = analysis_stage_context
    stashed = {
        "name": {"label": "Customer name", "type_override": "text"},
        "qty": {"label": "Quantity ordered", "type_override": "numeric"},
    }
    analysis_mod.dict_stash.save(context.resource_id, stashed)

    # No live datastore for this resource_id.
    monkeypatch.setattr(
        analysis_mod.dsu, "datastore_resource_exists", lambda rid: None
    )

    headers_dicts, _datetimecols, _stats = stage._parse_stats(
        context, minimal_stats_csv, {0: "name", 1: "qty"}
    )

    # The stash was loaded as existing_info.
    assert context.existing_info == stashed
    # The downstream merge applied it onto the rebuilt headers — both
    # fields carry the original info dicts on the rebuilt datastore.
    by_id = {h["id"]: h for h in headers_dicts}
    assert by_id["name"]["info"] == stashed["name"]
    assert by_id["qty"]["info"] == stashed["qty"]
    # Restoration was logged at info level with a "retry-after-failure"
    # tell, so operators can spot it.
    info_log_messages = [
        call.args[0] for call in context.logger.info.call_args_list
    ]
    assert any(
        "retry-after-failure" in msg.lower() for msg in info_log_messages
    ), info_log_messages


def test_retry_restore_is_noop_when_no_stash(
    analysis_stage_context, minimal_stats_csv, monkeypatch
):
    # First-run-with-no-prior-dictionary case. No live datastore, no
    # stash — ``existing_info`` must stay ``None`` and no info-level
    # "restoring" log fires. (Catches a regression where the branch
    # accidentally synthesizes a non-None empty dict.)
    from ckanext.datapusher_plus.jobs.stages import analysis as analysis_mod

    stage, context = analysis_stage_context
    monkeypatch.setattr(
        analysis_mod.dsu, "datastore_resource_exists", lambda rid: None
    )

    stage._parse_stats(context, minimal_stats_csv, {0: "name", 1: "qty"})

    assert context.existing_info is None
    info_log_messages = [
        call.args[0] for call in context.logger.info.call_args_list
    ]
    assert not any(
        "retry-after-failure" in msg.lower() for msg in info_log_messages
    ), info_log_messages


def test_retry_restore_does_not_fire_when_live_datastore_exists(
    analysis_stage_context, minimal_stats_csv, monkeypatch
):
    # When the datastore DOES exist, the existing-resource branch wins
    # and the stash is ignored on this path (the stash will be re-saved
    # before the delete on the same run). Belt-and-braces: a stash
    # left over from an unrelated prior run must not silently override
    # what's currently in the datastore.
    from ckanext.datapusher_plus.jobs.stages import analysis as analysis_mod

    stage, context = analysis_stage_context
    # A stale stash from a long-ago failed run.
    analysis_mod.dict_stash.save(
        context.resource_id, {"name": {"label": "ANCIENT"}}
    )
    # The live datastore reports DIFFERENT info — this is what wins.
    live_info = {"label": "FRESH"}
    monkeypatch.setattr(
        analysis_mod.dsu,
        "datastore_resource_exists",
        lambda rid: {"fields": [{"id": "name", "info": live_info}]},
    )
    # The branch that follows (delete + stash-save) is not under test
    # here — stub the side-effecty calls so we don't reach CKAN action
    # internals that aren't available in this unit-test scaffold.
    monkeypatch.setattr(
        analysis_mod.dsu, "delete_datastore_resource", lambda rid: None
    )

    stage._parse_stats(context, minimal_stats_csv, {0: "name", 1: "qty"})

    # The live datastore's info won, not the stale stash.
    assert context.existing_info == {"name": live_info}


# ---------------------------------------------------------------------------
# _rollback_database type-mapping (Copilot #307 finding)
# ---------------------------------------------------------------------------


def test_rollback_restore_derives_field_type_from_type_override(
    stash_module, tmp_path, monkeypatch
):
    # Copilot #307: every other call site to ``send_resource_to_datastore``
    # in this codebase passes headers with ``id``, ``type``, AND ``info``.
    # If the rollback's restore omits ``type``, CKAN's ``datastore_create``
    # falls back to ``text`` for every column — a column the operator
    # originally annotated ``numeric`` or ``timestamp`` would be silently
    # restored as ``text``. The rollback hook now derives ``type`` from
    # each stashed entry's ``info["type_override"]`` so the rebuilt
    # zero-row resource matches the dictionary's declared types.
    pytest.importorskip("ckan")
    from types import SimpleNamespace
    from unittest import mock

    from ckanext.datapusher_plus.jobs import prefect_flow

    resource_id = "res-rollback-types"
    stashed = {
        "name": {"label": "Name", "type_override": "text"},
        "qty": {"label": "Quantity", "type_override": "numeric"},
        "when": {"label": "Created at", "type_override": "timestamp"},
        "blob": {"label": "Untyped"},  # no type_override → text fallback
    }
    stash_module.save(resource_id, stashed)

    # Stub the runtime + dsu so the rollback runs in isolation. We only
    # care that ``send_resource_to_datastore`` is called with the right
    # ``headers`` shape, not that any real CKAN action fires.
    runtime = SimpleNamespace(resource_id=resource_id, logger=mock.Mock())
    monkeypatch.setattr(prefect_flow, "_runtime_or_none", lambda: runtime)
    monkeypatch.setattr(
        prefect_flow.dsu, "delete_datastore_resource", lambda rid: None
    )
    captured = {}
    def _capture(**kwargs):
        captured.update(kwargs)
        return {}
    monkeypatch.setattr(prefect_flow.dsu, "send_resource_to_datastore", _capture)

    prefect_flow._rollback_database(txn=None)

    # Headers were assembled with type derived from type_override.
    by_id = {h["id"]: h for h in captured["headers"]}
    assert by_id["name"]["type"] == "text"
    assert by_id["qty"]["type"] == "numeric"
    assert by_id["when"]["type"] == "timestamp"
    # No type_override → safe text fallback (not omitted).
    assert by_id["blob"]["type"] == "text"
    # All entries still carry their info dict.
    assert by_id["qty"]["info"]["type_override"] == "numeric"


def test_rollback_restore_ignores_unknown_type_override(
    stash_module, tmp_path, monkeypatch
):
    # Defensive: if a stash has a ``type_override`` that isn't in
    # ``TYPE_MAPPING.values()`` (corruption / older format / future
    # type we don't know about), fall back to ``text`` rather than
    # passing an unknown type through to ``datastore_create``.
    pytest.importorskip("ckan")
    from types import SimpleNamespace
    from unittest import mock

    from ckanext.datapusher_plus.jobs import prefect_flow

    resource_id = "res-rollback-unknown-type"
    stash_module.save(
        resource_id,
        {"weird": {"label": "Weird", "type_override": "not-a-real-pg-type"}},
    )

    runtime = SimpleNamespace(resource_id=resource_id, logger=mock.Mock())
    monkeypatch.setattr(prefect_flow, "_runtime_or_none", lambda: runtime)
    monkeypatch.setattr(
        prefect_flow.dsu, "delete_datastore_resource", lambda rid: None
    )
    captured = {}
    monkeypatch.setattr(
        prefect_flow.dsu,
        "send_resource_to_datastore",
        lambda **kwargs: captured.update(kwargs) or {},
    )

    prefect_flow._rollback_database(txn=None)

    by_id = {h["id"]: h for h in captured["headers"]}
    assert by_id["weird"]["type"] == "text"
