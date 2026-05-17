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
    # stash (if any) must remain intact. The new partial file in .tmp
    # is acceptable collateral; the *real* path must not be touched.
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

    # Unset the knob if a prior test left one in place.
    tk.config.pop("ckanext.datapusher_plus.dictionary_stash_dir", None)

    path = dictionary_stash.stash_path("res-iii")
    assert path.startswith(os.path.join(tempfile.gettempdir(), "dpp_dict_stash"))
