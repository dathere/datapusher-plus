# -*- coding: utf-8 -*-
"""
Regression coverage for issue #61 ("Smart resource download").

Issue #61 (filed 2023-02-03) originally proposed a
``DOWNLOAD_ALWAYS_WHITELIST`` that paired with a never-built
``DOWNLOAD_PREVIEW_ONLY`` partial-download mode. DP+'s architecture
landed on "always download the full file for comprehensive metadata
inference," so the partial-download parent is moot.

The 2026-05-19 reinterpretation (kept in the BACKLOG-SWEEP3 handoff
doc): keep the ``DOWNLOAD_ALWAYS_WHITELIST`` name but repurpose it
as an operator-controlled list of hostnames that **always trigger
re-processing**, bypassing the file-hash-based upload-skip
optimization in ``DownloadStage._should_skip_upload``. The use
cases:

  * Daily-refreshed reports that overwrite the same URL with the same
    bytes (so the file hash matches) but logically-different data —
    operators want each push re-analyzed regardless of hash.
  * Local / peered hosts where re-download cost is negligible and the
    operator wants forced re-analysis on every push.

These tests pin six properties:

1. The config parses an empty default to an empty ``frozenset``
   (default behavior is "no host bypass").
2. A whitespace-separated string of hostnames parses into a
   lowercased ``frozenset`` (case folding makes operator-typed
   ``DATA.GOV`` match ``data.gov`` on the wire).
3. ``_host_in_always_whitelist`` matches exact hostnames
   case-insensitively, strips ports, and is robust against
   missing/unparseable URLs (returns ``False`` rather than raising).
4. ``_should_skip_upload`` returns ``False`` when the host is in the
   whitelist even when the byte hash matches — the core promise.
5. ``_should_skip_upload`` preserves its prior skip behavior for
   hosts NOT in the whitelist (regression guard against an
   inadvertent "always re-process everyone" bug).
6. The YAML declaration default agrees with the ``config.py``
   import-time fallback (the #179-style declaration-vs-fallback
   drift guard).
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest


REPO_ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Config parsing
# ---------------------------------------------------------------------------


def _reimport_conf(monkeypatch, value):
    """Reload config.py with the given whitelist value applied to
    ``tk.config`` so the import-time parse picks it up. Returns the
    freshly-imported module."""
    import importlib

    pytest.importorskip("ckan")
    import ckan.plugins.toolkit as tk

    monkeypatch.setitem(
        tk.config, "ckanext.datapusher_plus.download_always_whitelist", value
    )
    import ckanext.datapusher_plus.config as conf

    return importlib.reload(conf)


def test_empty_config_parses_to_empty_frozenset(monkeypatch):
    # Default behavior — operators without the setting must not get a
    # surprise "everyone is whitelisted" or a crash on first download.
    conf = _reimport_conf(monkeypatch, "")
    assert conf.DOWNLOAD_ALWAYS_WHITELIST == frozenset()
    assert isinstance(conf.DOWNLOAD_ALWAYS_WHITELIST, frozenset)


def test_whitespace_separated_string_parses_to_lowercased_frozenset(
    monkeypatch,
):
    # Mirrors the existing ``FORMATS`` pattern: whitespace-separated
    # in ``ckan.ini``, parsed at import. Lowercasing happens here so
    # the hot-path ``_host_in_always_whitelist`` doesn't have to
    # re-fold the set on every call.
    conf = _reimport_conf(
        monkeypatch,
        "Data.Internal.Gov   data.partner.org\tlocalhost",
    )
    assert conf.DOWNLOAD_ALWAYS_WHITELIST == frozenset(
        {"data.internal.gov", "data.partner.org", "localhost"}
    )


def test_list_value_parses_to_lowercased_frozenset(monkeypatch):
    # Some CKAN config layers (env-vars via ``CKAN_INI`` overrides,
    # programmatic ``tk.config`` patches in tests) pass list values
    # through directly. The branch that handles ``isinstance(str)``
    # must leave a real list alone.
    conf = _reimport_conf(monkeypatch, ["Foo.example", "BAR.example"])
    assert conf.DOWNLOAD_ALWAYS_WHITELIST == frozenset(
        {"foo.example", "bar.example"}
    )


def test_empty_strings_in_split_are_filtered(monkeypatch):
    # ``"  data.gov  ".split()`` already drops empties, but if the
    # operator writes ``"data.gov, , partner.org"`` and a downstream
    # comma-tolerant parser is added later, the comprehension's
    # ``if h`` guard catches the empty entries so the frozenset
    # doesn't contain ``""`` (which would match every hostless URL).
    conf = _reimport_conf(monkeypatch, "data.gov  partner.org")
    assert "" not in conf.DOWNLOAD_ALWAYS_WHITELIST


# ---------------------------------------------------------------------------
# Helper: _host_in_always_whitelist
# ---------------------------------------------------------------------------


@pytest.fixture
def helper(monkeypatch):
    """Return the freshly-imported ``_host_in_always_whitelist`` with
    a known whitelist applied. Each test gets its own fresh module
    state so monkeypatching is hermetic."""
    pytest.importorskip("ckan")
    _reimport_conf(monkeypatch, "data.internal.gov localhost 10.0.0.5")
    # Reload the download module too so its ``conf`` reference picks
    # up the reloaded constant. Without this, the function closes
    # over an old conf object and the test would see the old value.
    import importlib
    import ckanext.datapusher_plus.jobs.stages.download as download

    download = importlib.reload(download)
    return download._host_in_always_whitelist


def test_empty_whitelist_returns_false(monkeypatch):
    # If no whitelist is configured, the helper must short-circuit
    # ``False`` so it can't even mis-match the empty string against a
    # hostless URL.
    pytest.importorskip("ckan")
    _reimport_conf(monkeypatch, "")
    import importlib
    import ckanext.datapusher_plus.jobs.stages.download as download

    download = importlib.reload(download)
    assert download._host_in_always_whitelist("https://anything.example") is False


def test_exact_hostname_match_returns_true(helper):
    assert helper("https://data.internal.gov/csv/file.csv") is True


def test_non_matching_hostname_returns_false(helper):
    assert helper("https://data.external.gov/csv/file.csv") is False


def test_match_is_case_insensitive(helper):
    # URL hosts are DNS-case-insensitive; the helper must fold the
    # URL's host to lowercase before set lookup or it'd miss
    # ``https://DATA.INTERNAL.GOV/...`` (which a CDN or legacy CMS
    # could plausibly emit).
    assert helper("https://DATA.INTERNAL.GOV/file.csv") is True


def test_port_is_stripped_before_match(helper):
    # ``urlparse.hostname`` already drops the port, but pin this so a
    # future "use raw netloc" refactor would break this test before
    # it breaks production for ``localhost:5050`` deployments.
    assert helper("http://localhost:5050/api/3/action/...") is True


def test_ip_address_host_matches(helper):
    # IPv4 literals are valid hosts; ``hostname`` returns them as-is.
    # Used by deployments that pin internal services by IP.
    assert helper("http://10.0.0.5/csv/file.csv") is True


def test_unparseable_url_returns_false(helper):
    # Garbage in → ``False`` out, not an exception. The download
    # stage handles its own URL validation elsewhere
    # (``_validate_url_scheme``); this helper should not become a
    # second exception source on the hot path.
    assert helper("://://broken") is False


def test_empty_url_returns_false(helper):
    assert helper("") is False


def test_none_url_returns_false(helper):
    # Defensive against ``context.resource.get("url")`` returning
    # ``None`` (which is technically possible if the resource record
    # is malformed — surface as a non-match rather than an
    # ``AttributeError`` on ``.hostname``).
    assert helper(None) is False


def test_hostless_url_returns_false(helper):
    # ``file:///tmp/foo.csv`` parses cleanly but has an empty host;
    # must not match an empty-string entry in the set (the
    # ``if h`` filter in config.py guarantees the set has no ``""``,
    # but pin the helper's own guard too).
    assert helper("file:///tmp/foo.csv") is False


# ---------------------------------------------------------------------------
# Integration: _should_skip_upload honors the whitelist
# ---------------------------------------------------------------------------


def _make_context(url, file_hash="abc123", last_modified=None):
    """Build a minimal stand-in for ``ProcessingContext`` shaped just
    enough for ``_should_skip_upload`` to run. Uses ``SimpleNamespace``
    + ``MagicMock`` for the logger so the production code's
    ``context.logger.info`` call is silent + assertable."""
    return SimpleNamespace(
        resource={
            "url": url,
            "hash": file_hash,
            "last_modified": last_modified,
        },
        metadata={},
        logger=mock.MagicMock(),
    )


def test_should_skip_upload_returns_false_when_host_whitelisted(monkeypatch):
    # The core promise of #61: a host in the whitelist forces
    # re-processing even when the byte hash matches what's already
    # stored on the resource.
    pytest.importorskip("ckan")
    _reimport_conf(monkeypatch, "data.internal.gov")
    import importlib
    import ckanext.datapusher_plus.jobs.stages.download as download

    download = importlib.reload(download)

    stage = download.DownloadStage()
    ctx = _make_context(
        url="https://data.internal.gov/csv/file.csv",
        file_hash="matching-hash",
    )

    assert stage._should_skip_upload(ctx, "matching-hash", {}) is False
    # And the operator-facing log line fired so they can see WHY a
    # whitelisted host bypassed the dedup path.
    ctx.logger.info.assert_called_once()


def test_should_skip_upload_preserves_skip_for_non_whitelisted(monkeypatch):
    # Regression guard: a host NOT in the whitelist must still skip
    # when the hash matches, ``ignore_hash`` is false, and
    # ``IGNORE_FILE_HASH`` is false. Without this test, an
    # accidentally-inverted whitelist check (e.g., ``not in``) would
    # silently re-process every resource — bypassing the whole
    # dedup optimization the stage was built around.
    pytest.importorskip("ckan")
    _reimport_conf(monkeypatch, "data.internal.gov")
    import importlib
    import ckanext.datapusher_plus.jobs.stages.download as download

    download = importlib.reload(download)
    # Force IGNORE_FILE_HASH false so the hash check is the deciding
    # factor (otherwise the existing global override would also
    # return False and mask a buggy whitelist).
    monkeypatch.setattr(download.conf, "IGNORE_FILE_HASH", False)

    stage = download.DownloadStage()
    ctx = _make_context(
        url="https://data.external.gov/csv/file.csv",
        file_hash="matching-hash",
    )

    assert stage._should_skip_upload(ctx, "matching-hash", {}) is True


def test_should_skip_upload_empty_whitelist_preserves_legacy_behavior(
    monkeypatch,
):
    # Operators who never set the new key get exactly the pre-#61
    # behavior — hash match means skip. Without this test, a future
    # refactor that accidentally inverted the "empty whitelist
    # short-circuit" path could break upgrade-in-place deployments.
    pytest.importorskip("ckan")
    _reimport_conf(monkeypatch, "")
    import importlib
    import ckanext.datapusher_plus.jobs.stages.download as download

    download = importlib.reload(download)
    monkeypatch.setattr(download.conf, "IGNORE_FILE_HASH", False)

    stage = download.DownloadStage()
    ctx = _make_context(
        url="https://data.internal.gov/csv/file.csv",
        file_hash="matching-hash",
    )

    assert stage._should_skip_upload(ctx, "matching-hash", {}) is True


# ---------------------------------------------------------------------------
# Declaration-vs-fallback drift guard (#179-style)
# ---------------------------------------------------------------------------


def test_config_declaration_default_matches_config_py_fallback():
    """Both sources of truth for the default whitelist must agree.

    Drift between ``config_declaration.yaml`` and ``config.py``'s
    ``tk.config.get(..., default)`` was the root cause of issue #179
    (date → timestamp casting). Pin the same property for #61 so a
    future "let's only update one of them" PR fails this test
    instead of silently shipping inconsistent defaults.
    """
    import ast
    import yaml

    # Read declaration default
    yaml_path = (
        REPO_ROOT
        / "ckanext"
        / "datapusher_plus"
        / "config_declaration.yaml"
    )
    with yaml_path.open() as fh:
        decl = yaml.safe_load(fh)
    options = decl["groups"][0]["options"]
    whitelist_decl = next(
        opt
        for opt in options
        if opt["key"] == "ckanext.datapusher_plus.download_always_whitelist"
    )
    assert whitelist_decl["default"] == ""
    assert whitelist_decl["editable"] is True

    # Read config.py inline fallback via AST (avoid CKAN bootstrap)
    config_py = (
        REPO_ROOT / "ckanext" / "datapusher_plus" / "config.py"
    ).read_text()
    tree = ast.parse(config_py)
    found_default = None
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and getattr(node.func, "attr", None) == "get"
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and node.args[0].value
            == "ckanext.datapusher_plus.download_always_whitelist"
        ):
            # Second positional arg is the fallback
            if len(node.args) >= 2 and isinstance(node.args[1], ast.Constant):
                found_default = node.args[1].value
                break
    assert found_default == "", (
        "config.py inline fallback for download_always_whitelist must "
        "match the YAML declaration default (both empty string)."
    )
