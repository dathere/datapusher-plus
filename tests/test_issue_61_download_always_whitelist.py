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

These tests pin seven properties:

1. The live-read parser handles an unset / empty value as
   "no host bypass" (the default behavior).
2. Whitespace-separated strings and direct list values both parse
   into a lowercased ``frozenset`` (case folding makes operator-typed
   ``DATA.GOV`` match ``data.gov`` on the wire).
3. ``_host_in_always_whitelist`` matches exact hostnames
   case-insensitively, strips ports, returns the matched host (so
   the caller can log it without a second ``urlparse``), and is
   robust against missing/unparseable URLs (returns ``None``
   rather than raising).
4. Matching is exact — ``data.gov`` in the whitelist does NOT
   match ``subdomain.data.gov``. Pinned so a future "support
   subdomains" patch breaks this test rather than quietly
   changing behavior.
5. ``_should_skip_upload`` returns ``False`` when the host is in the
   whitelist even when the byte hash matches — the core promise.
6. ``_should_skip_upload`` preserves its prior skip behavior for
   hosts NOT in the whitelist (regression guard against an
   inadvertent "always re-process everyone" bug).
7. Live-read takes effect on the next call (roborev #2293 LOW —
   the ``editable: true`` declaration is now truthful).
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest


REPO_ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Fixtures: set the whitelist via tk.config, since reads are now live
# ---------------------------------------------------------------------------


@pytest.fixture
def set_whitelist(monkeypatch):
    """Set ``ckanext.datapusher_plus.download_always_whitelist`` live
    on ``tk.config``. Returns a setter the test can call multiple
    times to simulate admin-UI runtime edits."""
    pytest.importorskip("ckan")
    import ckan.plugins.toolkit as tk

    def _set(value):
        monkeypatch.setitem(
            tk.config,
            "ckanext.datapusher_plus.download_always_whitelist",
            value,
        )

    return _set


@pytest.fixture
def download_module():
    """Return the download stage module without reloading (live-read
    means no reload dance is needed)."""
    pytest.importorskip("ckan")
    import ckanext.datapusher_plus.jobs.stages.download as download

    return download


# ---------------------------------------------------------------------------
# Live-read parsing
# ---------------------------------------------------------------------------


def test_unset_config_parses_to_empty_frozenset(monkeypatch, download_module):
    # Default behavior — operators without the setting must not get a
    # surprise "everyone is whitelisted" or a crash on first download.
    pytest.importorskip("ckan")
    import ckan.plugins.toolkit as tk

    monkeypatch.delitem(
        tk.config,
        "ckanext.datapusher_plus.download_always_whitelist",
        raising=False,
    )
    parsed = download_module._get_download_always_whitelist()
    assert parsed == frozenset()
    assert isinstance(parsed, frozenset)


def test_empty_string_parses_to_empty_frozenset(set_whitelist, download_module):
    set_whitelist("")
    assert download_module._get_download_always_whitelist() == frozenset()


def test_whitespace_separated_string_parses_to_lowercased_frozenset(
    set_whitelist, download_module
):
    # Mirrors the existing ``FORMATS`` pattern: whitespace-separated
    # in ``ckan.ini``, parsed by the live-read function. Lowercasing
    # happens here so the hot-path lookup can use ``in`` directly
    # without re-folding.
    set_whitelist("Data.Internal.Gov   data.partner.org\tlocalhost")
    assert download_module._get_download_always_whitelist() == frozenset(
        {"data.internal.gov", "data.partner.org", "localhost"}
    )


def test_list_value_parses_to_lowercased_frozenset(
    set_whitelist, download_module
):
    # Some CKAN config layers (env-vars via ``CKAN_INI`` overrides,
    # programmatic ``tk.config`` patches in tests) pass list values
    # through directly. The ``isinstance(str)`` branch must leave a
    # real list alone.
    set_whitelist(["Foo.example", "BAR.example"])
    assert download_module._get_download_always_whitelist() == frozenset(
        {"foo.example", "bar.example"}
    )


def test_empty_strings_in_split_are_filtered(set_whitelist, download_module):
    # If a future comma-tolerant parser is added, the ``if h`` guard
    # catches empty entries so the frozenset doesn't contain ``""``
    # (which would match every hostless URL).
    set_whitelist("data.gov  partner.org")
    assert "" not in download_module._get_download_always_whitelist()


def test_malformed_value_degrades_to_empty_frozenset(
    set_whitelist, download_module
):
    # Defensive: a non-iterable / non-string value (e.g., someone
    # pokes an int into the live config via a broken admin UI) must
    # not crash the download stage — falls back to "no host bypass."
    set_whitelist(12345)
    assert download_module._get_download_always_whitelist() == frozenset()


# ---------------------------------------------------------------------------
# Helper: _host_in_always_whitelist returns matched host (or None)
# ---------------------------------------------------------------------------


def test_empty_whitelist_returns_none(set_whitelist, download_module):
    # If no whitelist is configured, the helper must short-circuit
    # ``None`` so it can't even mis-match the empty string against a
    # hostless URL.
    set_whitelist("")
    assert (
        download_module._host_in_always_whitelist("https://anything.example")
        is None
    )


def test_exact_hostname_match_returns_lowercased_host(
    set_whitelist, download_module
):
    # Returning the matched host (not just True) lets the caller
    # log it without a second urlparse call (roborev #2293 LOW).
    set_whitelist("data.internal.gov localhost 10.0.0.5")
    matched = download_module._host_in_always_whitelist(
        "https://data.internal.gov/csv/file.csv"
    )
    assert matched == "data.internal.gov"


def test_non_matching_hostname_returns_none(set_whitelist, download_module):
    set_whitelist("data.internal.gov")
    assert (
        download_module._host_in_always_whitelist(
            "https://data.external.gov/csv/file.csv"
        )
        is None
    )


def test_match_is_case_insensitive(set_whitelist, download_module):
    # URL hosts are DNS-case-insensitive; the helper must fold the
    # URL's host to lowercase before set lookup or it'd miss
    # ``https://DATA.INTERNAL.GOV/...`` (which a CDN or legacy CMS
    # could plausibly emit).
    set_whitelist("data.internal.gov")
    matched = download_module._host_in_always_whitelist(
        "https://DATA.INTERNAL.GOV/file.csv"
    )
    assert matched == "data.internal.gov"


def test_port_is_stripped_before_match(set_whitelist, download_module):
    # ``urlparse.hostname`` already drops the port, but pin this so a
    # future "use raw netloc" refactor would break this test before
    # it breaks production for ``localhost:5050`` deployments.
    set_whitelist("localhost")
    assert (
        download_module._host_in_always_whitelist(
            "http://localhost:5050/api/3/action/..."
        )
        == "localhost"
    )


def test_ip_address_host_matches(set_whitelist, download_module):
    # IPv4 literals are valid hosts; ``hostname`` returns them as-is.
    # Used by deployments that pin internal services by IP.
    set_whitelist("10.0.0.5")
    assert (
        download_module._host_in_always_whitelist(
            "http://10.0.0.5/csv/file.csv"
        )
        == "10.0.0.5"
    )


def test_subdomain_does_not_match_parent_domain(
    set_whitelist, download_module
):
    # roborev #2293 LOW pin: matching is exact-hostname, not
    # suffix. Listing ``data.internal.gov`` must NOT also match
    # ``sub.data.internal.gov``. Operators who want subdomain
    # behavior list each host separately. A future "support
    # subdomain wildcards" change should break this test rather
    # than quietly altering the semantic.
    set_whitelist("data.internal.gov")
    assert (
        download_module._host_in_always_whitelist(
            "https://sub.data.internal.gov/file.csv"
        )
        is None
    )


def test_parent_domain_does_not_match_subdomain_entry(
    set_whitelist, download_module
):
    # The reverse direction — listing the subdomain must not match
    # the parent. Catches a naive ``endswith`` implementation.
    set_whitelist("sub.data.internal.gov")
    assert (
        download_module._host_in_always_whitelist(
            "https://data.internal.gov/file.csv"
        )
        is None
    )


def test_unparseable_url_returns_none(set_whitelist, download_module):
    # Garbage in → ``None`` out, not an exception. The download
    # stage handles its own URL validation elsewhere
    # (``_validate_url_scheme``); this helper should not become a
    # second exception source on the hot path.
    set_whitelist("data.gov")
    assert (
        download_module._host_in_always_whitelist("://://broken") is None
    )


def test_empty_url_returns_none(set_whitelist, download_module):
    set_whitelist("data.gov")
    assert download_module._host_in_always_whitelist("") is None


def test_none_url_returns_none(set_whitelist, download_module):
    # Defensive against ``context.resource.get("url")`` returning
    # ``None`` (which is technically possible if the resource record
    # is malformed — surface as a non-match rather than an
    # ``AttributeError`` on ``.hostname``).
    set_whitelist("data.gov")
    assert download_module._host_in_always_whitelist(None) is None


def test_hostless_url_returns_none(set_whitelist, download_module):
    # ``file:///tmp/foo.csv`` parses cleanly but has an empty host;
    # must not match an empty-string entry in the set.
    set_whitelist("data.gov")
    assert (
        download_module._host_in_always_whitelist("file:///tmp/foo.csv")
        is None
    )


def test_live_read_picks_up_runtime_changes(set_whitelist, download_module):
    # The live-read pattern (issue #221 / file_hash_algorithm) must
    # actually pick up a runtime change without any reload. This is
    # the "editable: true is truthful" property — without it, the
    # admin-UI knob would be a lie.
    set_whitelist("first.example")
    assert (
        download_module._host_in_always_whitelist("https://first.example/x")
        == "first.example"
    )
    # Operator edits the whitelist via the admin UI…
    set_whitelist("second.example")
    # …and the next call sees the new value, no restart needed.
    assert (
        download_module._host_in_always_whitelist("https://first.example/x")
        is None
    )
    assert (
        download_module._host_in_always_whitelist("https://second.example/x")
        == "second.example"
    )


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


def test_should_skip_upload_returns_false_when_host_whitelisted(
    set_whitelist, download_module, monkeypatch
):
    # The core promise of #61: a host in the whitelist forces
    # re-processing even when the byte hash matches what's already
    # stored on the resource.
    set_whitelist("data.internal.gov")

    stage = download_module.DownloadStage()
    ctx = _make_context(
        url="https://data.internal.gov/csv/file.csv",
        file_hash="matching-hash",
    )

    assert stage._should_skip_upload(ctx, "matching-hash", {}) is False
    # And the operator-facing log line fired so they can see WHY a
    # whitelisted host bypassed the dedup path. The matched host
    # is one of the positional args (so the log adapter formats it
    # in regardless of %-vs-{} style).
    ctx.logger.info.assert_called_once()
    log_args = ctx.logger.info.call_args
    assert "data.internal.gov" in log_args.args


def test_should_skip_upload_preserves_skip_for_non_whitelisted(
    set_whitelist, download_module, monkeypatch
):
    # Regression guard: a host NOT in the whitelist must still skip
    # when the hash matches, ``ignore_hash`` is false, and
    # ``IGNORE_FILE_HASH`` is false. Without this test, an
    # accidentally-inverted whitelist check (e.g., ``not in``) would
    # silently re-process every resource — bypassing the whole
    # dedup optimization the stage was built around.
    set_whitelist("data.internal.gov")
    # Force IGNORE_FILE_HASH false so the hash check is the deciding
    # factor (otherwise the existing global override would also
    # return False and mask a buggy whitelist).
    monkeypatch.setattr(download_module.conf, "IGNORE_FILE_HASH", False)

    stage = download_module.DownloadStage()
    ctx = _make_context(
        url="https://data.external.gov/csv/file.csv",
        file_hash="matching-hash",
    )

    assert stage._should_skip_upload(ctx, "matching-hash", {}) is True


def test_should_skip_upload_empty_whitelist_preserves_legacy_behavior(
    set_whitelist, download_module, monkeypatch
):
    # Operators who never set the new key get exactly the pre-#61
    # behavior — hash match means skip. Without this test, a future
    # refactor that accidentally inverted the "empty whitelist
    # short-circuit" path could break upgrade-in-place deployments.
    set_whitelist("")
    monkeypatch.setattr(download_module.conf, "IGNORE_FILE_HASH", False)

    stage = download_module.DownloadStage()
    ctx = _make_context(
        url="https://data.internal.gov/csv/file.csv",
        file_hash="matching-hash",
    )

    assert stage._should_skip_upload(ctx, "matching-hash", {}) is True


# ---------------------------------------------------------------------------
# Declaration-vs-config drift guard (#179-style)
# ---------------------------------------------------------------------------


def test_config_declaration_default_matches_live_read_fallback():
    """Both sources of truth for the default whitelist must agree.

    Drift between ``config_declaration.yaml`` and the live-read
    fallback in ``download.py`` was the root cause of issue #179
    (date → timestamp casting). Pin the same property for #61 so a
    future "let's only update one of them" PR fails this test
    instead of silently shipping inconsistent defaults.

    Now that the value is read live (roborev #2293 LOW resolution),
    the fallback lives in ``download.py:_get_download_always_whitelist``
    rather than ``config.py`` — AST-parse that file instead.
    """
    import ast

    # PyYAML import guarded the same way the #112 / #142 drift-guards
    # do — CI environments that run pytest without the extension's
    # full dep set still get a useful skip rather than an ImportError.
    try:
        import yaml
    except ImportError:
        pytest.skip("PyYAML not available")

    # Iterate all groups + options so this guard survives a future
    # ``config_declaration.yaml`` restructure (additional groups, key
    # moved between groups, etc.) — same shape as #112's drift guard.
    yaml_path = (
        REPO_ROOT
        / "ckanext"
        / "datapusher_plus"
        / "config_declaration.yaml"
    )
    decl = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
    target_key = "ckanext.datapusher_plus.download_always_whitelist"
    whitelist_decl = None
    for group in decl.get("groups", []):
        for opt in group.get("options", []):
            if opt.get("key") == target_key:
                whitelist_decl = opt
                break
        if whitelist_decl is not None:
            break
    assert whitelist_decl is not None, (
        f"{target_key} is missing from config_declaration.yaml — "
        "issue #61 should declare it."
    )
    assert whitelist_decl["default"] == ""
    assert whitelist_decl["editable"] is True

    # Read download.py live-read fallback via AST (avoid CKAN bootstrap)
    download_py = (
        REPO_ROOT
        / "ckanext"
        / "datapusher_plus"
        / "jobs"
        / "stages"
        / "download.py"
    ).read_text()
    tree = ast.parse(download_py)
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
        "download.py live-read fallback for "
        "download_always_whitelist must match the YAML declaration "
        "default (both empty string)."
    )
