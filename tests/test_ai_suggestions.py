# -*- coding: utf-8 -*-
"""
Unit coverage for the AI-suggestions pipeline addition (PR for #253 →
sub-task #259).

The stage is opt-in (``ckanext.datapusher_plus.enable_ai_suggestions``)
and non-blocking: every failure mode (subprocess error, qsv timeout,
malformed JSON, package-patch failure) must drop a warning and let the
flow continue. These tests exercise:

* ``QSVCommand.describegpt`` argument assembly (no real qsv binary
  required — subprocess.run is mocked).
* ``AISuggestionsStage.should_skip`` honors the config flag.
* ``AISuggestionsStage.process`` happy / failure paths.
* ``helpers.scheming_get_ai_suggestion`` lookup logic across the
  output shapes qsv describegpt emits.

Co-authored with @minhajuddin2510 (AI suggestions feature from PR #253,
re-landed against the current Prefect pipeline).
"""

from __future__ import annotations

import json
import subprocess
from types import SimpleNamespace
from unittest import mock

import pytest


# ---------------------------------------------------------------------------
# QSVCommand.describegpt
# ---------------------------------------------------------------------------


@pytest.fixture
def qsv_command():
    pytest.importorskip("ckan")
    # ``QSVCommand.__init__`` verifies the qsv binary exists and runs a
    # version check — neither is interesting for these tests. Patch
    # both before constructing.
    from ckanext.datapusher_plus import qsv_utils

    with mock.patch.object(qsv_utils.Path, "is_file", return_value=True), \
         mock.patch.object(qsv_utils.QSVCommand, "check_version", return_value=None):
        yield qsv_utils.QSVCommand(logger=mock.Mock())


def _completed(stdout: str = "", returncode: int = 0) -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(
        args=[], returncode=returncode, stdout=stdout, stderr=""
    )


def test_describegpt_default_flags(qsv_command):
    """Default invocation emits --description --dictionary --tags --format JSON.

    Regression note: qsv 20.0.0 does NOT have a ``--json`` flag (an
    earlier draft of this wrapper assumed it did, which caused real
    invocations to fail with ``Unknown flag: '--json'``). Output
    format is controlled by ``--format <Markdown|TSV|JSON|TOON>``.
    """
    with mock.patch("subprocess.run", return_value=_completed("{}")) as run:
        qsv_command.describegpt(input_file="/tmp/sample.csv")

    args = run.call_args[0][0]
    # First arg is the qsv binary path; rest are the subcommand args.
    assert args[1] == "describegpt"
    assert "--description" in args
    assert "--dictionary" in args
    assert "--tags" in args
    # JSON output via --format JSON, NOT --json.
    assert "--format" in args
    assert args[args.index("--format") + 1] == "JSON"
    assert "--json" not in args
    assert args[-1] == "/tmp/sample.csv"


def test_describegpt_prompt_file_threaded_through(qsv_command):
    """``prompt_file`` is passed as ``--prompt-file <path>``."""
    with mock.patch("subprocess.run", return_value=_completed("{}")) as run:
        qsv_command.describegpt(
            input_file="/tmp/sample.csv",
            prompt_file="/etc/qsv/describegpt.toml",
        )

    args = run.call_args[0][0]
    assert "--prompt-file" in args
    idx = args.index("--prompt-file")
    assert args[idx + 1] == "/etc/qsv/describegpt.toml"


def test_describegpt_output_file_threaded_through(qsv_command):
    """``output_file`` is passed as ``--output <path>``."""
    with mock.patch("subprocess.run", return_value=_completed("{}")) as run:
        qsv_command.describegpt(
            input_file="/tmp/sample.csv", output_file="/tmp/out.json"
        )

    args = run.call_args[0][0]
    assert "--output" in args
    idx = args.index("--output")
    assert args[idx + 1] == "/tmp/out.json"


def test_describegpt_skips_disabled_flags(qsv_command):
    """Falsy flags omit their CLI counterpart entirely."""
    with mock.patch("subprocess.run", return_value=_completed("{}")) as run:
        qsv_command.describegpt(
            input_file="/tmp/sample.csv",
            description=False,
            dictionary=False,
            tags=False,
            json_output=False,
        )

    args = run.call_args[0][0]
    assert "--description" not in args
    assert "--dictionary" not in args
    assert "--tags" not in args
    # ``json_output=False`` means no ``--format JSON`` — qsv falls
    # back to its default Markdown report.
    assert "--format" not in args


def test_describegpt_default_timeout_from_config(qsv_command):
    """When ``timeout`` is unset, ``conf.DESCRIBEGPT_TIMEOUT_SECONDS`` wins."""
    from ckanext.datapusher_plus import config as conf

    with mock.patch.object(conf, "DESCRIBEGPT_TIMEOUT_SECONDS", 42), \
         mock.patch("subprocess.run", return_value=_completed("{}")) as run:
        qsv_command.describegpt(input_file="/tmp/sample.csv")

    assert run.call_args.kwargs["timeout"] == 42


def test_describegpt_explicit_timeout_overrides_config(qsv_command):
    """Per-call timeout takes precedence over the config default."""
    from ckanext.datapusher_plus import config as conf

    with mock.patch.object(conf, "DESCRIBEGPT_TIMEOUT_SECONDS", 42), \
         mock.patch("subprocess.run", return_value=_completed("{}")) as run:
        qsv_command.describegpt(input_file="/tmp/sample.csv", timeout=7)

    assert run.call_args.kwargs["timeout"] == 7


def test_describegpt_api_key_threaded_through(qsv_command):
    """``--api-key`` lets callers reach a non-localhost LLM endpoint.

    qsv 20.0.0 refuses to start when the resolved base URL doesn't
    contain ``localhost`` AND no API key is set, with the error
    ``Neither QSV_LLM_BASE_URL nor QSV_LLM_APIKEY environment
    variables are set``. For unauthenticated local LLMs reached via
    container-host hostnames (``host.docker.internal`` etc.) the
    documented incantation is ``--api-key NONE``.
    """
    with mock.patch("subprocess.run", return_value=_completed("{}")) as run:
        qsv_command.describegpt(input_file="/tmp/sample.csv", api_key="NONE")

    args = run.call_args[0][0]
    assert "--api-key" in args
    assert args[args.index("--api-key") + 1] == "NONE"


def test_describegpt_base_url_threaded_through(qsv_command):
    """``--base-url`` overrides the endpoint without rewriting the prompt-file."""
    with mock.patch("subprocess.run", return_value=_completed("{}")) as run:
        qsv_command.describegpt(
            input_file="/tmp/sample.csv",
            base_url="http://host.docker.internal:1234/v1",
        )

    args = run.call_args[0][0]
    assert "--base-url" in args
    assert (
        args[args.index("--base-url") + 1]
        == "http://host.docker.internal:1234/v1"
    )


def test_describegpt_optional_flags_omitted_by_default(qsv_command):
    """``api_key`` / ``base_url`` are None by default — let qsv discover."""
    with mock.patch("subprocess.run", return_value=_completed("{}")) as run:
        qsv_command.describegpt(input_file="/tmp/sample.csv")

    args = run.call_args[0][0]
    assert "--api-key" not in args
    assert "--base-url" not in args


# ---------------------------------------------------------------------------
# AISuggestionsStage.should_skip
# ---------------------------------------------------------------------------


@pytest.fixture
def stage_cls():
    pytest.importorskip("ckan")
    from ckanext.datapusher_plus.jobs.stages.ai_suggestions import AISuggestionsStage

    return AISuggestionsStage


@pytest.fixture
def context_factory():
    def _make(tmp="/tmp/sample.csv", resource=None):
        return SimpleNamespace(
            tmp=tmp,
            resource=resource or {"package_id": "pkg-1"},
            logger=mock.Mock(),
        )

    return _make


def test_should_skip_when_flag_off(stage_cls, context_factory):
    from ckanext.datapusher_plus import config as conf

    with mock.patch.object(conf, "ENABLE_AI_SUGGESTIONS", False):
        assert stage_cls().should_skip(context_factory()) is True


def test_should_not_skip_when_flag_on(stage_cls, context_factory):
    from ckanext.datapusher_plus import config as conf

    with mock.patch.object(conf, "ENABLE_AI_SUGGESTIONS", True):
        assert stage_cls().should_skip(context_factory()) is False


# ---------------------------------------------------------------------------
# AISuggestionsStage.process — happy path
# ---------------------------------------------------------------------------


def test_process_happy_path_persists_suggestions(stage_cls, context_factory):
    """Successful qsv call against the REAL qsv 20.0.0 JSON envelope
    (captured against LM Studio in ``tests/fixtures/qsv_describegpt_sample.json``)
    → ``patch_package`` fires with the reshaped per-field
    ``{value, source}`` suggestions, plus STATUS=DONE and generated_at
    bookkeeping.

    The fixture is committed verbatim so a future qsv schema change
    that breaks the reshape gets caught here rather than only in
    integration. Regenerate via::

        qsv describegpt sample.csv --all --format JSON \\
            --prompt-file <prompt> > tests/fixtures/qsv_describegpt_sample.json
    """
    from pathlib import Path

    from ckanext.datapusher_plus.jobs.stages import ai_suggestions as ai_mod

    fixture_path = Path(__file__).parent / "fixtures" / "qsv_describegpt_sample.json"
    qsv_output = fixture_path.read_text()

    fake_qsv = mock.Mock()
    fake_qsv.describegpt.return_value = _completed(qsv_output)

    captured = {}

    def fake_patch(package):
        captured["package"] = package

    package = {"id": "pkg-1", "name": "widgets-2024"}

    with mock.patch.object(ai_mod, "QSVCommand", return_value=fake_qsv), \
         mock.patch.object(
             ai_mod.dsu, "get_scheming_yaml", return_value=({}, package)
         ), \
         mock.patch.object(ai_mod.dsu, "patch_package", side_effect=fake_patch):
        ctx = context_factory()
        result = stage_cls().process(ctx)

    assert result is ctx, "stage must always return the same context object"
    ai = captured["package"]["dpp_suggestions"]["ai_suggestions"]

    # Description.response (Markdown string from qsv) lands at top
    # level under "description".
    assert ai["description"]["source"] == "qsv describegpt"
    assert "Dataset Description" in ai["description"]["value"]

    # Tags.response.tags list → comma-joined string.
    assert ai["tags"]["source"] == "qsv describegpt"
    assert ai["tags"]["value"] == (
        "product, inventory, ecommerce, retail, pricing, "
        "catalog_data, sku_management, stock_levels"
    )

    # Dictionary.response.fields → per-column entries keyed by ``name``,
    # value = LLM-generated ``description``, label folded into source.
    assert "sku" in ai
    assert ai["sku"]["source"].startswith("qsv describegpt · ")
    assert "Stock Keeping Unit" in ai["sku"]["source"]
    assert "alphanumeric identifier" in ai["sku"]["value"]
    # All four columns from the widgets.csv fixture are present.
    for col in ("sku", "name", "price", "quantity_in_stock"):
        assert col in ai, f"missing per-column entry for {col!r}"
        assert "value" in ai[col]

    # Bookkeeping for the polling JS + audit log.
    assert ai["STATUS"] == "DONE"
    assert "generated_at" in ai


def test_process_reshape_handles_partial_envelope(stage_cls, context_factory):
    """Only Description present → only ``description`` populated; no
    KeyError when ``Dictionary`` / ``Tags`` are missing entirely."""
    from ckanext.datapusher_plus.jobs.stages import ai_suggestions as ai_mod

    partial = {
        "Description": {"response": "Just a description.", "reasoning": "",
                        "token_usage": {}},
    }
    fake_qsv = mock.Mock()
    fake_qsv.describegpt.return_value = _completed(json.dumps(partial))

    captured = {}

    def fake_patch(package):
        captured["package"] = package

    with mock.patch.object(ai_mod, "QSVCommand", return_value=fake_qsv), \
         mock.patch.object(
             ai_mod.dsu, "get_scheming_yaml",
             return_value=({}, {"id": "pkg-1"}),
         ), \
         mock.patch.object(ai_mod.dsu, "patch_package", side_effect=fake_patch):
        stage_cls().process(context_factory())

    ai = captured["package"]["dpp_suggestions"]["ai_suggestions"]
    assert ai["description"]["value"] == "Just a description."
    assert "tags" not in ai
    # STATUS + generated_at always present.
    assert ai["STATUS"] == "DONE"


def test_process_reshape_skips_column_named_description(stage_cls, context_factory):
    """A CSV column literally named ``description`` must NOT overwrite the
    dataset-level Description envelope. Same for ``tags`` / ``STATUS`` /
    ``generated_at`` (the bookkeeping keys). Regression test for the
    Medium finding on roborev #2206."""
    from ckanext.datapusher_plus.jobs.stages import ai_suggestions as ai_mod

    envelope = {
        "Description": {
            "response": "Dataset-level description.",
            "reasoning": "", "token_usage": {},
        },
        "Tags": {
            "response": {"tags": ["dataset", "level"]},
            "reasoning": "", "token_usage": {},
        },
        "Dictionary": {
            "response": {
                "fields": [
                    # Two colliding columns + one good one.
                    {"name": "description",
                     "label": "User Notes",
                     "description": "The notes column from the CSV."},
                    {"name": "tags",
                     "label": "Item Tags",
                     "description": "The per-row tag list."},
                    {"name": "sku",
                     "label": "SKU",
                     "description": "Unique stock keeping unit."},
                ],
            },
            "reasoning": "", "token_usage": {},
        },
    }
    fake_qsv = mock.Mock()
    fake_qsv.describegpt.return_value = _completed(json.dumps(envelope))

    captured = {}
    with mock.patch.object(ai_mod, "QSVCommand", return_value=fake_qsv), \
         mock.patch.object(
             ai_mod.dsu, "get_scheming_yaml",
             return_value=({}, {"id": "pkg-1"}),
         ), \
         mock.patch.object(
             ai_mod.dsu, "patch_package",
             side_effect=lambda pkg: captured.update(package=pkg),
         ):
        stage_cls().process(context_factory())

    ai = captured["package"]["dpp_suggestions"]["ai_suggestions"]

    # Dataset-level entries survived intact.
    assert ai["description"]["value"] == "Dataset-level description."
    assert ai["tags"]["value"] == "dataset, level"

    # The non-colliding per-column entry came through.
    assert ai["sku"]["value"] == "Unique stock keeping unit."

    # Bookkeeping still set, despite the column entries that would
    # have collided.
    assert ai["STATUS"] == "DONE"
    assert "generated_at" in ai


def test_process_reshape_label_only_marks_source(stage_cls, context_factory):
    """When a dictionary entry has ``label`` but no ``description``, the
    label is surfaced as the value AND ``source`` carries a marker so
    operators know they're seeing a label, not a description."""
    from ckanext.datapusher_plus.jobs.stages import ai_suggestions as ai_mod

    envelope = {
        "Dictionary": {
            "response": {
                "fields": [
                    {"name": "sku", "label": "SKU"},  # no description
                ],
            },
            "reasoning": "", "token_usage": {},
        },
    }
    fake_qsv = mock.Mock()
    fake_qsv.describegpt.return_value = _completed(json.dumps(envelope))

    captured = {}
    with mock.patch.object(ai_mod, "QSVCommand", return_value=fake_qsv), \
         mock.patch.object(
             ai_mod.dsu, "get_scheming_yaml",
             return_value=({}, {"id": "pkg-1"}),
         ), \
         mock.patch.object(
             ai_mod.dsu, "patch_package",
             side_effect=lambda pkg: captured.update(package=pkg),
         ):
        stage_cls().process(context_factory())

    ai = captured["package"]["dpp_suggestions"]["ai_suggestions"]
    assert ai["sku"]["value"] == "SKU"
    assert "(label only)" in ai["sku"]["source"]


def test_process_reshape_filters_tags_with_commas(stage_cls, context_factory):
    """Tags containing literal commas would be split by scheming's
    parser. Filter them out and keep the comma-free tags."""
    from ckanext.datapusher_plus.jobs.stages import ai_suggestions as ai_mod

    envelope = {
        "Tags": {
            "response": {
                "tags": ["clean_tag", "retail, b2b", "another_clean"],
            },
            "reasoning": "", "token_usage": {},
        },
    }
    fake_qsv = mock.Mock()
    fake_qsv.describegpt.return_value = _completed(json.dumps(envelope))

    captured = {}
    with mock.patch.object(ai_mod, "QSVCommand", return_value=fake_qsv), \
         mock.patch.object(
             ai_mod.dsu, "get_scheming_yaml",
             return_value=({}, {"id": "pkg-1"}),
         ), \
         mock.patch.object(
             ai_mod.dsu, "patch_package",
             side_effect=lambda pkg: captured.update(package=pkg),
         ):
        stage_cls().process(context_factory())

    ai = captured["package"]["dpp_suggestions"]["ai_suggestions"]
    # Comma-containing tag dropped, others survived.
    assert ai["tags"]["value"] == "clean_tag, another_clean"


def test_process_reshape_filters_all_tags_with_commas_drops_field(stage_cls, context_factory):
    """If ALL tags contain commas, the tags entry isn't written
    (vs. an empty-string entry that would render badly in the UI)."""
    from ckanext.datapusher_plus.jobs.stages import ai_suggestions as ai_mod

    envelope = {
        "Tags": {
            "response": {"tags": ["one, two", "three, four"]},
            "reasoning": "", "token_usage": {},
        },
    }
    fake_qsv = mock.Mock()
    fake_qsv.describegpt.return_value = _completed(json.dumps(envelope))

    captured = {}
    with mock.patch.object(ai_mod, "QSVCommand", return_value=fake_qsv), \
         mock.patch.object(
             ai_mod.dsu, "get_scheming_yaml",
             return_value=({}, {"id": "pkg-1"}),
         ), \
         mock.patch.object(
             ai_mod.dsu, "patch_package",
             side_effect=lambda pkg: captured.update(package=pkg),
         ):
        stage_cls().process(context_factory())

    ai = captured["package"]["dpp_suggestions"]["ai_suggestions"]
    assert "tags" not in ai


def test_describegpt_empty_api_key_passes_through(qsv_command):
    """``api_key=""`` is unusual but the docstring contract is
    ``None`` ⇒ omit, anything else ⇒ pass through. ``""`` should
    not be silently dropped."""
    with mock.patch("subprocess.run", return_value=_completed("{}")) as run:
        qsv_command.describegpt(input_file="/tmp/sample.csv", api_key="")

    args = run.call_args[0][0]
    assert "--api-key" in args
    assert args[args.index("--api-key") + 1] == ""


def test_process_reshape_skips_unknown_envelope_keys(stage_cls, context_factory):
    """Top-level keys other than Description/Tags/Dictionary are ignored
    (forward-compat: future qsv versions may add new sections)."""
    from ckanext.datapusher_plus.jobs.stages import ai_suggestions as ai_mod

    forward_compat = {
        "Description": {"response": "Hi.", "reasoning": "", "token_usage": {}},
        "FutureSection": {"response": "Surprise!"},  # qsv 21.x hypothetical
    }
    fake_qsv = mock.Mock()
    fake_qsv.describegpt.return_value = _completed(json.dumps(forward_compat))

    captured = {}

    def fake_patch(package):
        captured["package"] = package

    with mock.patch.object(ai_mod, "QSVCommand", return_value=fake_qsv), \
         mock.patch.object(
             ai_mod.dsu, "get_scheming_yaml",
             return_value=({}, {"id": "pkg-1"}),
         ), \
         mock.patch.object(ai_mod.dsu, "patch_package", side_effect=fake_patch):
        stage_cls().process(context_factory())

    ai = captured["package"]["dpp_suggestions"]["ai_suggestions"]
    # Description handled.
    assert ai["description"]["value"] == "Hi."
    # Unknown key ignored, not crashed on.
    assert "FutureSection" not in ai


# ---------------------------------------------------------------------------
# AISuggestionsStage.process — non-blocking failure paths
# ---------------------------------------------------------------------------


def test_process_swallows_qsv_subprocess_error(stage_cls, context_factory):
    """A raising ``describegpt`` must NOT propagate — log + return ctx."""
    from ckanext.datapusher_plus.jobs.stages import ai_suggestions as ai_mod
    from ckanext.datapusher_plus import utils

    fake_qsv = mock.Mock()
    fake_qsv.describegpt.side_effect = utils.JobError("qsv blew up")

    with mock.patch.object(ai_mod, "QSVCommand", return_value=fake_qsv), \
         mock.patch.object(ai_mod.dsu, "patch_package") as patch_pkg:
        ctx = context_factory()
        result = stage_cls().process(ctx)

    assert result is ctx
    ctx.logger.warning.assert_called()
    patch_pkg.assert_not_called()


def test_process_swallows_invalid_json(stage_cls, context_factory):
    """qsv returns non-JSON garbage → warn + skip persist, no raise."""
    from ckanext.datapusher_plus.jobs.stages import ai_suggestions as ai_mod

    fake_qsv = mock.Mock()
    fake_qsv.describegpt.return_value = _completed("not actually json")

    with mock.patch.object(ai_mod, "QSVCommand", return_value=fake_qsv), \
         mock.patch.object(ai_mod.dsu, "patch_package") as patch_pkg:
        ctx = context_factory()
        result = stage_cls().process(ctx)

    assert result is ctx
    ctx.logger.warning.assert_called()
    patch_pkg.assert_not_called()


def test_process_swallows_empty_stdout(stage_cls, context_factory):
    """qsv exits 0 but stdout is empty → warn + skip persist."""
    from ckanext.datapusher_plus.jobs.stages import ai_suggestions as ai_mod

    fake_qsv = mock.Mock()
    fake_qsv.describegpt.return_value = _completed("   \n  ")  # whitespace only

    with mock.patch.object(ai_mod, "QSVCommand", return_value=fake_qsv), \
         mock.patch.object(ai_mod.dsu, "patch_package") as patch_pkg:
        ctx = context_factory()
        stage_cls().process(ctx)

    ctx.logger.warning.assert_called()
    patch_pkg.assert_not_called()


def test_process_swallows_json_non_object(stage_cls, context_factory):
    """qsv returns a JSON list / string → warn + skip persist."""
    from ckanext.datapusher_plus.jobs.stages import ai_suggestions as ai_mod

    fake_qsv = mock.Mock()
    fake_qsv.describegpt.return_value = _completed('["not", "an", "object"]')

    with mock.patch.object(ai_mod, "QSVCommand", return_value=fake_qsv), \
         mock.patch.object(ai_mod.dsu, "patch_package") as patch_pkg:
        ctx = context_factory()
        stage_cls().process(ctx)

    ctx.logger.warning.assert_called()
    patch_pkg.assert_not_called()


def test_process_swallows_patch_package_error(stage_cls, context_factory):
    """``patch_package`` raising must NOT propagate — log + return ctx."""
    from ckanext.datapusher_plus.jobs.stages import ai_suggestions as ai_mod

    fake_qsv = mock.Mock()
    fake_qsv.describegpt.return_value = _completed('{"description": "x"}')

    with mock.patch.object(ai_mod, "QSVCommand", return_value=fake_qsv), \
         mock.patch.object(
             ai_mod.dsu, "get_scheming_yaml", return_value=({}, {"id": "pkg-1"})
         ), \
         mock.patch.object(
             ai_mod.dsu, "patch_package", side_effect=RuntimeError("CKAN 500")
         ):
        ctx = context_factory()
        result = stage_cls().process(ctx)

    assert result is ctx
    ctx.logger.warning.assert_called()


def test_process_skips_when_dpp_suggestions_is_non_dict(stage_cls, context_factory):
    """If ``package['dpp_suggestions']`` is not a dict, we skip the write
    rather than overwriting — preserving whatever the operator put there
    (legacy JSON string, custom plugin's scalar, …)."""
    from ckanext.datapusher_plus.jobs.stages import ai_suggestions as ai_mod

    fake_qsv = mock.Mock()
    fake_qsv.describegpt.return_value = _completed('{"description": "x"}')

    package = {"id": "pkg-1", "dpp_suggestions": "this should be a dict"}

    with mock.patch.object(ai_mod, "QSVCommand", return_value=fake_qsv), \
         mock.patch.object(
             ai_mod.dsu, "get_scheming_yaml", return_value=({}, package)
         ), \
         mock.patch.object(ai_mod.dsu, "patch_package") as patch_pkg:
        ctx = context_factory()
        stage_cls().process(ctx)

    # Non-destructive: original value preserved, no patch_package call.
    assert package["dpp_suggestions"] == "this should be a dict"
    patch_pkg.assert_not_called()
    ctx.logger.warning.assert_called()


def test_process_initializes_missing_dpp_suggestions(stage_cls, context_factory):
    """Happy path when dpp_suggestions is absent: dict is initialized
    and the AI sub-key gets written."""
    from ckanext.datapusher_plus.jobs.stages import ai_suggestions as ai_mod

    fake_qsv = mock.Mock()
    fake_qsv.describegpt.return_value = _completed('{"description": "x"}')

    package = {"id": "pkg-1"}  # no dpp_suggestions at all

    captured = {}

    def fake_patch(pkg):
        captured["package"] = pkg

    with mock.patch.object(ai_mod, "QSVCommand", return_value=fake_qsv), \
         mock.patch.object(
             ai_mod.dsu, "get_scheming_yaml", return_value=({}, package)
         ), \
         mock.patch.object(ai_mod.dsu, "patch_package", side_effect=fake_patch):
        stage_cls().process(context_factory())

    assert isinstance(captured["package"]["dpp_suggestions"], dict)
    assert "ai_suggestions" in captured["package"]["dpp_suggestions"]


# ---------------------------------------------------------------------------
# helpers.scheming_get_ai_suggestion_value /
# helpers.scheming_get_ai_suggestion_source /
# helpers.scheming_field_supports_ai_suggestion /
# helpers.scheming_has_ai_suggestion_fields
# ---------------------------------------------------------------------------


@pytest.fixture
def get_value():
    pytest.importorskip("ckan")
    from ckanext.datapusher_plus import helpers as dph

    return dph.scheming_get_ai_suggestion_value


@pytest.fixture
def get_source():
    pytest.importorskip("ckan")
    from ckanext.datapusher_plus import helpers as dph

    return dph.scheming_get_ai_suggestion_source


@pytest.fixture
def supports_ai():
    pytest.importorskip("ckan")
    from ckanext.datapusher_plus import helpers as dph

    return dph.scheming_field_supports_ai_suggestion


@pytest.fixture
def has_ai_fields():
    pytest.importorskip("ckan")
    from ckanext.datapusher_plus import helpers as dph

    return dph.scheming_has_ai_suggestion_fields


def _ai_data(entries):
    """Build a package-data dict with the given per-field ai_suggestions."""
    return {"dpp_suggestions": {"ai_suggestions": entries}}


# --- scheming_get_ai_suggestion_value ---


def test_value_returns_empty_when_data_missing(get_value):
    assert get_value("description", data=None) == ''
    assert get_value("description", data={}) == ''


def test_value_returns_empty_when_no_ai_suggestions(get_value):
    data = {"dpp_suggestions": {"package": {"foo": "bar"}}}
    assert get_value("description", data=data) == ''


def test_value_returns_empty_when_ai_suggestions_not_a_dict(get_value):
    assert get_value("description", data=_ai_data(["wat"])) == ''


def test_value_per_field_dict_entry(get_value):
    """The post-reshape on-disk shape: ai_suggestions[field] = {value, source}."""
    data = _ai_data({
        "description": {"value": "Widgets dataset", "source": "qsv describegpt"},
        "sku": {"value": "Stock keeping unit", "source": "qsv describegpt"},
    })
    assert get_value("description", data=data) == "Widgets dataset"
    assert get_value("sku", data=data) == "Stock keeping unit"
    # Unmatched field → empty.
    assert get_value("nonexistent", data=data) == ''


def test_value_returns_empty_when_entry_has_no_value(get_value):
    """Legacy / partial entries without a 'value' key surface as empty."""
    data = _ai_data({"description": {"source": "qsv describegpt"}})
    assert get_value("description", data=data) == ''


def test_value_decodes_dpp_suggestions_json_string(get_value):
    """CKAN sometimes re-serializes extras; the helper accepts a JSON string."""
    payload = '{"ai_suggestions": {"description": {"value": "x", "source": "qsv"}}}'
    assert get_value("description", data={"dpp_suggestions": payload}) == "x"


def test_value_scalar_entry_stringified(get_value):
    """A non-dict entry (legacy verbatim shape) falls back to str()."""
    assert get_value("description", data=_ai_data({"description": 42})) == "42"


def test_value_returns_empty_on_malformed_data(get_value):
    """Random garbage in the suggestion namespace must not raise."""
    assert get_value("description", data={"dpp_suggestions": "not-a-dict"}) == ''
    assert get_value("description", data={"dpp_suggestions": "{bad json"}) == ''


# --- scheming_get_ai_suggestion_source ---


def test_source_returns_label_when_present(get_source):
    data = _ai_data({"description": {"value": "x", "source": "qsv describegpt"}})
    assert get_source("description", data=data) == "qsv describegpt"


def test_source_returns_empty_when_data_missing(get_source):
    assert get_source("description", data=None) == ''
    assert get_source("description", data={}) == ''


def test_source_returns_empty_for_legacy_scalar_entry(get_source):
    """Scalar entries don't carry a source — return empty, don't raise."""
    assert get_source("description", data=_ai_data({"description": "x"})) == ''


# --- scheming_field_supports_ai_suggestion ---


def test_supports_ai_true_when_field_opts_in(supports_ai):
    assert supports_ai({"field_name": "description", "ai_suggestion": True}) is True


def test_supports_ai_false_by_default(supports_ai):
    assert supports_ai({"field_name": "description"}) is False
    assert supports_ai({"field_name": "description", "ai_suggestion": False}) is False


def test_supports_ai_handles_bad_input(supports_ai):
    """Non-dict input must not raise — opt-in convention only."""
    assert supports_ai(None) is False
    assert supports_ai("not a field") is False


# --- scheming_has_ai_suggestion_fields ---


def test_has_ai_fields_true_when_dataset_field_opts_in(has_ai_fields):
    schema = {
        "dataset_fields": [
            {"field_name": "title"},
            {"field_name": "description", "ai_suggestion": True},
        ]
    }
    assert has_ai_fields(schema) is True


def test_has_ai_fields_true_when_resource_field_opts_in(has_ai_fields):
    schema = {
        "dataset_fields": [{"field_name": "title"}],
        "resource_fields": [{"field_name": "name", "ai_suggestion": True}],
    }
    assert has_ai_fields(schema) is True


def test_has_ai_fields_false_when_no_field_opts_in(has_ai_fields):
    schema = {
        "dataset_fields": [{"field_name": "title"}],
        "resource_fields": [{"field_name": "name"}],
    }
    assert has_ai_fields(schema) is False


def test_has_ai_fields_handles_bad_input(has_ai_fields):
    assert has_ai_fields(None) is False
    assert has_ai_fields("not a schema") is False
    assert has_ai_fields({}) is False
