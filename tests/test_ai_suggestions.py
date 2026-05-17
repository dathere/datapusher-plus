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
    """Default invocation emits --description --dictionary --tags --json."""
    with mock.patch("subprocess.run", return_value=_completed("{}")) as run:
        qsv_command.describegpt(input_file="/tmp/sample.csv")

    args = run.call_args[0][0]
    # First arg is the qsv binary path; rest are the subcommand args.
    assert args[1] == "describegpt"
    assert "--description" in args
    assert "--dictionary" in args
    assert "--tags" in args
    assert "--json" in args
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
    assert "--json" not in args


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
    """Successful qsv call + valid JSON → patch_package fires with the
    suggestions stored under ``dpp_suggestions.ai_suggestions``."""
    from ckanext.datapusher_plus.jobs.stages import ai_suggestions as ai_mod

    qsv_payload = {
        "description": "A dataset of widgets.",
        "tags": ["widgets", "production"],
        "dictionary": [{"field": "sku", "summary": "Stock keeping unit"}],
    }

    fake_qsv = mock.Mock()
    fake_qsv.describegpt.return_value = _completed(json.dumps(qsv_payload))

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
    assert "dpp_suggestions" in captured["package"]
    ai = captured["package"]["dpp_suggestions"]["ai_suggestions"]
    assert ai["description"] == "A dataset of widgets."
    assert ai["tags"] == ["widgets", "production"]
    assert ai["dictionary"][0]["field"] == "sku"
    # Generation timestamp is auto-stamped.
    assert "generated_at" in ai


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


def test_process_resets_corrupted_dpp_suggestions(stage_cls, context_factory):
    """If ``package['dpp_suggestions']`` is not a dict, it's reset before write."""
    from ckanext.datapusher_plus.jobs.stages import ai_suggestions as ai_mod

    fake_qsv = mock.Mock()
    fake_qsv.describegpt.return_value = _completed('{"description": "x"}')

    package = {"id": "pkg-1", "dpp_suggestions": "this should be a dict"}

    captured = {}

    def fake_patch(pkg):
        captured["package"] = pkg

    with mock.patch.object(ai_mod, "QSVCommand", return_value=fake_qsv), \
         mock.patch.object(
             ai_mod.dsu, "get_scheming_yaml", return_value=({}, package)
         ), \
         mock.patch.object(ai_mod.dsu, "patch_package", side_effect=fake_patch):
        stage_cls().process(context_factory())

    # Reset to a fresh dict, with our ai_suggestions present.
    assert isinstance(captured["package"]["dpp_suggestions"], dict)
    assert "ai_suggestions" in captured["package"]["dpp_suggestions"]


# ---------------------------------------------------------------------------
# helpers.scheming_get_ai_suggestion
# ---------------------------------------------------------------------------


@pytest.fixture
def helper():
    pytest.importorskip("ckan")
    from ckanext.datapusher_plus import helpers as dph

    return dph.scheming_get_ai_suggestion


def test_helper_returns_empty_when_data_missing(helper):
    assert helper("description", data=None) == ''
    assert helper("description", data={}) == ''


def test_helper_returns_empty_when_no_ai_suggestions(helper):
    data = {"dpp_suggestions": {"package": {"foo": "bar"}}}
    assert helper("description", data=data) == ''


def test_helper_returns_empty_when_ai_suggestions_not_a_dict(helper):
    data = {"dpp_suggestions": {"ai_suggestions": ["wat"]}}
    assert helper("description", data=data) == ''


def test_helper_direct_top_level_lookup(helper):
    data = {
        "dpp_suggestions": {
            "ai_suggestions": {
                "description": "Widgets dataset",
                "tags": ["widgets"],
            }
        }
    }
    assert helper("description", data=data) == "Widgets dataset"
    assert helper("tags", data=data) == ["widgets"]


def test_helper_dictionary_field_match(helper):
    data = {
        "dpp_suggestions": {
            "ai_suggestions": {
                "dictionary": [
                    {"field": "sku", "summary": "Stock keeping unit"},
                    {"field": "price", "summary": "Unit price in USD"},
                ]
            }
        }
    }
    assert helper("sku", data=data) == "Stock keeping unit"
    assert helper("price", data=data) == "Unit price in USD"
    # Unmatched field → empty string.
    assert helper("nonexistent", data=data) == ''


def test_helper_dictionary_falls_back_to_description_key(helper):
    """Custom prompts sometimes emit 'description' instead of 'summary'."""
    data = {
        "dpp_suggestions": {
            "ai_suggestions": {
                "dictionary": [
                    {"field": "sku", "description": "Stock keeping unit"},
                ]
            }
        }
    }
    assert helper("sku", data=data) == "Stock keeping unit"


def test_helper_returns_empty_on_malformed_data(helper):
    """Random garbage in the suggestion namespace must not raise."""
    data = {"dpp_suggestions": "not-a-dict"}
    assert helper("description", data=data) == ''
