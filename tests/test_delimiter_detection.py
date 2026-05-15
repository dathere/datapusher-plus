# -*- coding: utf-8 -*-
"""
Unit-level coverage for non-comma delimiter detection.

Commit efb254f made ``FormatConverterStage`` sniff a file's delimiter
(via ``qsv sniff --json``) and forward a detected ``;`` / tab / ``|`` to
``qsv input --delimiter`` so semicolon-, tab-, and pipe-delimited CSVs
normalize correctly instead of being mis-parsed as a single wide column.

These tests pin two contracts the feature depends on, without needing a
qsv binary, Prefect, CKAN, or Postgres:

* ``QSVCommand.sniff`` degrades gracefully to ``None`` (the value the
  caller treats as "could not determine") on empty / non-JSON / non-
  ``CompletedProcess`` output, and returns the parsed dict otherwise.
* ``FormatConverterStage._normalize_csv`` forwards a detected
  ``;``/``\\t``/``|`` delimiter to ``QSVCommand.input`` and leaves it
  ``None`` for comma, unrecognised, or unsniffable input.

Stage/qsv internals are mocked, so these run as plain pytest unit tests.
"""

from __future__ import annotations

import subprocess
from unittest import mock

import pytest


# ---------------------------------------------------------------------------
# QSVCommand.sniff
# ---------------------------------------------------------------------------


def _make_qsv():
    """Build a QSVCommand without running its binary-probing __init__.

    ``QSVCommand.__init__`` checks the qsv binary exists and runs a
    version check; ``sniff`` itself only calls ``self._run_command``
    (mocked here). ``logger`` / ``qsv_bin`` are set defensively for the
    ``__init__``-bypassed instance, not because ``sniff`` reads them.
    """
    import logging

    from ckanext.datapusher_plus.qsv_utils import QSVCommand

    qsv = object.__new__(QSVCommand)
    qsv.logger = logging.getLogger("test-qsv")
    qsv.qsv_bin = "/nonexistent/qsv"
    return qsv


def _completed(stdout, returncode=0):
    return subprocess.CompletedProcess(
        args=["qsv", "sniff"], returncode=returncode, stdout=stdout, stderr=""
    )


def test_sniff_returns_parsed_dict_on_valid_json():
    qsv = _make_qsv()
    payload = '{"delimiter_char": ";", "header_row": true, "num_fields": 3}'
    with mock.patch.object(qsv, "_run_command", return_value=_completed(payload)):
        result = qsv.sniff("/tmp/data.csv")
    assert result == {
        "delimiter_char": ";",
        "header_row": True,
        "num_fields": 3,
    }


def test_sniff_invokes_qsv_sniff_with_check_false():
    """The graceful-degradation contract relies on check=False so a
    non-zero qsv exit returns a CompletedProcess instead of raising."""
    qsv = _make_qsv()
    with mock.patch.object(
        qsv, "_run_command", return_value=_completed("{}")
    ) as run:
        qsv.sniff("/tmp/data.csv")
    run.assert_called_once_with(
        ["sniff", "--json", "/tmp/data.csv"], check=False
    )


def test_sniff_returns_none_on_empty_stdout():
    qsv = _make_qsv()
    with mock.patch.object(
        qsv, "_run_command", return_value=_completed("", returncode=1)
    ):
        assert qsv.sniff("/tmp/data.csv") is None


def test_sniff_returns_none_on_invalid_json():
    qsv = _make_qsv()
    with mock.patch.object(
        qsv, "_run_command", return_value=_completed("not json at all")
    ):
        assert qsv.sniff("/tmp/data.csv") is None


def test_sniff_returns_none_when_run_command_returns_non_process():
    """``_run_command`` can return a bare stderr string on some failure
    paths; ``getattr(result, "stdout", None)`` must yield None there."""
    qsv = _make_qsv()
    with mock.patch.object(
        qsv, "_run_command", return_value="qsv: some error on stderr"
    ):
        assert qsv.sniff("/tmp/data.csv") is None


# ---------------------------------------------------------------------------
# FormatConverterStage._normalize_csv delimiter wiring
# ---------------------------------------------------------------------------


def _make_context(sniff_return):
    """A ProcessingContext stand-in for _normalize_csv.

    Only the attributes _normalize_csv touches are populated; ``qsv`` is
    a MagicMock so ``input`` calls are recorded and ``sniff`` is
    scripted per-test.
    """
    ctx = mock.MagicMock()
    ctx.tmp = "/tmp/source.csv"
    ctx.temp_dir = "/tmp"
    ctx.qsv.sniff.return_value = sniff_return
    return ctx


def _run_normalize(sniff_return):
    from ckanext.datapusher_plus.jobs.stages.format_converter import (
        FormatConverterStage,
    )

    stage = FormatConverterStage()
    ctx = _make_context(sniff_return)
    # Skip real encoding detection so source_file == ctx.tmp and no
    # re-encode subprocess is spawned.
    with mock.patch.object(stage, "_detect_encoding", return_value="UTF-8"):
        stage._normalize_csv(ctx, "CSV")
    return ctx


@pytest.mark.parametrize("delim", [";", "\t", "|"])
def test_normalize_csv_forwards_detected_delimiter(delim):
    ctx = _run_normalize({"delimiter_char": delim})
    ctx.qsv.sniff.assert_called_once_with("/tmp/source.csv")
    _, kwargs = ctx.qsv.input.call_args
    assert kwargs["delimiter"] == delim


@pytest.mark.parametrize(
    "sniff_return",
    [
        {"delimiter_char": ","},  # comma: no override, prior behavior
        {"delimiter_char": ":"},  # uncommon delimiter, not in allowlist
        {"delimiter_char": ""},  # qsv reported nothing usable
        {"header_row": True},  # JSON without a delimiter_char key
        None,  # sniff failed (empty/invalid output)
    ],
)
def test_normalize_csv_leaves_delimiter_none(sniff_return):
    ctx = _run_normalize(sniff_return)
    _, kwargs = ctx.qsv.input.call_args
    assert kwargs["delimiter"] is None
