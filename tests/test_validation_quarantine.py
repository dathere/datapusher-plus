# -*- coding: utf-8 -*-
"""
Unit coverage for ValidationStage's Python-side quarantine pass.

Pins the fix where ``ValidationStage._validate_csv`` counts the *valid*
rows (not just the quarantined ones) and stores the count on
``context.rows_to_copy`` — so ``validate_task``'s quarantine-rate
denominator (``valid + quarantined``) is correct at validate time.
Before the fix, ``rows_to_copy`` was still 0 at validate time (only
``AnalysisStage``, which runs later, set it), making the quarantine
percentage 100% on any run with even one quarantined row.

``ProcessingContext`` is mocked, so this runs as a plain pytest unit
test with no CKAN, Postgres, or qsv binary.
"""

from __future__ import annotations

from unittest import mock

import pytest


def test_quarantine_pass_counts_valid_and_quarantined_rows(tmp_path):
    from ckanext.datapusher_plus import utils
    from ckanext.datapusher_plus.jobs.stages.validation import ValidationStage

    # 3 well-formed rows + 2 ragged rows (wrong field count).
    src = tmp_path / "data.csv"
    src.write_text(
        "a,b,c\n"
        "1,2,3\n"
        "4,5,6\n"
        "7,8\n"  # ragged: 2 fields
        "9,10,11\n"
        "12,13,14,15\n"  # ragged: 4 fields
    )

    ctx = mock.MagicMock()
    ctx.tmp = str(src)

    # qsv.validate raises on the original (forcing the Python quarantine
    # path) and passes on the cleaned subset.
    def _validate(path):
        if path == str(src):
            raise utils.JobError("strict RFC-4180 validation failed")

    ctx.qsv.validate.side_effect = _validate

    ValidationStage()._validate_csv(ctx)

    assert ctx.quarantined_rows == 2
    # The fix: valid rows are counted and exposed on the context so
    # validate_task can compute total_rows = valid + quarantined
    # correctly (a 2/5 = 40% rate here, not the pre-fix 2/2 = 100%).
    assert ctx.rows_to_copy == 3


def _ctx_for(src):
    """A MagicMock ProcessingContext whose qsv.validate raises on the
    original file (forcing the quarantine path) and passes otherwise."""
    ctx = mock.MagicMock()
    ctx.tmp = str(src)

    def _validate(path):
        if path == str(src):
            from ckanext.datapusher_plus import utils

            raise utils.JobError("strict RFC-4180 validation failed")

    ctx.qsv.validate.side_effect = _validate
    return ctx


def test_quarantine_replaces_undecodable_bytes_and_keeps_routing_ragged_rows(
    tmp_path,
):
    """``errors="replace"`` on the source open: a row with undecodable
    bytes is salvaged (bytes -> U+FFFD) and still counts as valid, while
    a genuinely ragged row is still quarantined."""
    from ckanext.datapusher_plus.jobs.stages.validation import ValidationStage

    src = tmp_path / "data.csv"
    # row 2: valid; row 3: invalid UTF-8 byte but correct field count;
    # row 4: ragged (2 fields).
    src.write_bytes(b"a,b,c\n1,2,3\n\xff,5,6\n7,8\n")

    ctx = _ctx_for(src)
    ValidationStage()._validate_csv(ctx)

    # The \xff row is salvaged (U+FFFD) and counts as valid; only the
    # ragged row is quarantined.
    assert ctx.quarantined_rows == 1
    assert ctx.rows_to_copy == 2
    # The cleaned subset carries the U+FFFD substitution, not the raw byte.
    valid_text = (tmp_path / "data.csv.valid.csv").read_text(encoding="utf-8")
    assert "�,5,6" in valid_text
    assert b"\xff" not in valid_text.encode("utf-8")


def test_quarantine_pass_with_no_ragged_rows_raises_encoding_error(tmp_path):
    """Encoding-only failure: every row has the right field count, so
    nothing is quarantined and the stage surfaces the explicit
    'needs manual repair' error rather than silently continuing."""
    from ckanext.datapusher_plus import utils
    from ckanext.datapusher_plus.jobs.stages.validation import ValidationStage

    src = tmp_path / "data.csv"
    # Bad UTF-8 byte, but every row still has 3 fields -> quarantined==0.
    src.write_bytes(b"a,b,c\n1,2,3\n\xff,5,6\n")

    ctx = _ctx_for(src)
    with pytest.raises(utils.JobError, match="encoding or quoting"):
        ValidationStage()._validate_csv(ctx)
