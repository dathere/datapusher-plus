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
