# -*- coding: utf-8 -*-
"""
Regression coverage for issue #173 ("Date records are not loading in
correct format").

The reporter (then on DP+ v1.0.3) uploaded ``date_test.csv`` — a CSV
that *advertises* nine different date-format columns but is actually
malformed: the RFC 2822 column carries unquoted commas inside its
value (e.g. ``Fri, 11 Oct 2024 14:30:00 +0000``). The header has 10
fields; every data row has 11. v1.0.3's pipeline ingested it anyway
with the columns silently shifted by one, which surfaced as "dates
in the wrong format" in the resulting datastore preview.

Three things have changed since then that bear on this issue:

1. **v3.0's ValidationStage quarantines mismatched-field-count rows.**
   This exact malformed CSV is now caught with a clear error rather
   than silently shifted (see ``tests/test_validation_quarantine.py``).

2. **PR #314 (issue #179) mapped qsv ``Date`` -> Postgres ``date``.**
   Date-only columns no longer pick up a phantom ``T00:00:00`` time
   component (see ``tests/test_date_without_timestamp.py``).

3. **qsv's date-format inference has gaps** that are not DP+ bugs but
   that this test surfaces for future reference — particularly
   dash-separated DD-MM-YYYY and bare Unix epoch integers.

The test below uses the actual reporter-provided fixture (preserved
under ``tests/static/issue_173_date_test_malformed.csv``) plus a
hand-quoted variant (``issue_173_date_test_quoted.csv``) to lock in
both halves of the story end-to-end at the qsv layer:

* The malformed variant must fail RFC 4180 validation with a
  ``field count`` diagnostic — the precondition for DP+'s quarantine
  pass to engage downstream.
* The quoted variant, fed through ``qsv stats --infer-dates``, must
  produce the exact inference matrix below. A future qsv upgrade
  changing any cell of that matrix should fail loudly here so the
  maintainer can update the comment on issue #173 (or surface a real
  regression) rather than discover it through a user report.
"""

from __future__ import annotations

import csv
import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Dict, Optional, Tuple

import pytest


REPO_ROOT = Path(__file__).resolve().parent.parent
FIXTURES_DIR = Path(__file__).parent / "static"
MALFORMED_FIXTURE = FIXTURES_DIR / "issue_173_date_test_malformed.csv"
QUOTED_FIXTURE = FIXTURES_DIR / "issue_173_date_test_quoted.csv"


def _locate_qsv() -> Optional[str]:
    """Find a qsv binary to test against.

    Mirrors ``tests/test_qsv_v20_regression.py:_locate_qsv`` so the
    skip-when-missing behavior is identical across the qsv-touching
    test suite.
    """
    for c in (os.environ.get("QSV_BIN"), shutil.which("qsvdp"), shutil.which("qsv")):
        if c and Path(c).is_file():
            return c
    return None


QSV_BIN = _locate_qsv()


def _qsv_version(binary: str) -> Optional[Tuple[int, int, int]]:
    """Parse ``qsv --version`` into (major, minor, patch)."""
    try:
        out = subprocess.run(
            [binary, "--version"],
            capture_output=True, text=True, check=True, timeout=5,
        ).stdout
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        return None
    m = re.search(r"(\d+)\.(\d+)\.(\d+)", out)
    return (int(m.group(1)), int(m.group(2)), int(m.group(3))) if m else None


# Issue #173 was opened against qsv ~v0.x; the inference table below
# was captured against qsv 20.x. Don't run on older qsv — the
# inferred-type column set drifted across versions, and a stale
# baseline would either false-pass or false-fail.
QSV_VERSION = _qsv_version(QSV_BIN) if QSV_BIN else None
requires_qsv_20 = pytest.mark.skipif(
    QSV_BIN is None or QSV_VERSION is None or QSV_VERSION < (20, 0, 0),
    reason=(
        f"qsv >= 20.0.0 not available "
        f"(QSV_BIN={QSV_BIN}, version={QSV_VERSION})."
    ),
)


def _qsv(*args: str) -> subprocess.CompletedProcess:
    """Invoke qsv; ``check=False`` because some tests assert on failures."""
    assert QSV_BIN is not None
    return subprocess.run(
        [QSV_BIN, *args],
        capture_output=True, text=True, check=False, timeout=30,
    )


# ---------------------------------------------------------------------------
# Malformed CSV (reporter's actual file) - quarantine precondition
# ---------------------------------------------------------------------------


@requires_qsv_20
def test_malformed_csv_fails_rfc4180_validation_with_field_count_error():
    """The reporter's CSV must fail ``qsv validate`` with the exact
    error string DP+'s ValidationStage catches.

    ``ValidationStage._validate_csv`` is a two-phase guard:

      phase 1: ``qsv validate`` (strict RFC 4180); on failure ->
      phase 2: Python-side row-by-row quarantine that routes rows
               whose len() != header into a sibling ``.invalid.csv``.

    The phase-2 quarantine only kicks in when phase 1 emits exactly
    this class of failure. If qsv's error message ever stops mentioning
    "fields" / "record", the quarantine pass would still trigger
    correctly (it doesn't introspect the qsv error text) — but a
    different failure shape from qsv would mean we're shipping users
    a different diagnostic, worth knowing about. Pinned loosely.
    """
    result = _qsv("validate", str(MALFORMED_FIXTURE))

    # qsv writes the diagnostic to stderr OR stdout depending on
    # build flags / version; concatenate to be tolerant.
    combined = (result.stdout or "") + (result.stderr or "")
    assert "fields" in combined.lower() or "record" in combined.lower(), (
        "qsv validate produced an unexpected error shape on the issue #173 "
        f"malformed fixture; saw:\n{combined}"
    )


def test_malformed_csv_field_count_matches_reporters_original():
    """Document the precondition: header has 10 fields, every data row
    has 11. If a future cleanup accidentally re-quotes the RFC 2822
    column in the fixture, the malformed test above would start
    passing for the wrong reason. This test pins the field count
    drift directly via the stdlib CSV reader (no qsv dependency).
    """
    with MALFORMED_FIXTURE.open(encoding="utf-8", newline="") as fh:
        reader = csv.reader(fh)
        rows = list(reader)
    assert len(rows[0]) == 10, "Header should have 10 fields"
    for i, row in enumerate(rows[1:], start=2):
        assert len(row) == 11, (
            f"Data row {i} should have 11 fields (header has 10) — the "
            "fixture has been accidentally repaired; the malformed-CSV "
            "regression test for issue #173 needs revisiting."
        )


# ---------------------------------------------------------------------------
# Well-formed (quoted) CSV — qsv inference coverage matrix
# ---------------------------------------------------------------------------


def _qsv_stats_types(csv_path: Path) -> Dict[str, str]:
    """Run ``qsv stats --infer-dates --dates-whitelist all`` and return
    a {field: type} map.

    qsv stats emits CSV (no --json flag), so we parse the rows back
    with the stdlib reader and pull the ``field`` / ``type`` columns.
    """
    result = _qsv(
        "stats", str(csv_path),
        "--infer-dates", "--dates-whitelist", "all",
    )
    assert result.returncode == 0, (
        f"qsv stats failed on {csv_path}\nstderr:{result.stderr}"
    )
    reader = csv.DictReader(result.stdout.splitlines())
    return {row["field"]: row["type"] for row in reader}


# Pinning the inference table here so a future qsv upgrade has to
# explicitly update the test (and the issue #173 comment) rather than
# silently change user-facing behavior. The "gap" rows are NOT bugs in
# DP+ — they're qsv inference limitations, called out so the next
# maintainer who looks at #173 doesn't have to rediscover them.
# Baseline captured against qsv 20.0.0 — the version pinned in
# ``Dockerfile.worker`` (production worker), CI, and the ``dpp-test``
# container. Newer qsv versions narrow some of the gaps (e.g. qsv 20.1+
# starts recognizing the ISO 8601 column as DateTime) — when the
# pinned version moves, update this matrix and the comment on #173.
EXPECTED_INFERENCE = {
    "ID":                                 "Integer",
    "Event Name":                         "String",
    # gap: ISO 8601 (YYYY-MM-DDTHH:MM:SS) values like
    # ``2024-10-11T14:30:00`` (no tz, T-separator) are NOT inferred by
    # qsv 20.0.0 — they fall through to String. Recognized in qsv ≥ 20.1.
    "ISO 8601 (YYYY-MM-DDTHH:MM:SS)":     "String",
    "RFC 2822":                           "DateTime",   # impressively
    "Unix Timestamp":                     "Integer",    # gap: epoch ints not detected
    "MM/DD/YYYY":                         "Date",
    "DD-MM-YYYY":                         "String",     # gap: dash-separated DMY not inferred
    "YYYY/MM/DD":                         "Date",
    "DD/MM/YYYY HH:MM":                   "DateTime",   # MDY by default; flips with --prefer-dmy
    "YYYY-MM-DD HH:MM:SS":                "DateTime",
}


@requires_qsv_20
def test_quoted_csv_inference_matrix():
    """Pin the qsv-inferred type for each date-format column in the
    reporter's CSV (well-formed variant).

    Any cell of ``EXPECTED_INFERENCE`` that drifts on a qsv upgrade
    indicates either:

      a) a qsv fix (DD-MM-YYYY now recognized — celebrate, update the
         comment on issue #173 to remove the gap), or
      b) a qsv regression (a previously-working format stops being
         recognized — file a qsv bug and pin the workaround here).

    Either way the test gives the next maintainer a clean diff against
    a known baseline.
    """
    types = _qsv_stats_types(QUOTED_FIXTURE)

    diffs = []
    for field, expected in EXPECTED_INFERENCE.items():
        actual = types.get(field, "<missing>")
        if actual != expected:
            diffs.append(f"  {field!r}: expected {expected!r}, got {actual!r}")

    assert not diffs, (
        "qsv inference matrix drifted from the issue #173 baseline:\n"
        + "\n".join(diffs)
        + "\n\nIf this is intentional (qsv upgrade improved inference), "
          "update EXPECTED_INFERENCE and the comment on issue #173."
    )


@requires_qsv_20
def test_quoted_csv_prefer_dmy_flips_dmy_columns():
    """Document the ``--prefer-dmy`` knob's effect on the same fixture.

    qsv parses ambiguous ``11/10/2024``-style values as MDY by default
    (so DD/MM/YYYY columns load with the day-and-month swapped).
    Operators handling European data set DP+'s
    ``ckanext.datapusher_plus.prefer_dmy = True`` config, which makes
    DP+ pass ``--prefer-dmy`` to qsv (see
    ``ckanext/datapusher_plus/jobs/stages/analysis.py``). This test
    confirms the effect is present in the qsv version we ship.

    Practical caveat for the reporter on issue #173: a single CSV that
    mixes MDY *and* DMY columns can't be parsed correctly under
    ``prefer_dmy`` either way — it's a per-dataset choice, not a
    per-column choice. That ambiguity is fundamental, not a DP+ bug.
    """
    result = _qsv(
        "stats", str(QUOTED_FIXTURE),
        "--infer-dates", "--dates-whitelist", "all",
        "--prefer-dmy",
    )
    assert result.returncode == 0, (
        f"qsv stats --prefer-dmy failed\nstderr:{result.stderr}"
    )
    by_field = {
        r["field"]: r
        for r in csv.DictReader(result.stdout.splitlines())
    }

    # With --prefer-dmy, ``11/10/2024`` is read as 11 Oct (DMY) — so the
    # DD/MM/YYYY HH:MM column's max is the 2024-10-11 value (row 1).
    # Without --prefer-dmy, qsv would read it as 11 Nov (MDY), giving
    # a max of 2024-11-10 — verified empirically while writing this test.
    dmy_max = by_field["DD/MM/YYYY HH:MM"]["max"]
    assert "2024-10-11" in dmy_max, (
        f"--prefer-dmy didn't flip DD/MM/YYYY HH:MM parse; max={dmy_max!r}. "
        "Either qsv's --prefer-dmy behavior changed or DP+'s wiring "
        "(analysis.py PREFER_DMY) is broken."
    )
