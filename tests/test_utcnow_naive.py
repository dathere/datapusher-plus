# -*- coding: utf-8 -*-
"""
Unit coverage for ``ckanext.datapusher_plus.utils.utcnow_naive``.

This helper is the single source of truth for "now" across DP+'s
persistent timestamp writes — job ``finished_timestamp``, log
``timestamp``, CKAN ``task_status.last_updated``. Pinning its contract
(UTC + naive) here is what protects against the silent regressions
that issue #145 catalogued:

* ``datetime.datetime.now()`` (no tz) returns *local* time; a worker
  running in non-UTC reports timestamps that look like UTC on read.
* ``datetime.datetime.now(tz=utc)`` returns a *tz-aware* value; DP+'s
  TIMESTAMP WITHOUT TIME ZONE columns reject it on insert.
* ``datetime.datetime.utcnow()`` is correct semantically but deprecated
  in Python 3.12+.

So the helper has exactly one job: return a naive datetime whose
wall-clock value is UTC. The tests below are written to fail loudly
if any of those properties slip.
"""

from __future__ import annotations

import datetime

import pytest


@pytest.fixture
def helper():
    pytest.importorskip("ckan")
    from ckanext.datapusher_plus.utils import utcnow_naive

    return utcnow_naive


def test_returns_a_datetime(helper):
    assert isinstance(helper(), datetime.datetime)


def test_returned_datetime_is_naive(helper):
    # ``tzinfo is None`` is the contract — DP+ persists into TIMESTAMP
    # WITHOUT TIME ZONE columns and SQLAlchemy/psycopg2 reject aware
    # datetimes on insert. A regression that "fixes" the helper by
    # leaving tzinfo attached would crash the COPY path at runtime.
    result = helper()
    assert result.tzinfo is None


def test_returned_datetime_is_utc_wallclock(helper):
    # The semantic contract: the wall-clock value MUST be UTC, not the
    # worker's local time. We can't compare to ``datetime.now()``
    # directly because that's exactly the bug — instead compare to
    # ``datetime.now(tz=utc).replace(tzinfo=None)`` which is also UTC
    # but constructed via a different path. They should agree within
    # a few seconds.
    expected = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    actual = helper()
    drift = abs((actual - expected).total_seconds())
    # Loose bound — these calls are microseconds apart in practice;
    # 5s is generous and catches any "actually returned local time"
    # regression on a non-UTC worker.
    assert drift < 5, (
        f"utcnow_naive drift too large: {drift:.3f}s. "
        f"actual={actual!r} expected={expected!r}. "
        "Are you accidentally returning local time?"
    )


def test_two_calls_in_different_tzs_agree(helper, monkeypatch):
    # Simulate the worker running in a non-UTC timezone — the helper
    # must still return UTC. We can't actually change the process's
    # tz mid-test on every platform, but we can verify the constructed
    # value matches the same construction via tz-aware UTC, which
    # bypasses ``time.localtime`` entirely.
    aware_utc = datetime.datetime.now(datetime.timezone.utc)
    naive_helper = helper()

    # Convert both to a POSIX timestamp for comparison — these
    # represent the same instant if and only if the helper is UTC.
    expected_ts = aware_utc.timestamp()
    actual_ts = naive_helper.replace(tzinfo=datetime.timezone.utc).timestamp()
    assert abs(actual_ts - expected_ts) < 5
