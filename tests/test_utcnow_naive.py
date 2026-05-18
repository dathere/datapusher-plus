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


def test_helper_returns_utc_even_when_process_tz_is_not_utc(
    helper, monkeypatch
):
    # The point of this test: on a worker process running in a non-UTC
    # local tz, the helper MUST still return UTC. The earlier shape of
    # this test compared two values both sampled in the process's
    # current tz — a regression to ``datetime.datetime.now()`` would
    # pass on a UTC CI host (which our CI is) because local tz IS UTC
    # there. Actually flipping ``TZ`` + ``time.tzset()`` to a known
    # non-UTC zone is how you exercise the contract; the helper has
    # to return UTC regardless of what ``time.localtime`` reports.
    #
    # ``time.tzset`` is POSIX-only — Windows doesn't expose it. Skip
    # there rather than silently weaken the assertion.
    import time

    if not hasattr(time, "tzset"):
        pytest.skip("time.tzset not available on this platform")

    # Pacific/Auckland is +12 (or +13 during DST) — picked so the local
    # wall-clock differs from UTC by enough hours that a regression
    # would be obvious in the failure diff.
    monkeypatch.setenv("TZ", "Pacific/Auckland")
    time.tzset()

    # Now ``datetime.now()`` (no tz) and ``datetime.now(tz=local_tz)``
    # both report Auckland time — but ``helper()`` must report UTC.
    local_now = datetime.datetime.now()
    helper_now = helper()

    # If helper is correctly UTC, the wall-clock hour difference vs.
    # local Auckland time is roughly 12-13 hours. A regression to
    # ``datetime.datetime.now()`` (local time) would make these agree
    # to within a few seconds.
    delta = abs((helper_now - local_now).total_seconds())
    assert delta > 3600, (  # at least 1 hour apart
        f"utcnow_naive returned local time, not UTC. "
        f"helper={helper_now!r} local_now={local_now!r} delta={delta:.0f}s. "
        "(With TZ=Pacific/Auckland, UTC should be ~12-13h behind local.)"
    )

    # And the instant the helper returned, interpreted as UTC, should
    # match ``datetime.now(tz=utc)`` — the canonical UTC source.
    aware_utc = datetime.datetime.now(datetime.timezone.utc)
    naive_as_utc = helper_now.replace(tzinfo=datetime.timezone.utc)
    instant_drift = abs((naive_as_utc - aware_utc).total_seconds())
    assert instant_drift < 5, (
        f"utcnow_naive's instant disagrees with datetime.now(tz=utc): "
        f"naive_as_utc={naive_as_utc!r} aware_utc={aware_utc!r} "
        f"drift={instant_drift:.3f}s"
    )
