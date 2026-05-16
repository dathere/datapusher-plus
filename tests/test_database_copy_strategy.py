# -*- coding: utf-8 -*-
"""
Unit coverage for ``DatabaseStage._copy_data``'s two strategies,
controlled by ``ckanext.datapusher_plus.use_truncate_freeze``.

Asserts the SQL emitted by each path so a regression that, say,
drops the ``FREEZE 1`` option from the fast path or accidentally
keeps it on the read-friendly path would fail loudly.

Mocks ``psycopg2.connect`` and the CSV file open so the test runs
in any environment that has ``psycopg2`` importable (the
``dpp-test`` ckan-dev container; CI). No real DB connection is made.
"""

from __future__ import annotations

from unittest import mock

import pytest


@pytest.fixture
def stage():
    """Import the database stage lazily — its top-level imports need
    psycopg2 and CKAN-dependent helpers."""
    pytest.importorskip("psycopg2")
    pytest.importorskip("ckan")
    from ckanext.datapusher_plus.jobs.stages.database import DatabaseStage

    return DatabaseStage()


def _make_context(tmp_path):
    """Minimal mock context with the fields ``_copy_data`` reads."""
    csv = tmp_path / "x.csv"
    csv.write_text("a,b\n1,2\n")
    ctx = mock.MagicMock()
    ctx.resource_id = "res-abc"
    ctx.headers_dicts = [{"id": "a"}, {"id": "b"}]
    ctx.tmp = str(csv)
    return ctx


def _extract_sql_text(composable):
    """Walk a ``psycopg2.sql.Composable`` and return the concatenated
    literal SQL — without calling ``as_string()`` (which needs a real
    connection / cursor for the C-level adapter, defeating mock-based
    unit testing).

    ``sql.SQL`` exposes its template via ``.string``; ``sql.Identifier``
    its name parts via ``.strings``; ``sql.Composed`` its child nodes
    via ``.seq``. That's enough to reconstruct the SQL fragments we
    care about asserting against (``FREEZE 1`` presence / absence).
    """
    from psycopg2 import sql

    if isinstance(composable, sql.SQL):
        return composable.string
    if isinstance(composable, sql.Identifier):
        return ".".join(composable.strings)
    if isinstance(composable, sql.Composed):
        return "".join(_extract_sql_text(c) for c in composable.seq)
    return str(composable)


def _captured_copy_sql(cur_mock):
    """Return the COPY SQL literal that was handed to ``copy_expert``."""
    call = cur_mock.copy_expert.call_args
    return _extract_sql_text(call.args[0])


def test_copy_data_with_freeze_uses_freeze_option_and_single_commit(
    stage, tmp_path, monkeypatch
):
    """Default path: TRUNCATE + COPY ... WITH FREEZE in one txn.
    The SQL contains ``FREEZE 1``; only the post-COPY commit fires
    (the TRUNCATE rides along in the same transaction)."""
    from ckanext.datapusher_plus.jobs.stages import database as db_mod

    monkeypatch.setattr(db_mod.conf, "USE_TRUNCATE_FREEZE", True)

    conn, cur = mock.MagicMock(), mock.MagicMock()
    conn.cursor.return_value = cur
    monkeypatch.setattr(db_mod.psycopg2, "connect", lambda *_a, **_k: conn)
    monkeypatch.setattr(stage, "_vacuum_analyze", mock.MagicMock())

    ctx = _make_context(tmp_path)
    stage._copy_data(ctx)

    sql_text = _captured_copy_sql(cur)
    assert "FREEZE 1" in sql_text
    # Only the closing commit fires; TRUNCATE rides the same txn.
    assert conn.commit.call_count == 1


def test_copy_data_without_freeze_omits_freeze_and_double_commits(
    stage, tmp_path, monkeypatch
):
    """Opt-out path: ``FREEZE 1`` must be absent from the COPY SQL
    so PostgreSQL doesn't reject it (only valid inside the same txn
    as the table-emptying statement). Two commits fire: one after
    TRUNCATE to release the AccessExclusive lock, one after COPY."""
    from ckanext.datapusher_plus.jobs.stages import database as db_mod

    monkeypatch.setattr(db_mod.conf, "USE_TRUNCATE_FREEZE", False)

    conn, cur = mock.MagicMock(), mock.MagicMock()
    conn.cursor.return_value = cur
    monkeypatch.setattr(db_mod.psycopg2, "connect", lambda *_a, **_k: conn)
    monkeypatch.setattr(stage, "_vacuum_analyze", mock.MagicMock())

    ctx = _make_context(tmp_path)
    stage._copy_data(ctx)

    sql_text = _captured_copy_sql(cur)
    assert "FREEZE 1" not in sql_text
    assert "FORMAT CSV" in sql_text
    assert "HEADER 1" in sql_text
    # Two commits: one after TRUNCATE (releases AccessExclusive lock
    # before the long COPY), one after COPY succeeds.
    assert conn.commit.call_count == 2


def test_truncate_table_rollback_on_undefined_table(stage):
    """``_truncate_table`` swallows the expected ``UndefinedTable``
    error (table doesn't exist yet on a brand-new resource) and rolls
    back so the caller's subsequent ``commit()`` doesn't hit
    ``InFailedSqlTransaction`` in the opt-out path."""
    import psycopg2

    cur = mock.MagicMock()
    cur.execute.side_effect = psycopg2.errors.UndefinedTable(
        "relation \"missing-resource-id\" does not exist"
    )
    conn = mock.MagicMock()

    # Must not raise (the swallowed-error contract for this specific
    # error class).
    stage._truncate_table(cur, conn, "missing-resource-id")

    # Rollback called once to clear the aborted transaction.
    conn.rollback.assert_called_once()


def test_truncate_table_reraises_other_errors_as_jobsterror(stage):
    """Any ``psycopg2.Error`` other than ``UndefinedTable`` must
    propagate as a ``JobError`` so the job fails loudly rather than
    COPYing into a non-truncated table holding rows from a prior run.
    Connection is still rolled back to leave the txn state clean."""
    import psycopg2

    from ckanext.datapusher_plus import utils

    cur = mock.MagicMock()
    cur.execute.side_effect = psycopg2.errors.InsufficientPrivilege(
        "permission denied for table some-resource-id"
    )
    conn = mock.MagicMock()

    with pytest.raises(utils.JobError, match="TRUNCATE TABLE"):
        stage._truncate_table(cur, conn, "some-resource-id")

    # Rollback fires before the raise so the txn isn't left aborted.
    conn.rollback.assert_called_once()
