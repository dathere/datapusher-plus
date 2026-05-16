# -*- coding: utf-8 -*-
"""
Unit coverage for ``prefect_client`` helpers.

Currently pins the ``ensure_work_pool`` running-loop guard: the helper
is synchronous (it is called from the sync ``prefect-deploy`` CLI) and
must fail with a clear ``RuntimeError`` rather than the opaque
``asyncio.run() cannot be called from a running event loop`` if it is
ever invoked from inside a coroutine.
"""

from __future__ import annotations

import asyncio

import pytest


def test_ensure_work_pool_rejects_running_event_loop():
    # The guard only runs once the Prefect client imports succeed; skip
    # cleanly if Prefect is not installed in this environment.
    pytest.importorskip("prefect")

    from ckanext.datapusher_plus import prefect_client

    async def _call_from_loop():
        # Invoked inside a running loop -> ensure_work_pool must raise a
        # clear RuntimeError instead of letting asyncio.run() blow up.
        prefect_client.ensure_work_pool("dpp-test-pool")

    with pytest.raises(RuntimeError, match="running event loop"):
        asyncio.run(_call_from_loop())
