"""``DownloadStage`` must not overwrite ``context.timer_start`` (issue #355).

``pipeline_core`` stamps ``timer_start`` with ``time.time()`` and the JOB DONE
line (and the Prefect ``duration_seconds``) subtract it from ``time.time()``.
The download stage reused the field for its own ``perf_counter`` fetch timer,
so every job reported an elapsed time of roughly the Unix epoch.
"""

from __future__ import annotations

import time
from unittest import mock

import pytest


@pytest.fixture
def stage():
    pytest.importorskip("ckan")
    from ckanext.datapusher_plus.jobs.stages.download import DownloadStage

    stage = DownloadStage()
    stage._validate_url_scheme = mock.MagicMock()
    stage._download_file = mock.MagicMock(return_value=("abc", 154, "csv", {}))
    stage._should_skip_upload = mock.MagicMock(return_value=False)
    stage._handle_zip_file = mock.MagicMock()
    return stage


@pytest.fixture
def context():
    ctx = mock.MagicMock()
    ctx.resource = {}
    ctx.timer_start = time.time()
    return ctx


def test_job_timer_survives_the_download_stage(stage, context):
    job_start = context.timer_start

    assert stage.process(context) is context
    assert context.timer_start == job_start


def test_total_elapsed_is_seconds_not_epoch(stage, context):
    stage.process(context)

    total_elapsed = time.time() - context.timer_start
    assert 0 <= total_elapsed < 60
