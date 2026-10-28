"""FormulaStage.should_skip must read ``ckan.plugins`` as the list CKAN
2.10+ returns, not the raw ini string (issue #354).

Before the fix the stage called ``.split()`` on the list, the resulting
``AttributeError`` was swallowed and reported as "skip", so formula
processing was silently disabled on every modern CKAN.
"""

from __future__ import annotations

from unittest import mock

import pytest


@pytest.fixture
def context():
    ctx = mock.MagicMock()
    ctx.logger = mock.MagicMock()
    return ctx


@pytest.fixture
def stage():
    pytest.importorskip("ckan")
    from ckanext.datapusher_plus.jobs.stages.formula import FormulaStage

    return FormulaStage()


@pytest.fixture
def set_plugins(monkeypatch):
    pytest.importorskip("ckan")
    import ckan.plugins.toolkit as tk

    def _set(value):
        monkeypatch.setitem(tk.config, "ckan.plugins", value)

    return _set


def test_scheming_in_plugins_list_is_not_skipped(stage, context, set_plugins):
    set_plugins(["datastore", "scheming_datasets", "datapusher_plus"])

    assert stage.should_skip(context) is False
    context.logger.warning.assert_not_called()


def test_scheming_in_plugins_string_is_not_skipped(stage, context, set_plugins):
    set_plugins("datastore scheming_datasets datapusher_plus")

    assert stage.should_skip(context) is False
    context.logger.warning.assert_not_called()


def test_plugins_list_without_scheming_is_skipped(stage, context, set_plugins):
    set_plugins(["datastore", "datapusher_plus"])

    assert stage.should_skip(context) is True
    context.logger.warning.assert_not_called()
