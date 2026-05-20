# -*- coding: utf-8 -*-
"""
Coverage for the ``FORMATS`` config — the gate that decides which
uploaded file formats DataPusher+ will process.

``format_converter.py`` can convert ``.xlsm`` and ``.xlsb`` workbooks
(both are in ``FormatConverterStage.SPREADSHEET_EXTENSIONS``), but the
``FORMATS`` default in ``config.py`` historically omitted them — so a
``.xlsm`` / ``.xlsb`` resource was silently skipped before it ever
reached the converter. These tests pin:

1. ``xlsm`` and ``xlsb`` are in the ``FORMATS`` default — the direct
   regression guard for that fix.
2. Every spreadsheet extension the converter advertises support for
   (``SPREADSHEET_EXTENSIONS``) is present in ``FORMATS`` — a
   consistency guard so a future "the converter now also handles
   ``.xyz``" change can't silently leave ``.xyz`` ungated.
"""

from __future__ import annotations

import pytest


@pytest.fixture
def formats():
    pytest.importorskip("ckan")
    import ckanext.datapusher_plus.config as conf

    return conf.FORMATS


def test_xlsm_and_xlsb_are_in_formats(formats):
    # format_converter can convert both; FORMATS must let them in.
    assert "xlsm" in formats
    assert "xlsb" in formats


def test_formats_covers_every_converter_spreadsheet_extension(formats):
    # Consistency guard: anything FormatConverterStage advertises in
    # SPREADSHEET_EXTENSIONS must be gated in by FORMATS, or DP+ would
    # reject a file format it actually knows how to convert.
    pytest.importorskip("ckan")
    from ckanext.datapusher_plus.jobs.stages.format_converter import (
        FormatConverterStage,
    )

    formats_lower = {f.lower() for f in formats}
    for ext in FormatConverterStage.SPREADSHEET_EXTENSIONS:
        assert ext.lower() in formats_lower, (
            f"{ext} is in FormatConverterStage.SPREADSHEET_EXTENSIONS but "
            f"not in config.FORMATS — DP+ would skip .{ext.lower()} files "
            "before the converter ever sees them."
        )
