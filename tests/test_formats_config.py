# -*- coding: utf-8 -*-
"""
Coverage for two config.py correctness fixes (FORMATS gate + the
spatial-tolerance config key).

``format_converter.py`` can convert ``.xlsm`` and ``.xlsb`` workbooks
(both are in ``FormatConverterStage.SPREADSHEET_EXTENSIONS``), but the
``FORMATS`` default in ``config.py`` historically omitted them — so a
``.xlsm`` / ``.xlsb`` resource was silently skipped before it ever
reached the converter.

Separately, ``config.py`` read the spatial-simplification relative
tolerance from an UPPERCASE ckan.ini key, inconsistent with every
other DP+ setting.

These tests pin:

1. ``xlsm`` and ``xlsb`` are in the ``FORMATS`` default — the direct
   regression guard for that fix.
2. Every spreadsheet extension the converter advertises support for
   (``SPREADSHEET_EXTENSIONS``) is present in ``FORMATS`` — a
   consistency guard so a future "the converter now also handles
   ``.xyz``" change can't silently leave ``.xyz`` ungated.
3. The spatial-tolerance setting is read from an all-lowercase key,
   so a revert to the uppercase typo fails CI.
"""

from __future__ import annotations

from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parent.parent


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
    # (The `formats` fixture already importorskip'd ckan.)
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


def test_spatial_tolerance_config_key_is_lowercase():
    """The spatial-simplification relative-tolerance setting must be
    read from an all-lowercase ckan.ini key.

    config.py originally read it from
    ``ckanext.datapusher_plus.SPATIAL_SIMPLIFICATION_RELATIVE_TOLERANCE``
    — UPPERCASE, unlike every other ~50 DP+ keys — so an operator
    setting the documented lowercase form had no effect. AST-parsed
    (no CKAN bootstrap) so a revert to the uppercase typo fails CI.
    """
    import ast

    config_py = (
        REPO_ROOT / "ckanext" / "datapusher_plus" / "config.py"
    ).read_text()
    tree = ast.parse(config_py)
    found_key = None
    for node in ast.walk(tree):
        if not (
            isinstance(node, ast.Assign)
            and any(
                getattr(t, "id", None)
                == "SPATIAL_SIMPLIFICATION_RELATIVE_TOLERANCE"
                for t in node.targets
            )
        ):
            continue
        for call in ast.walk(node):
            if (
                isinstance(call, ast.Call)
                and getattr(call.func, "attr", None) == "get"
                and call.args
                and isinstance(call.args[0], ast.Constant)
            ):
                found_key = call.args[0].value
                break
        break
    assert found_key == (
        "ckanext.datapusher_plus.spatial_simplification_relative_tolerance"
    ), (
        f"config.py reads the spatial tolerance from {found_key!r}; the "
        "key must be all-lowercase to match the rest of DP+'s config "
        "namespace and the README."
    )
