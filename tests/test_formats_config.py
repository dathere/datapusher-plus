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

These tests are AST-parsed against the source files — no CKAN import,
no runtime config — so they are deterministic config-drift guards in
the same style as ``test_pii_screening_config_key`` /
``test_issue_142_auto_index_threshold``. They pin:

1. ``xlsm`` and ``xlsb`` are in the ``FORMATS`` default list literal.
2. Every spreadsheet extension the converter advertises
   (``SPREADSHEET_EXTENSIONS``) is present in the ``FORMATS`` default
   — a consistency guard so a future "the converter now also handles
   ``.xyz``" change can't silently leave ``.xyz`` ungated.
3. The spatial-tolerance setting's *primary* (outermost) config key
   is the all-lowercase name — robust to the legacy-uppercase-key
   fallback nested as a second ``tk.config.get`` argument.
"""

from __future__ import annotations

import ast
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PY = REPO_ROOT / "ckanext" / "datapusher_plus" / "config.py"
FORMAT_CONVERTER_PY = (
    REPO_ROOT
    / "ckanext"
    / "datapusher_plus"
    / "jobs"
    / "stages"
    / "format_converter.py"
)


def _find_assignment(source_path, var_name):
    """Return the ``ast.Assign`` node that assigns ``var_name`` at any
    depth in ``source_path``, or ``None``."""
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
            getattr(t, "id", None) == var_name for t in node.targets
        ):
            return node
    return None


def _string_list(list_node):
    """Extract the Python string values from an ``ast.List`` of string
    constants."""
    assert isinstance(list_node, ast.List), (
        f"expected a list literal, got {type(list_node).__name__}"
    )
    return [
        elt.value
        for elt in list_node.elts
        if isinstance(elt, ast.Constant) and isinstance(elt.value, str)
    ]


def _formats_default_list():
    """The ``FORMATS`` default list literal from ``config.py``.

    ``FORMATS = tk.config.get("...formats", [<default list>])`` — the
    default is the call's second positional argument.
    """
    assign = _find_assignment(CONFIG_PY, "FORMATS")
    assert assign is not None, "FORMATS assignment not found in config.py"
    call = assign.value
    assert isinstance(call, ast.Call) and len(call.args) >= 2, (
        "expected `FORMATS = tk.config.get(<key>, [<default>])`"
    )
    return _string_list(call.args[1])


def _converter_spreadsheet_extensions():
    """The ``SPREADSHEET_EXTENSIONS`` list literal from
    ``format_converter.py``."""
    assign = _find_assignment(FORMAT_CONVERTER_PY, "SPREADSHEET_EXTENSIONS")
    assert assign is not None, (
        "SPREADSHEET_EXTENSIONS not found in format_converter.py"
    )
    return _string_list(assign.value)


def test_xlsm_and_xlsb_are_in_formats_default():
    # format_converter can convert both; the FORMATS default must let
    # them in or DP+ skips the file before the converter sees it.
    formats = _formats_default_list()
    assert "xlsm" in formats
    assert "xlsb" in formats


def test_formats_default_covers_every_converter_spreadsheet_extension():
    # Consistency guard: anything FormatConverterStage advertises in
    # SPREADSHEET_EXTENSIONS must be gated in by the FORMATS default,
    # or DP+ would reject a file format it actually knows how to
    # convert.
    formats_lower = {f.lower() for f in _formats_default_list()}
    for ext in _converter_spreadsheet_extensions():
        assert ext.lower() in formats_lower, (
            f"{ext} is in FormatConverterStage.SPREADSHEET_EXTENSIONS but "
            f"not in config.FORMATS — DP+ would skip .{ext.lower()} files "
            "before the converter ever sees them."
        )


def test_spatial_tolerance_primary_config_key_is_lowercase():
    """The spatial-simplification relative-tolerance setting's primary
    key must be all-lowercase.

    config.py originally read it from
    ``ckanext.datapusher_plus.SPATIAL_SIMPLIFICATION_RELATIVE_TOLERANCE``
    — UPPERCASE, unlike every other ~50 DP+ keys — so an operator
    setting the documented lowercase form had no effect.

    The assertion targets the assignment's ``.value`` directly — the
    *outermost* ``tk.config.get`` — so it stays correct even though a
    legacy-uppercase-key fallback is nested as that call's second
    argument. (Walking for "the first ``.get``" would be fragile here.)
    """
    assign = _find_assignment(
        CONFIG_PY, "SPATIAL_SIMPLIFICATION_RELATIVE_TOLERANCE"
    )
    assert assign is not None, (
        "SPATIAL_SIMPLIFICATION_RELATIVE_TOLERANCE assignment not found "
        "in config.py"
    )
    primary = assign.value
    assert (
        isinstance(primary, ast.Call)
        and getattr(primary.func, "attr", None) == "get"
        and primary.args
        and isinstance(primary.args[0], ast.Constant)
    ), "expected the setting to be read via tk.config.get(<key>, ...)"
    primary_key = primary.args[0].value
    assert primary_key == (
        "ckanext.datapusher_plus.spatial_simplification_relative_tolerance"
    ), (
        f"config.py's primary key for the spatial tolerance is "
        f"{primary_key!r}; it must be the all-lowercase name to match "
        "the rest of DP+'s config namespace and the README. (A legacy "
        "UPPERCASE key may still be honoured as a nested fallback.)"
    )
