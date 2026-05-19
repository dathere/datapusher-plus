# -*- coding: utf-8 -*-
"""
Regression coverage for issue #261 ("Automatic Empty Date Range handling").

Cross-referenced from
https://github.com/TNRIS/texaswaterhub_CKAN/issues/1035 — reporter's
scheming YAML had ``suggest_formula`` expressions returning ``None``
when the dataset had no date columns (the common case of "this CSV
doesn't carry dates"). Jinja2 renders Python ``None`` to the string
``"None"`` (and undefined variables / empty templates to ``""``), so
the raw output of ``FormulaProcessor.process_formulae`` stored the
literal string ``"None"`` in the ``dpp_suggestions`` JSON instead of
the JSON ``null`` the front-end expected.

The UI then surfaced ``"None"`` as a clickable "Suggestion" because
the string was truthy — and clicking it inserted the literal
``"None"`` into the metadata field. The fix landed in commit
``62c18ea`` (Minhajuddin Mohammed, 2025-11-21) which was made on a
branch that never reached ``main``; this regression test pins the
backend behavior so a future refactor can't silently re-introduce
the bug.

The fix has two layers:

1. **Python** (``FormulaProcessor.process_formulae``): coerce rendered
   ``"None"`` / ``""`` outputs back to actual ``None`` so the
   ``dpp_suggestions`` JSON has true ``null`` for empty suggestions.
2. **JS** (``scheming-suggestions.js`` + ``suggestions.css``): when
   the suggestion value is ``null`` / ``undefined`` / ``"None"`` /
   ``""``, render the per-field button greyed out, with a "no
   suggestion" tooltip, and short-circuit the click handler.

This file covers (1). The JS side is exercised manually in the issue
thread and would be covered by a future vitest spec for
``scheming-suggestions.js`` (the legacy DRUF suggestions module has
no JS test coverage today — out of scope for this fix).

Tests pin six properties of the Python-side coercion:

* ``{{ none }}`` template → Python ``None`` in the output dict
  (NOT the literal string ``"None"`` — that's the #261 failure mode).
* Empty Jinja2 output (undefined variable, empty conditional) → Python ``None``.
* Whitespace-only Jinja2 output (``" "``, ``"\\n"``, etc.) → Python ``None``
  (roborev #2289 LOW caught the original ``== ""`` miss).
* Templates that render a non-empty real value pass through unchanged.
* Even an explicit ``"None"`` string literal in the formula coerces to
  Python ``None`` per the fix-as-written — documents the choice as
  deliberate for the suggestion path.
* Coercion is GATED on ``formula_type == "suggestion_formula"`` —
  direct ``formula`` fields (which write straight into the
  package/resource dict) preserve their rendered output verbatim
  even when empty. Copilot review on PR #322 caught the original
  unconditional coercion.
"""

from __future__ import annotations

from unittest import mock

import pytest


def _build_stub_processor():
    """Construct a ``FormulaProcessor`` without running ``__init__``.

    ``FormulaProcessor.__init__`` does heavy lat/lon/date inference
    on its constructor args and pulls in CKAN config; we just need
    the ``process_formulae`` method here. Mirror the bypass pattern
    used by ``test_security.test_formula_processor_uses_sandboxed_environment``.
    """
    jinja2_helpers = pytest.importorskip(
        "ckanext.datapusher_plus.jinja2_helpers"
    )
    proc = jinja2_helpers.FormulaProcessor.__new__(
        jinja2_helpers.FormulaProcessor
    )
    proc.package = {"id": "pkg-261-test", "name": "issue-261-test"}
    proc.resource = {"id": "res-261-test"}
    proc.resource_fields_stats = {}
    proc.resource_fields_freqs = {}
    proc.dpp = {
        "DATE_FIELDS": [],
        "NO_DATE_FIELDS": True,
        "DATETIME_FIELDS": [],
        "NO_DATETIME_FIELDS": True,
        "LAT_FIELD": None,
        "LON_FIELD": None,
        "NO_LAT_LON_FIELDS": True,
        "dataset_stats": {},
    }
    # Silence the chatty logger; ``trace`` is not a stdlib level so
    # we mock the whole object rather than using ``logging.getLogger``.
    proc.logger = mock.Mock()
    return proc


# The scheming-yaml key (and ``formula_type`` argument) the issue is
# actually about. Initial draft of these tests used ``"suggest_formula"``,
# which is NOT the production key — caught by Copilot review on PR #322.
# Production code (``jobs/stages/formula.py:355,398``) and bundled
# schemas (``dataset-druf.yaml``, ``docs/dataset_schema.yaml``) all use
# ``"suggestion_formula"``.
SUGGEST_FORMULA = "suggestion_formula"


def _yaml_with(field_specs):
    """Build a minimal scheming YAML carrying the given field specs."""
    return {
        "dataset_fields": [
            {"field_name": name, SUGGEST_FORMULA: tmpl}
            for name, tmpl in field_specs
        ],
        "resource_fields": [],
    }


def test_jinja2_none_literal_is_coerced_to_python_none():
    """``{{ none }}`` renders to the string ``"None"`` in Jinja2.
    Without the #261 fix, that string ends up in ``dpp_suggestions``
    and the front-end shows a clickable "None" suggestion.

    The fix coerces ``"None"`` → Python ``None`` so the JSON has
    ``null`` and the UI greys the button out.
    """
    proc = _build_stub_processor()
    proc.scheming_yaml = _yaml_with([
        ("temporal_coverage_start", "{{ dpp.DATE_FIELDS[0] if not dpp.NO_DATE_FIELDS else none }}"),
    ])

    updates = proc.process_formulae(
        entity_type="package",
        fields_key="dataset_fields",
        formula_type=SUGGEST_FORMULA,
    )

    assert "temporal_coverage_start" in updates, (
        "FormulaProcessor must always return an entry for declared "
        f"formula fields; got {updates!r}"
    )
    assert updates["temporal_coverage_start"] is None, (
        "Jinja2's string 'None' must be coerced to Python None for "
        "JSON-null serialization (issue #261). Got "
        f"{updates['temporal_coverage_start']!r} — if you see the "
        "string 'None' here, the fix from commit 62c18ea has "
        "regressed and the UI will start showing clickable 'None' "
        "suggestions again."
    )


def test_empty_template_render_is_coerced_to_python_none():
    """Empty-string Jinja2 output (undefined variables, empty
    conditionals, etc.) must also coerce to Python ``None`` — same
    rationale as the explicit-none case, different render path.
    """
    proc = _build_stub_processor()
    proc.scheming_yaml = _yaml_with([
        # Conditional that produces no output when the branch is empty.
        ("license_id", "{% if dpp.LAT_FIELD %}{{ dpp.LAT_FIELD }}{% endif %}"),
    ])

    updates = proc.process_formulae(
        entity_type="package",
        fields_key="dataset_fields",
        formula_type=SUGGEST_FORMULA,
    )

    assert updates["license_id"] is None, (
        "Empty Jinja2 output must be coerced to Python None (issue "
        f"#261). Got {updates['license_id']!r} — empty string in "
        "dpp_suggestions JSON would show as a clickable empty "
        "Suggestion in the UI."
    )



def test_whitespace_only_template_render_is_coerced_to_python_none():
    """Roborev #2289 LOW caught this: the initial fix used ``== ""``,
    which doesn't match whitespace-only Jinja2 output. A common case is
    a ``{% if %}`` block missing the ``-`` trim markers — the template
    renders to ``" "`` or ``"\\n"`` instead of an empty string, and
    that string would have leaked through as a "non-empty suggestion."

    The strengthened check (``.strip() in ("", "None")``) catches all
    three: empty, whitespace-only, and the ``"None"`` sentinel.
    """
    proc = _build_stub_processor()
    proc.scheming_yaml = _yaml_with([
        # ``{% if %}`` without trim markers — common source of stray
        # whitespace in Jinja2 outputs.
        ("publisher", "  \n  {% if dpp.LAT_FIELD %}{{ dpp.LAT_FIELD }}{% endif %}  \n"),
        ("notes", "   "),
        ("category", "\n\t  "),
    ])

    updates = proc.process_formulae(
        entity_type="package",
        fields_key="dataset_fields",
        formula_type=SUGGEST_FORMULA,
    )

    for field in ("publisher", "notes", "category"):
        assert updates[field] is None, (
            f"Whitespace-only Jinja2 output for {field!r} must coerce to "
            f"Python None; got {updates[field]!r} — if this regresses, "
            "the suggestion button will display whitespace as a "
            "'clickable suggestion' (roborev #2289 LOW)."
        )


def test_real_values_pass_through_unchanged():
    """Negative control: the None-coercion must not swallow
    legitimate non-empty formula outputs. ``"None"`` and ``""`` are
    sentinels; everything else flows through verbatim.
    """
    proc = _build_stub_processor()
    proc.scheming_yaml = _yaml_with([
        ("title", "Inferred title: {{ package.name }}"),
        ("notes", "{{ 'a non-empty string' }}"),
        ("version", "1.0"),
    ])

    updates = proc.process_formulae(
        entity_type="package",
        fields_key="dataset_fields",
        formula_type=SUGGEST_FORMULA,
    )

    assert updates["title"] == "Inferred title: issue-261-test", (
        f"Real values must not be coerced to None; got {updates['title']!r}"
    )
    assert updates["notes"] == "a non-empty string", (
        f"Non-empty string literals must pass through; got {updates['notes']!r}"
    )
    assert updates["version"] == "1.0", (
        f"Static formula values must pass through; got {updates['version']!r}"
    )


def test_string_literal_none_in_formula_output_is_coerced():
    """Belt-and-suspenders: even when the *formula* explicitly
    produces the string ``"None"`` (rather than rendering a Python
    None via ``{{ none }}``), the coercion still fires. This matches
    the actual implementation in commit 62c18ea which checks the
    string value after rendering.

    Slightly contentious (one could argue a formula explicitly
    emitting the literal text "None" should be respected), but the
    fix as landed treats it as a sentinel — and the UI side relies
    on that to disable the button.
    """
    proc = _build_stub_processor()
    proc.scheming_yaml = _yaml_with([
        ("status", "None"),
    ])

    updates = proc.process_formulae(
        entity_type="package",
        fields_key="dataset_fields",
        formula_type=SUGGEST_FORMULA,
    )

    assert updates["status"] is None, (
        f"String 'None' formula output must coerce to Python None "
        f"per the #261 fix in 62c18ea; got {updates['status']!r}."
    )



def test_coercion_is_gated_on_suggestion_formula_type():
    """Copilot review on PR #322 caught this: the initial fix applied
    the empty/None coercion unconditionally for every ``formula_type``.
    But ``process_formulae`` is also called with ``formula_type="formula"``
    (see ``jobs/stages/formula.py:290,321``) to write direct field
    updates into the package / resource dict — coercing those would
    silently change patch semantics for CKAN fields where validators
    treat empty string and null differently.

    This test pins the gating: with ``formula_type="formula"``, an
    empty Jinja2 render passes through as the empty string. With
    ``formula_type="suggestion_formula"``, the same render coerces
    to Python ``None``. Same input, different output, gated on intent.
    """
    proc = _build_stub_processor()
    # Identical template in both fields — only the formula_type differs.
    proc.scheming_yaml = {
        "dataset_fields": [
            {"field_name": "direct_field", "formula": "{{ none }}"},
            {"field_name": "suggested_field", "suggestion_formula": "{{ none }}"},
        ],
        "resource_fields": [],
    }

    # Direct ``formula`` path — must preserve the verbatim "None" string
    # so CKAN's validators see what the formula actually produced.
    direct_updates = proc.process_formulae(
        entity_type="package",
        fields_key="dataset_fields",
        formula_type="formula",
    )
    assert direct_updates["direct_field"] == "None", (
        "Direct ``formula`` outputs must NOT be coerced — preserves "
        "patch semantics for CKAN validators that distinguish empty "
        "string from null. Got "
        f"{direct_updates['direct_field']!r} — if this regresses, the "
        "Copilot-caught broad-coercion bug from PR #322 is back."
    )

    # Suggestion path — gets the #261 coercion.
    suggestion_updates = proc.process_formulae(
        entity_type="package",
        fields_key="dataset_fields",
        formula_type="suggestion_formula",
    )
    assert suggestion_updates["suggested_field"] is None, (
        "Suggestion path must coerce to Python None for the FE's null-check. "
        f"Got {suggestion_updates['suggested_field']!r}."
    )
