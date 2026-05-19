# -*- coding: utf-8 -*-
"""
Regression coverage for issue #112 ("Does not handle locale").

The original reporter's sample (German locale, comma as decimal
separator) caused qsv to infer numeric columns as ``String`` and DP+
to store them as Postgres Text:

    "Kennziffer","Einheit","12.06.1994","13.06.1999",...
    "Wahlbeteiligung","%","57,957","41,84699",...
    "GRÜNE","%","10,16194","4,992445",...

qsv's number parser only recognizes ``.`` as the decimal separator —
CSV files don't carry locale metadata, so that's correct default
behavior. The fix runs a Python preprocessing pass between header
sanitization and the stats / index pass; the pass is gated on a
locale being resolved for the resource.

Resolution order (per resource, per ingestion):

1. ``context.resource.get('dpp_locale')`` (per-resource override)
2. ``conf.DEFAULT_LOCALE`` (global default)
3. ``conf.DECIMAL_SEPARATOR`` (single-char escape hatch)
4. None of the above → no-op (pre-#112 behavior preserved)

Locale paths route through ``babel.numbers.parse_decimal`` so the full
CLDR shape works: ``57,957`` (de_DE), ``1.234,56`` (de_DE thousands +
decimal), ``57 957,12`` (fr_FR space-thousands), etc. The
separator-only path uses an anchored regex (``^-?\\d+<sep>\\d+$``) for
operators who know the separator but don't have or want CLDR locale
info.

These tests pin nine properties:

1. ``config.py``'s inline fallbacks for ``DEFAULT_LOCALE`` and
   ``DECIMAL_SEPARATOR`` are both empty strings (opt-in only; the
   historical default behavior is preserved).
2. ``config_declaration.yaml`` declares both keys with matching empty
   defaults (a #179-style declaration-vs-fallback drift would let the
   defaults silently disagree).
3. ``dataset-druf.yaml`` declares ``dpp_locale`` as a resource_field
   (so the scheming UI surfaces it and ``resource_show`` returns it
   as a top-level key on the resource dict).
4. ``AnalysisStage._normalize_locale_numbers`` is a no-op when nothing
   resolves — no temp file written, no ``context.tmp`` mutation.
5. ``default_locale='de_DE'`` normalizes the German sample correctly,
   including the thousands+decimal form ``1.234,56`` → ``1234.56``.
6. Per-resource ``dpp_locale='fr_FR'`` overrides the global default
   (French uses both comma decimal AND space thousands).
7. ``decimal_separator=','`` works with no locale at all.
8. Unknown locale ``xx_YY`` logs a WARNING and short-circuits to a
   no-op (does NOT crash, does NOT fall back to the default).
9. **End-to-end with real qsv** on the reporter's exact German sample:
   without conversion all comma-decimal columns infer as ``String``
   (pinning the #112 failure mode); with ``default_locale='de_DE'``
   they infer as ``Float`` and the legitimate string columns
   (``Kennziffer``, ``Einheit``) stay ``String``.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest


REPO_ROOT = Path(__file__).resolve().parent.parent

# The reporter's exact sample, augmented with a third data row and one
# thousands+decimal value so qsv's inference has more than one row to
# work with AND the test exercises babel's full CLDR parsing (not just
# the simple ``<int>,<frac>`` shape that a regex would also catch).
GERMAN_SAMPLE_CSV = (
    '"Kennziffer","Einheit","12.06.1994","13.06.1999","13.06.2004",'
    '"07.06.2009","25.05.2014","26.05.2019"\n'
    '"Wahlbeteiligung","%","57,957","41,84699","39,0107",'
    '"41,86174","42,6241","61,59743"\n'
    '"GRÜNE","%","10,16194","4,992445","12,1425",'
    '"13,09138","12,38852","20,18807"\n'
    '"SPD","%","1.234,56","22,1234","21,5432",'
    '"24,7890","19,8765","18,4321"\n'
)


# ---------------------------------------------------------------------------
# Default values — pinned via AST/YAML parse so the test doesn't need CKAN
# ---------------------------------------------------------------------------


def _walk_config_get_defaults(prefix_substring: str) -> dict:
    """Walk ``config.py`` for ``tk.config.get(<key>, <default>)`` calls
    where ``<key>`` contains ``prefix_substring``.

    AST-parsed rather than imported to keep this test free of the CKAN
    bootstrap — same pattern as ``test_issue_142_auto_index_threshold.py``.
    """
    import ast

    src = (REPO_ROOT / "ckanext" / "datapusher_plus" / "config.py").read_text(
        encoding="utf-8"
    )
    tree = ast.parse(src)

    found = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not (
            isinstance(node.func, ast.Attribute) and node.func.attr == "get"
        ):
            continue
        if not node.args or not isinstance(node.args[0], ast.Constant):
            continue
        key = node.args[0].value
        if not isinstance(key, str) or prefix_substring not in key:
            continue
        if len(node.args) >= 2 and isinstance(node.args[1], ast.Constant):
            found[key] = node.args[1].value
    return found


def test_config_py_inline_fallbacks_are_empty_strings():
    """``config.py``'s inline ``DEFAULT_LOCALE`` and
    ``DECIMAL_SEPARATOR`` fallbacks must both be empty strings.

    Empty = "don't preprocess." Operators who want #112 behavior have
    to opt in explicitly via ``default_locale`` (CLDR id) or
    ``decimal_separator`` (single char).
    """
    defaults = _walk_config_get_defaults("ckanext.datapusher_plus.")
    assert (
        defaults.get("ckanext.datapusher_plus.default_locale") == ""
    ), (
        "config.py default_locale inline default is "
        f"{defaults.get('ckanext.datapusher_plus.default_locale')!r}; "
        "must be the empty string per issue #112 (opt-in only)."
    )
    assert (
        defaults.get("ckanext.datapusher_plus.decimal_separator") == ""
    ), (
        "config.py decimal_separator inline default is "
        f"{defaults.get('ckanext.datapusher_plus.decimal_separator')!r}; "
        "must be the empty string per issue #112 (opt-in only)."
    )


def test_config_declaration_defaults_match_python_fallback():
    """The YAML declaration defaults must agree with the Python
    fallbacks. Drift between the two (the #179-style failure mode)
    would mean operators on different CKAN versions silently see
    different defaults.

    NOTE on ``type``: CKAN's config_declaration loader accepts only
    ``base``, ``bool``, ``int``, ``dynamic``, ``list`` — there's no
    ``str`` type. The convention for string-valued settings is to
    OMIT the ``type`` key entirely (see ``download_proxy``,
    ``file_hash_algorithm``). The loader then defaults to ``base``,
    which validates as a free-form string. So we explicitly assert
    that ``type`` is *absent* — declaring it as ``str`` is the bug
    that broke CI on the first push of this PR.
    """
    try:
        import yaml
    except ImportError:
        pytest.skip("PyYAML not available")

    yaml_path = (
        REPO_ROOT / "ckanext" / "datapusher_plus" / "config_declaration.yaml"
    )
    decl = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))

    keys = {"ckanext.datapusher_plus.default_locale",
            "ckanext.datapusher_plus.decimal_separator"}
    found = {}
    for group in decl.get("groups", []):
        for opt in group.get("options", []):
            if opt.get("key") in keys:
                found[opt["key"]] = opt

    for key in keys:
        assert key in found, (
            f"{key} is missing from config_declaration.yaml — "
            "issue #112 should declare it."
        )
        assert "type" not in found[key], (
            f"{key} must NOT declare a ``type`` (string settings "
            "omit it so the loader defaults to ``base``). CKAN's "
            "config_declaration loader rejects ``type: str`` with "
            "``Value must be one of ['base', 'bool', 'int', "
            "'dynamic', 'list']`` — that's what broke CI initially."
        )
        assert found[key].get("default") == "", (
            f"{key} declared default is {found[key].get('default')!r}; "
            "must be '' to match config.py's inline fallback."
        )


def test_dataset_druf_declares_dpp_locale_resource_field():
    """``dpp_locale`` must be declared in ``dataset-druf.yaml`` as a
    resource_field so the scheming UI surfaces it and
    ``resource_show`` returns it as a top-level key on the resource
    dict. Without this declaration, ``context.resource.get('dpp_locale')``
    would silently return ``None`` and the per-resource override path
    would be dead code.
    """
    try:
        import yaml
    except ImportError:
        pytest.skip("PyYAML not available")

    druf_path = (
        REPO_ROOT / "ckanext" / "datapusher_plus" / "dataset-druf.yaml"
    )
    decl = yaml.safe_load(druf_path.read_text(encoding="utf-8"))

    resource_fields = decl.get("resource_fields") or []
    dpp_locale_field = next(
        (f for f in resource_fields if f.get("field_name") == "dpp_locale"),
        None,
    )
    assert dpp_locale_field is not None, (
        "dpp_locale resource_field is missing from dataset-druf.yaml; "
        "per-resource locale overrides won't reach AnalysisStage."
    )


# ---------------------------------------------------------------------------
# AnalysisStage._normalize_locale_numbers — gating + behavior
# ---------------------------------------------------------------------------


def _build_context(tmp_path, resource=None):
    """Minimal ``ProcessingContext`` for the helper."""
    src = tmp_path / "input.csv"
    src.write_text(GERMAN_SAMPLE_CSV, encoding="utf-8")
    ctx = SimpleNamespace(
        tmp=str(src),
        temp_dir=str(tmp_path),
        logger=mock.Mock(),
        resource=resource or {},
    )
    def _update_tmp(new_tmp):
        ctx.tmp = new_tmp
    ctx.update_tmp = _update_tmp
    return ctx


def _read_csv_rows(path):
    """Tiny helper for assertions on the rewritten CSV."""
    import csv
    with open(path, "r", encoding="utf-8", newline="") as fh:
        return list(csv.reader(fh))


def test_no_op_when_nothing_resolves(tmp_path):
    """Nothing set anywhere → no temp file, no ``context.tmp`` mutation.
    This is the single most important property of the helper: an
    upgrade that introduces ``_normalize_locale_numbers`` must not
    change behavior for operators who didn't opt in.
    """
    from ckanext.datapusher_plus.jobs.stages.analysis import AnalysisStage
    from ckanext.datapusher_plus import config as conf

    ctx = _build_context(tmp_path)
    original_tmp = ctx.tmp

    stage = AnalysisStage()
    with mock.patch.object(conf, "DEFAULT_LOCALE", ""), \
         mock.patch.object(conf, "DECIMAL_SEPARATOR", ""):
        stage._normalize_locale_numbers(ctx)

    assert ctx.tmp == original_tmp, (
        "decimal-comma off must not mutate context.tmp; got "
        f"{ctx.tmp!r}, expected {original_tmp!r}."
    )
    # The "qsv_locale_normalized.csv" file shouldn't even exist —
    # presence would indicate we did the work and then declined to
    # route it forward, which would be a different bug class.
    assert not (tmp_path / "qsv_locale_normalized.csv").exists(), (
        "no-op path must not write the normalized file at all."
    )


def test_default_locale_de_de_normalizes_german_sample(tmp_path):
    """``default_locale='de_DE'`` must rewrite ``57,957`` →
    ``57.957`` AND the thousands-and-decimal form ``1.234,56`` →
    ``1234.56`` (the second is the property that distinguishes a true
    babel-based approach from a single anchored regex).
    """
    from ckanext.datapusher_plus.jobs.stages.analysis import AnalysisStage
    from ckanext.datapusher_plus import config as conf

    ctx = _build_context(tmp_path)
    stage = AnalysisStage()
    with mock.patch.object(conf, "DEFAULT_LOCALE", "de_DE"), \
         mock.patch.object(conf, "DECIMAL_SEPARATOR", ""):
        stage._normalize_locale_numbers(ctx)

    assert ctx.tmp.endswith("qsv_locale_normalized.csv"), (
        "default_locale='de_DE' must route context.tmp to the "
        f"normalized output; got {ctx.tmp!r}."
    )
    rows = _read_csv_rows(ctx.tmp)
    # Header row passes through unchanged (column names are NOT
    # number-parsed even though some look like German dates).
    assert rows[0][0] == "Kennziffer"
    assert rows[0][2] == "12.06.1994"  # header still has dot
    # SPD row, column 2 — was "1.234,56", must now be "1234.56".
    assert rows[3][2] == "1234.56", (
        f"thousands+decimal '1.234,56' must normalize to '1234.56'; "
        f"got {rows[3][2]!r}. Full SPD row: {rows[3]!r}."
    )
    # Wahlbeteiligung row, column 2 — was "57,957", must now be "57.957".
    assert rows[1][2] == "57.957", (
        f"simple comma-decimal '57,957' must normalize to '57.957'; "
        f"got {rows[1][2]!r}."
    )
    # GRÜNE stays untouched (babel rejects it as NumberFormatError).
    assert rows[2][0] == "GRÜNE"


def test_resource_dpp_locale_overrides_global_default(tmp_path):
    """Per-resource ``dpp_locale='fr_FR'`` must win over a global
    ``default_locale='de_DE'``. Verified by parsing a French
    space-thousands form (``57 957,12``) that wouldn't make sense to
    a German parser.
    """
    from ckanext.datapusher_plus.jobs.stages.analysis import AnalysisStage
    from ckanext.datapusher_plus import config as conf

    src = tmp_path / "french.csv"
    # French CSV with U+202F NARROW NO-BREAK SPACE thousands separator
    # (what babel/CLDR uses for fr_FR). A plain ASCII space would not
    # round-trip cleanly with babel.
    src.write_text(
        'col\n"57 957,12"\n"42,5"\n',
        encoding="utf-8",
    )
    ctx = SimpleNamespace(
        tmp=str(src),
        temp_dir=str(tmp_path),
        logger=mock.Mock(),
        resource={"dpp_locale": "fr_FR"},
    )
    def _update_tmp(new_tmp):
        ctx.tmp = new_tmp
    ctx.update_tmp = _update_tmp

    stage = AnalysisStage()
    with mock.patch.object(conf, "DEFAULT_LOCALE", "de_DE"), \
         mock.patch.object(conf, "DECIMAL_SEPARATOR", ""):
        stage._normalize_locale_numbers(ctx)

    rows = _read_csv_rows(ctx.tmp)
    assert rows[1][0] == "57957.12", (
        "fr_FR space-thousands '57 957,12' must normalize to "
        f"'57957.12' under per-resource override; got {rows[1][0]!r}."
    )
    assert rows[2][0] == "42.5", (
        "fr_FR comma-decimal '42,5' must normalize to '42.5'; "
        f"got {rows[2][0]!r}."
    )


def test_decimal_separator_only_works_without_a_locale(tmp_path):
    """``decimal_separator=','`` with no locale must still convert
    ``57,957`` → ``57.957`` via the anchored regex path. This is the
    escape hatch for operators with non-CLDR data."""
    from ckanext.datapusher_plus.jobs.stages.analysis import AnalysisStage
    from ckanext.datapusher_plus import config as conf

    ctx = _build_context(tmp_path)
    stage = AnalysisStage()
    with mock.patch.object(conf, "DEFAULT_LOCALE", ""), \
         mock.patch.object(conf, "DECIMAL_SEPARATOR", ","):
        stage._normalize_locale_numbers(ctx)

    rows = _read_csv_rows(ctx.tmp)
    assert rows[1][2] == "57.957"
    # The separator path does NOT handle ``1.234,56`` — the dot makes
    # the cell fail the anchored regex. Pin that limitation; it's
    # documented and intentional (the locale path is the right
    # vehicle for thousands-separated forms).
    assert rows[3][2] == "1.234,56", (
        "separator path must leave thousands+decimal forms verbatim "
        f"(anchored regex doesn't match); got {rows[3][2]!r}."
    )


def test_unknown_locale_logs_warning_and_short_circuits(tmp_path):
    """Unknown locale ``xx_YY`` must produce a WARNING and a no-op.
    Crashing on a user-data typo would block ingestion; silently
    falling back to ``DEFAULT_LOCALE`` would hide misconfiguration.
    """
    from ckanext.datapusher_plus.jobs.stages.analysis import AnalysisStage
    from ckanext.datapusher_plus import config as conf

    ctx = _build_context(tmp_path, resource={"dpp_locale": "xx_YY"})
    original_tmp = ctx.tmp

    stage = AnalysisStage()
    # default_locale set to a real locale to prove we DON'T fall back to it
    with mock.patch.object(conf, "DEFAULT_LOCALE", "de_DE"), \
         mock.patch.object(conf, "DECIMAL_SEPARATOR", ""):
        stage._normalize_locale_numbers(ctx)

    # Warning was logged.
    warning_calls = [
        call.args[0] for call in ctx.logger.warning.mock_calls
    ]
    assert any("xx_YY" in msg for msg in warning_calls), (
        "Expected a WARNING mentioning the unknown locale; "
        f"saw: {warning_calls!r}"
    )
    # Behavior was a no-op (no fall-through to DEFAULT_LOCALE).
    assert ctx.tmp == original_tmp, (
        "Unknown locale must not fall through to DEFAULT_LOCALE; got "
        f"{ctx.tmp!r}, expected {original_tmp!r}."
    )



def test_de_de_does_not_corrupt_date_like_data_cells(tmp_path):
    """Critical regression test for the bug Copilot caught on PR #320.

    Under a comma-decimal locale like de_DE, ``babel.parse_decimal``
    with ``strict=False`` treats ``.`` as a permissive thousands
    separator and silently rewrites date-like strings::

        parse_decimal('12.06.1994', locale='de_DE', strict=False)
        # → Decimal('12061994')   ← SILENT DATA CORRUPTION

    ``strict=True`` enforces canonical grouping (3-digit thousands)
    and correctly raises ``NumberFormatError`` for non-canonical
    inputs, so the cell is left verbatim. This test pins that
    behavior: a German CSV with dates in DATA cells (not just headers)
    must survive locale normalization unmolested.
    """
    from ckanext.datapusher_plus.jobs.stages.analysis import AnalysisStage
    from ckanext.datapusher_plus import config as conf

    # Three-column CSV with: name, decimal-number, date.
    # The middle column has valid comma-decimal numbers (should
    # convert). The last column has German-format dates (must NOT
    # convert — that's the bug).
    src = tmp_path / "german_with_dates.csv"
    src.write_text(
        '"name","value","date"\n'
        '"Wahl",\"57,957\","12.06.1994"\n'
        '"GRÜNE",\"10,16194\","13.06.1999"\n'
        '"SPD",\"1.234,56\","26.05.2019"\n',
        encoding="utf-8",
    )
    ctx = SimpleNamespace(
        tmp=str(src),
        temp_dir=str(tmp_path),
        logger=mock.Mock(),
        resource={},
    )
    def _update_tmp(new_tmp):
        ctx.tmp = new_tmp
    ctx.update_tmp = _update_tmp

    stage = AnalysisStage()
    with mock.patch.object(conf, "DEFAULT_LOCALE", "de_DE"), \
         mock.patch.object(conf, "DECIMAL_SEPARATOR", ""):
        stage._normalize_locale_numbers(ctx)

    rows = _read_csv_rows(ctx.tmp)
    # Sanity: comma-decimal numbers DO convert.
    assert rows[1][1] == "57.957", (
        f"Comma-decimal value should still convert; got {rows[1][1]!r}."
    )
    assert rows[3][1] == "1234.56", (
        f"Thousands+decimal value should still convert; got {rows[3][1]!r}."
    )
    # The fix: date cells must be left VERBATIM, not silently rewritten
    # to enormous integers like Decimal('12061994') / '12061994'.
    assert rows[1][2] == "12.06.1994", (
        f"Date '12.06.1994' must NOT be number-parsed under de_DE; "
        f"got {rows[1][2]!r}. If you see '12061994' here, "
        "``strict=True`` is not wired in _rewrite_cell — that's the "
        "Copilot-caught data corruption bug from PR #320."
    )
    assert rows[2][2] == "13.06.1999", (
        f"Date '13.06.1999' must stay verbatim; got {rows[2][2]!r}."
    )
    assert rows[3][2] == "26.05.2019", (
        f"Date '26.05.2019' must stay verbatim; got {rows[3][2]!r}."
    )


def test_dpp_locale_dot_decimal_does_not_fall_through_to_separator(tmp_path):
    """Regression test for the second Copilot finding on PR #320.

    If an operator explicitly declares a per-resource
    ``dpp_locale='en_US'`` (or the global ``default_locale='en_US'``
    is set), that's an intentional "this resource is en_US" signal.
    The global ``decimal_separator`` must NOT silently apply to it,
    because the operator's declared locale already settles the
    question — applying the separator would be a surprise.

    Pinning: with ``dpp_locale='en_US'`` and a global
    ``decimal_separator=','`` set, ``57,957`` stays verbatim
    (NOT rewritten to ``57.957``).
    """
    from ckanext.datapusher_plus.jobs.stages.analysis import AnalysisStage
    from ckanext.datapusher_plus import config as conf

    src = tmp_path / "english.csv"
    src.write_text('col\n"57,957"\n"GRÜNE"\n', encoding="utf-8")
    ctx = SimpleNamespace(
        tmp=str(src),
        temp_dir=str(tmp_path),
        logger=mock.Mock(),
        resource={"dpp_locale": "en_US"},  # per-resource opts into en_US
    )
    original_tmp = ctx.tmp
    def _update_tmp(new_tmp):
        ctx.tmp = new_tmp
    ctx.update_tmp = _update_tmp

    stage = AnalysisStage()
    with mock.patch.object(conf, "DEFAULT_LOCALE", ""), \
         mock.patch.object(conf, "DECIMAL_SEPARATOR", ","):
        # Global separator says "rewrite comma-decimal" — but the
        # per-resource locale says "this is en_US". Per-resource intent
        # wins; the separator must NOT apply.
        stage._normalize_locale_numbers(ctx)

    # No-op: context.tmp untouched, no normalized file written.
    assert ctx.tmp == original_tmp, (
        "Per-resource dpp_locale='en_US' should suppress the global "
        "decimal_separator; expected context.tmp unchanged, got "
        f"{ctx.tmp!r}."
    )
    assert not (tmp_path / "qsv_locale_normalized.csv").exists(), (
        "Per-resource dot-decimal locale must short-circuit; no "
        "normalized file should have been written."
    )
    # And the INFO log should mention how to disable the locale if
    # the operator actually wants the separator to apply.
    info_calls = [call.args[0] for call in ctx.logger.info.mock_calls]
    assert any(
        "unset" in msg and "dpp_locale" in msg for msg in info_calls
    ), (
        "Skip log should guide the operator on how to enable the "
        f"separator fallback; saw: {info_calls!r}"
    )


# ---------------------------------------------------------------------------
# End-to-end with real qsv — pins the actual #112 behavior
# ---------------------------------------------------------------------------


def _find_qsv():
    """Return the qsv binary path used by tests, or ``None`` if absent."""
    if (env_bin := os.environ.get("QSV_BIN")):
        if Path(env_bin).is_file():
            return env_bin
    for name in ("qsvdp", "qsv"):
        if (found := shutil.which(name)):
            return found
    return None


@pytest.mark.skipif(_find_qsv() is None, reason="qsv binary not on PATH")
def test_end_to_end_german_sample_inference_with_real_qsv(tmp_path):
    """Run the full helper + a real ``qsv stats`` pass on the
    reporter's exact German sample, and pin the type inferences.

    Without the conversion, all six date-named columns infer as
    ``String`` (the #112 failure mode). With it, they infer as
    ``Float`` and the legitimate string columns stay ``String``.
    That's the whole contract of issue #112 in one assertion pass.
    """
    from ckanext.datapusher_plus.jobs.stages.analysis import AnalysisStage
    from ckanext.datapusher_plus import config as conf

    qsv_bin = _find_qsv()

    # ---- Baseline: pin the #112 failure mode ----
    baseline_csv = tmp_path / "baseline.csv"
    baseline_csv.write_text(GERMAN_SAMPLE_CSV, encoding="utf-8")
    baseline = subprocess.run(
        [qsv_bin, "stats", "--typesonly", str(baseline_csv)],
        capture_output=True, text=True, check=True,
    )
    baseline_types = dict(
        line.split(",", 1)
        for line in baseline.stdout.strip().splitlines()[1:]
    )
    assert all(
        baseline_types[col] == "String"
        for col in ("12.06.1994", "13.06.1999", "13.06.2004",
                    "07.06.2009", "25.05.2014", "26.05.2019")
    ), (
        "Baseline qsv stats must infer all comma-decimal columns as "
        f"String (that's the #112 failure mode); got {baseline_types!r}."
    )

    # ---- Run the helper with default_locale='de_DE' ----
    ctx = _build_context(tmp_path)
    stage = AnalysisStage()
    with mock.patch.object(conf, "DEFAULT_LOCALE", "de_DE"), \
         mock.patch.object(conf, "DECIMAL_SEPARATOR", ""):
        stage._normalize_locale_numbers(ctx)

    # ---- Re-run qsv stats on the converted file ----
    after = subprocess.run(
        [qsv_bin, "stats", "--typesonly", ctx.tmp],
        capture_output=True, text=True, check=True,
    )
    after_types = dict(
        line.split(",", 1)
        for line in after.stdout.strip().splitlines()[1:]
    )

    for col in ("12.06.1994", "13.06.1999", "13.06.2004",
                "07.06.2009", "25.05.2014", "26.05.2019"):
        assert after_types[col] == "Float", (
            f"After locale normalization, column {col!r} should infer "
            f"as Float; got {after_types[col]!r}. Full types: "
            f"{after_types!r}."
        )

    # Legitimate string columns must NOT be touched.
    assert after_types["Kennziffer"] == "String", (
        "Kennziffer (Wahlbeteiligung, GRÜNE, SPD) must stay String; "
        f"got {after_types['Kennziffer']!r}."
    )
    assert after_types["Einheit"] == "String", (
        f"Einheit (%) must stay String; got {after_types['Einheit']!r}."
    )
