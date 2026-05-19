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
CSV files don't carry locale metadata, so this is correct default
behavior. The fix is opt-in via the new
``ckanext.datapusher_plus.decimal_comma`` config flag, which adds a
preprocessing step that runs ``qsv replace`` with the anchored regex
``^(-?\\d+),(\\d+)$`` and replacement ``${1}.${2}``. Only whole-cell
comma-decimal values are rewritten; strings like ``GRÜNE`` and
``Wahlbeteiligung`` are left untouched because they don't match the
anchored pattern.

These tests pin five properties:

1. ``config.py``'s inline fallback for ``DECIMAL_COMMA`` is ``False``
   (opt-in only; the historical default behavior is preserved).
2. ``config_declaration.yaml`` declares the key with the same default
   (a #179-style declaration-vs-fallback drift would let the defaults
   silently disagree).
3. ``AnalysisStage._convert_decimal_comma`` is a no-op when the flag
   is off — no qsv invocation, no ``context.tmp`` mutation.
4. When the flag is on, the helper calls ``context.qsv.replace`` with
   the issue-#112 regex + replacement, then updates ``context.tmp`` to
   the converted file.
5. **End-to-end with real qsv**: running the regex replace on the
   issue's exact German sample yields a CSV where ``qsv stats`` infers
   the previously-misclassified columns as ``Float`` and leaves the
   legitimate string columns (``Kennziffer``, ``Einheit``) as
   ``String``.
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

# The reporter's exact sample, augmented with a third data row so qsv's
# inference has more than one row to work with (stats inference on a
# single data row is uninteresting — qsv would call almost anything a
# valid Float).
GERMAN_SAMPLE_CSV = (
    '"Kennziffer","Einheit","12.06.1994","13.06.1999","13.06.2004",'
    '"07.06.2009","25.05.2014","26.05.2019"\n'
    '"Wahlbeteiligung","%","57,957","41,84699","39,0107",'
    '"41,86174","42,6241","61,59743"\n'
    '"GRÜNE","%","10,16194","4,992445","12,1425",'
    '"13,09138","12,38852","20,18807"\n'
    '"SPD","%","23,4567","22,1234","21,5432",'
    '"24,7890","19,8765","18,4321"\n'
)


# ---------------------------------------------------------------------------
# Default values — pinned via AST/YAML parse so the test doesn't need CKAN
# ---------------------------------------------------------------------------


def test_config_py_inline_fallback_default_is_false():
    """``config.py``'s inline ``DECIMAL_COMMA`` fallback must be False.

    AST-parsed rather than imported to keep this test free of the CKAN
    bootstrap (matches the pattern in ``test_issue_142_...py``).
    """
    import ast

    config_py = (
        REPO_ROOT / "ckanext" / "datapusher_plus" / "config.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(config_py)

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
        if key != "ckanext.datapusher_plus.decimal_comma":
            continue
        if len(node.args) >= 2 and isinstance(node.args[1], ast.Constant):
            found[key] = node.args[1].value

    assert (
        found.get("ckanext.datapusher_plus.decimal_comma") is False
    ), (
        "config.py decimal_comma inline default is "
        f"{found.get('ckanext.datapusher_plus.decimal_comma')!r}; must "
        "be False per issue #112 (opt-in only; preserves pre-#112 "
        "behavior)."
    )


def test_config_declaration_default_is_false():
    """The YAML declaration default must agree with the Python fallback.

    Drift between the two (the #179-style failure mode) would mean
    operators running CKAN >= 2.10 see ``True`` as the default while
    code-paths that fall back to the inline default see ``False`` —
    silent and confusing.
    """
    try:
        import yaml
    except ImportError:
        pytest.skip("PyYAML not available")

    yaml_path = (
        REPO_ROOT / "ckanext" / "datapusher_plus" / "config_declaration.yaml"
    )
    decl = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))

    found = None
    for group in decl.get("groups", []):
        for opt in group.get("options", []):
            if opt.get("key") == "ckanext.datapusher_plus.decimal_comma":
                found = opt
                break
        if found:
            break

    assert found is not None, (
        "ckanext.datapusher_plus.decimal_comma is missing from "
        "config_declaration.yaml — issue #112 should declare it."
    )
    assert found.get("default") is False, (
        "config_declaration.yaml decimal_comma default is "
        f"{found.get('default')!r}; must be False to match "
        "config.py's inline fallback (drift would re-introduce the "
        "#179-style failure mode)."
    )
    assert found.get("type") == "bool", (
        "decimal_comma must be declared as type 'bool'."
    )


# ---------------------------------------------------------------------------
# AnalysisStage._convert_decimal_comma — gating + qsv-call shape
# ---------------------------------------------------------------------------


def _build_context_with_mock_qsv(tmp_path):
    """Minimal context with a mocked ``qsv`` and an on-disk working CSV."""
    src = tmp_path / "input.csv"
    src.write_text(GERMAN_SAMPLE_CSV, encoding="utf-8")

    qsv_mock = mock.Mock()
    # Simulate qsv replace producing an output file; the helper only
    # needs ``stderr`` for the log line and an output file on disk.
    def _fake_replace(input_file, pattern, replacement, output_file, not_one):
        # Mimic qsv's behavior: write the converted CSV to ``output_file``.
        # The unit test doesn't care about correctness of the conversion
        # itself — that's covered by the end-to-end test below.
        Path(output_file).write_text(
            Path(input_file).read_text(encoding="utf-8"), encoding="utf-8"
        )
        result = mock.Mock()
        result.stderr = b"42"  # bogus replacement count for log assertion
        return result
    qsv_mock.replace.side_effect = _fake_replace

    ctx = SimpleNamespace(
        tmp=str(src),
        temp_dir=str(tmp_path),
        logger=mock.Mock(),
        qsv=qsv_mock,
    )
    # Real ProcessingContext.update_tmp just sets self.tmp; mirror that.
    def _update_tmp(new_tmp):
        ctx.tmp = new_tmp
    ctx.update_tmp = _update_tmp
    return ctx


def test_convert_decimal_comma_is_noop_when_flag_off(tmp_path):
    """``DECIMAL_COMMA=False`` must produce zero qsv calls and leave
    ``context.tmp`` untouched. The flag is the only thing protecting
    operators from an unexpected file rewrite on every ingestion.
    """
    from ckanext.datapusher_plus.jobs.stages.analysis import AnalysisStage
    from ckanext.datapusher_plus import config as conf

    ctx = _build_context_with_mock_qsv(tmp_path)
    original_tmp = ctx.tmp

    stage = AnalysisStage()
    with mock.patch.object(conf, "DECIMAL_COMMA", False):
        stage._convert_decimal_comma(ctx)

    assert ctx.qsv.replace.call_count == 0, (
        "decimal_comma off must not invoke qsv replace — pre-#112 "
        "behavior is a no-op on this code path."
    )
    assert ctx.tmp == original_tmp, (
        "decimal_comma off must not mutate context.tmp; got "
        f"{ctx.tmp!r}, expected {original_tmp!r}."
    )


def test_convert_decimal_comma_uses_issue_112_regex_when_flag_on(tmp_path):
    """``DECIMAL_COMMA=True`` must invoke ``qsv replace`` with the
    anchored regex from issue #112 and route the converted file
    forward via ``context.update_tmp``.
    """
    from ckanext.datapusher_plus.jobs.stages.analysis import AnalysisStage
    from ckanext.datapusher_plus import config as conf

    ctx = _build_context_with_mock_qsv(tmp_path)

    stage = AnalysisStage()
    with mock.patch.object(conf, "DECIMAL_COMMA", True):
        stage._convert_decimal_comma(ctx)

    assert ctx.qsv.replace.call_count == 1, (
        "decimal_comma on must invoke qsv replace exactly once."
    )

    call_kwargs = ctx.qsv.replace.call_args.kwargs
    assert call_kwargs["pattern"] == r"^(-?\d+),(\d+)$", (
        "Regex must be the issue-#112 anchored whole-cell pattern; got "
        f"{call_kwargs['pattern']!r}."
    )
    assert call_kwargs["replacement"] == r"${1}.${2}", (
        "Replacement must use ${1}.${2} so qsv replace emits "
        f"dot-decimals; got {call_kwargs['replacement']!r}."
    )
    assert call_kwargs["not_one"] is True, (
        "not_one must be True so a sample with no comma-decimal cells "
        "doesn't trip qsv's 'no replacements made' non-zero exit."
    )

    # The converted file must be the one routed forward.
    assert ctx.tmp == call_kwargs["output_file"], (
        "context.tmp must point at the qsv-replace output after the "
        f"conversion; got {ctx.tmp!r}, expected "
        f"{call_kwargs['output_file']!r}."
    )


# ---------------------------------------------------------------------------
# End-to-end with real qsv — pins the actual #112 behavior
# ---------------------------------------------------------------------------


def _find_qsv():
    """Return the qsv binary path used by tests, or ``None`` if absent.

    The dpp-test container ships qsv as ``qsvdp`` and sets
    ``QSV_BIN``; locally the binary is usually plain ``qsv``. We honor
    ``QSV_BIN`` first so the test obeys the same configuration the
    production code uses.
    """
    if (env_bin := os.environ.get("QSV_BIN")):
        if Path(env_bin).is_file():
            return env_bin
    for name in ("qsvdp", "qsv"):
        if (found := shutil.which(name)):
            return found
    return None


@pytest.mark.skipif(_find_qsv() is None, reason="qsv binary not on PATH")
def test_end_to_end_german_sample_inference_with_real_qsv(tmp_path):
    """Run the issue-#112 regex replace + ``qsv stats`` on the
    reporter's exact German sample and pin the resulting type
    inferences.

    Without the conversion, all six date-named columns infer as
    ``String``. With it, they infer as ``Float`` — and the legitimate
    string columns (``Kennziffer``, ``Einheit``) stay ``String``
    because the anchored regex doesn't match values like ``GRÜNE`` or
    ``%``. That's the whole contract of issue #112 in one assertion
    pass.
    """
    qsv_bin = _find_qsv()
    src = tmp_path / "german.csv"
    src.write_text(GERMAN_SAMPLE_CSV, encoding="utf-8")

    # Baseline: without conversion, the comma-decimal columns are
    # inferred as String. Pinning this confirms the test fixture
    # actually exercises the issue's failure mode (not a sample that
    # would happen to infer correctly).
    baseline = subprocess.run(
        [qsv_bin, "stats", "--typesonly", str(src)],
        capture_output=True, text=True, check=True,
    )
    baseline_types = dict(
        line.split(",", 1)
        for line in baseline.stdout.strip().splitlines()[1:]  # skip header
    )
    assert all(
        baseline_types[col] == "String"
        for col in ("12.06.1994", "13.06.1999", "13.06.2004",
                    "07.06.2009", "25.05.2014", "26.05.2019")
    ), (
        "Baseline qsv stats should infer all comma-decimal columns as "
        f"String (that's the #112 failure mode); got {baseline_types!r}."
    )

    # Apply the issue-#112 regex replace.
    converted = tmp_path / "german_converted.csv"
    subprocess.run(
        [
            qsv_bin, "replace",
            r"^(-?\d+),(\d+)$",
            r"${1}.${2}",
            str(src),
            "--output", str(converted),
            "--not-one",
        ],
        capture_output=True, text=True, check=True,
    )

    # Re-run inference on the converted file.
    after = subprocess.run(
        [qsv_bin, "stats", "--typesonly", str(converted)],
        capture_output=True, text=True, check=True,
    )
    after_types = dict(
        line.split(",", 1)
        for line in after.stdout.strip().splitlines()[1:]
    )

    # The six previously-String columns must now infer as Float.
    for col in ("12.06.1994", "13.06.1999", "13.06.2004",
                "07.06.2009", "25.05.2014", "26.05.2019"):
        assert after_types[col] == "Float", (
            f"After #112 conversion, column {col!r} should infer as "
            f"Float; got {after_types[col]!r}. Full types: {after_types!r}."
        )

    # The legitimate string columns must NOT be touched — ``GRÜNE``
    # and ``%`` don't match the anchored numeric regex.
    assert after_types["Kennziffer"] == "String", (
        "Kennziffer (values: Wahlbeteiligung, GRÜNE, SPD) must remain "
        f"String; got {after_types['Kennziffer']!r}."
    )
    assert after_types["Einheit"] == "String", (
        f"Einheit (values: %) must remain String; got "
        f"{after_types['Einheit']!r}."
    )
