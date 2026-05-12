# -*- coding: utf-8 -*-
# flake8: noqa: E501
"""
qsv 20.0.0 upgrade regression tests.

Exercises the qsv behaviors that changed between the previous
MINIMUM_QSV_VERSION (4.0.0) and the new floor (20.0.0). These tests shell
out directly to the qsv binary (resolved from the QSV_BIN env var or `qsv` /
`qsvdp` on PATH) rather than through QSVCommand so that they exercise the
qsv contract DP+ depends on without requiring CKAN config bootstrap.

Tests skip if qsv is missing or older than 20.0.0.

Companion to docs/qsv-20.0.0-upgrade-test-plan.md.
"""
from __future__ import annotations

import csv
import json
import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import List, Optional, Tuple

import pytest


REPO_ROOT = Path(__file__).resolve().parent.parent
FIXTURES_DIR = Path(__file__).parent / "static"
REQUIRED_QSV: Tuple[int, int, int] = (20, 0, 0)
SAFE_HEADER_BYTE_CAP = 60  # qsv 20.0.0 contract: safe_header_names ≤ 60 bytes


def _locate_qsv() -> Optional[str]:
    """Find a qsv binary to test against.

    Order: QSV_BIN env var > qsvdp on PATH > qsv on PATH.
    """
    candidates = [
        os.environ.get("QSV_BIN"),
        shutil.which("qsvdp"),
        shutil.which("qsv"),
    ]
    for c in candidates:
        if c and Path(c).is_file():
            return c
    return None


def _qsv_version(binary: str) -> Optional[Tuple[int, int, int]]:
    """Parse `qsv --version` into (major, minor, patch)."""
    try:
        out = subprocess.run(
            [binary, "--version"],
            capture_output=True,
            text=True,
            check=True,
            timeout=5,
        ).stdout
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        return None
    m = re.search(r"(\d+)\.(\d+)\.(\d+)", out)
    if not m:
        return None
    return (int(m.group(1)), int(m.group(2)), int(m.group(3)))


QSV_BIN = _locate_qsv()
QSV_VERSION = _qsv_version(QSV_BIN) if QSV_BIN else None

requires_qsv_20 = pytest.mark.skipif(
    QSV_BIN is None or QSV_VERSION is None or QSV_VERSION < REQUIRED_QSV,
    reason=(
        f"qsv >= {'.'.join(map(str, REQUIRED_QSV))} not available "
        f"(QSV_BIN={QSV_BIN}, version={QSV_VERSION}). "
        "Set QSV_BIN to a qsv binary or add `qsv`/`qsvdp` to PATH."
    ),
)


# ---------------------------------------------------------------------------
# qsv invocation helpers
# ---------------------------------------------------------------------------


def _run(args, **kwargs) -> subprocess.CompletedProcess:
    """Run qsv with the given args; raise on non-zero exit."""
    return subprocess.run(
        [QSV_BIN] + list(args),
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
        **kwargs,
    )


def _safenames_verify_json(fixture: str) -> dict:
    """`qsv safenames <fixture> --mode j` -> parsed JSON."""
    result = _run(["safenames", str(FIXTURES_DIR / fixture), "--mode", "j"])
    return json.loads(result.stdout)


def _safenames_rewrite_headers(
    fixture: str, tmp_path: Path, mode: str = "c"
) -> List[str]:
    """Run `qsv safenames <fixture> --mode <mode> -o <out>` and return the
    rewritten header row.

    Default mode is 'c' (conditional), matching DP+'s production usage at
    ckanext/datapusher_plus/jobs/stages/analysis.py:177. Pass mode='a' to
    force the always-rewrite codepath that exercises `safe_header_names`
    unconditionally (and therefore the v20.0.0 byte-cap fix).
    """
    dst = tmp_path / f"{Path(fixture).stem}.{mode}.safenames.csv"
    _run([
        "safenames", str(FIXTURES_DIR / fixture),
        "--mode", mode,
        "--output", str(dst),
    ])
    return [
        line for line in _run(
            ["headers", "--just-names", str(dst)]
        ).stdout.splitlines() if line
    ]


# ---------------------------------------------------------------------------
# §2.a — safe_header_names byte-cap contract (qsv 20.0.0)
# ---------------------------------------------------------------------------


@requires_qsv_20
class TestSafenamesByteCapV20:
    """qsv 20.0.0 changed `util::safe_header_names` to enforce a 60-BYTE cap
    on the FINAL sanitized name, including any duplicate-disambiguation
    suffix. These tests use `--mode a` (always rewrite) to unconditionally
    exercise that codepath.

    See docs/qsv-20.0.0-upgrade-test-plan.md §2 and qsv 20.0.0 release notes.
    """

    def test_ascii_short_headers_under_always_mode_are_unchanged(self, tmp_path):
        """ASCII-only safe headers without duplicates are stable under --mode a."""
        rewritten = _safenames_rewrite_headers(
            "safenames_ascii_short.csv", tmp_path, mode="a"
        )
        assert rewritten == ["id", "name", "description", "created_at"]

    def test_long_ascii_duplicate_collision_fits_60_bytes(self, tmp_path):
        """Two long ASCII headers that sanitize to the same 60-char string:
        qsv must suffix one with `_<n>` AND keep the final byte length ≤ 60.

        Under qsv ≤ 19.1.0 the suffix was appended AFTER truncation, so the
        result could be 62-63 bytes (over Postgres' NAMEDATALEN=63). Under
        qsv 20.0.0 the truncation accounts for the suffix.

        Observed v20 output: ['the_quick_..._dog_at_noon', 'the_quick_..._dog_at_no_2']
        — second is truncated by 2 chars to make room for the _2 suffix.
        """
        rewritten = _safenames_rewrite_headers(
            "safenames_ascii_duplicate_long.csv", tmp_path, mode="a"
        )
        assert len(rewritten) == 2, rewritten
        for name in rewritten:
            byte_len = len(name.encode("utf-8"))
            assert byte_len <= SAFE_HEADER_BYTE_CAP, (
                f"qsv 20.0.0 contract violated: {name!r} is {byte_len} bytes "
                f"(must be ≤ {SAFE_HEADER_BYTE_CAP})"
            )
        # qsv must have disambiguated the two collisions.
        assert rewritten[0] != rewritten[1], rewritten
        # The disambiguated one must end with `_<n>`.
        assert any(re.search(r"_\d+$", n) for n in rewritten), rewritten

    def test_long_multibyte_header_fits_60_bytes(self, tmp_path):
        """A 69-char accented header (138 UTF-8 bytes) would have produced
        a >60-byte name under qsv ≤19.1.0. qsv 20.0.0 must trim to ≤60 bytes.

        We build this fixture inline since it's exercising a precise byte
        boundary that's awkward to pin in a static CSV.
        """
        src = tmp_path / "long_accent.csv"
        # 69 é characters = 138 UTF-8 bytes when preserved verbatim.
        long_accent = "é" * 69
        src.write_text(f"{long_accent},b\nx,y\n", encoding="utf-8")
        dst = tmp_path / "out.csv"
        _run(["safenames", str(src), "--mode", "c", "--output", str(dst)])
        headers = [
            line for line in _run(
                ["headers", "--just-names", str(dst)]
            ).stdout.splitlines() if line
        ]
        assert len(headers) == 2
        first_byte_len = len(headers[0].encode("utf-8"))
        assert first_byte_len <= SAFE_HEADER_BYTE_CAP, (
            f"qsv 20.0.0 contract violated: {headers[0]!r} is "
            f"{first_byte_len} bytes (must be ≤ {SAFE_HEADER_BYTE_CAP})"
        )

    def test_cjk_headers_under_always_mode_are_postgres_safe(self, tmp_path):
        """Under `--mode a`, CJK headers are sanitized to ASCII (CJK chars
        replaced with `_`, then `unsafe_` prefixed because the result starts
        with `_`). Resulting names must be Postgres-safe and ≤60 bytes.
        """
        rewritten = _safenames_rewrite_headers(
            "safenames_cjk.csv", tmp_path, mode="a"
        )
        assert len(rewritten) == 4
        pg_safe = re.compile(r"^[a-z_][a-z0-9_]*$")
        for name in rewritten:
            assert pg_safe.match(name), (
                f"--mode a CJK rewrite produced non-PG-safe name: {name!r}"
            )
            assert len(name.encode("utf-8")) <= SAFE_HEADER_BYTE_CAP, name


# ---------------------------------------------------------------------------
# §2.b — DP+'s actual usage: conditional mode (--mode c)
# ---------------------------------------------------------------------------


@requires_qsv_20
class TestSafenamesConditionalMode:
    """Documents the actual behavior DP+ relies on at
    ckanext/datapusher_plus/jobs/stages/analysis.py:177 (`mode="conditional"`).

    Conditional mode preserves "quoted identifiers" — headers that are valid
    Postgres identifiers when double-quoted (mixed case, embedded spaces,
    non-ASCII alphabetic characters). It still rewrites obviously-unsafe
    headers (leading digit, special chars, empty) and resolves duplicates.
    """

    def test_conditional_mode_preserves_cjk_as_quoted_identifiers(self, tmp_path):
        """CJK headers are preserved verbatim under --mode c. DP+ will
        therefore create Postgres columns with quoted Unicode identifiers
        like `"顧客識別子"`."""
        rewritten = _safenames_rewrite_headers(
            "safenames_cjk.csv", tmp_path, mode="c"
        )
        assert rewritten == ["顧客識別子", "商品名称", "価格", "登録日時"]

    def test_conditional_mode_preserves_embedded_spaces(self, tmp_path):
        """Long ASCII headers with embedded spaces are preserved under --mode c
        even when they collide with their snake_case equivalents — qsv treats
        them as distinct quoted identifiers."""
        rewritten = _safenames_rewrite_headers(
            "safenames_ascii_duplicate_long.csv", tmp_path, mode="c"
        )
        assert len(rewritten) == 2
        # The space-bearing one must be preserved verbatim.
        assert any(" " in r for r in rewritten), rewritten

    def test_conditional_mode_disambiguates_exact_duplicates(self, tmp_path):
        """Exact-duplicate headers always get `_<n>` suffixes — even in
        conditional mode, even when each individual header is safe."""
        rewritten = _safenames_rewrite_headers(
            "safenames_dupe_suffix.csv", tmp_path, mode="c"
        )
        assert rewritten == ["col", "col_2", "col_3", "col_4"]


# ---------------------------------------------------------------------------
# §2.c — verify-mode (--mode j) JSON output corrections (qsv 20.0.0)
# ---------------------------------------------------------------------------


@requires_qsv_20
class TestSafenamesVerifyModeV20:
    """qsv 20.0.0 corrected three verify-mode behaviors that DP+ touches via
    `json.loads(qsv_safenames.stdout)` at analysis.py:168."""

    def test_verify_mode_trims_surrounding_whitespace_and_quotes(self):
        """unsafe-header strings now have leading/trailing whitespace and
        surrounding `"` already trimmed (matching what the safe-rename pass
        actually evaluates)."""
        verify = _safenames_verify_json("safenames_emoji.csv")
        unsafe = verify.get("unsafe_headers", [])
        assert unsafe, (
            f"emoji/whitespace/quote headers should be flagged unsafe; got {verify!r}"
        )
        for header in unsafe:
            assert header == header.strip(), (
                f"qsv 20.0.0 should have trimmed whitespace: {header!r}"
            )
            assert not (header.startswith('"') and header.endswith('"')), (
                f"qsv 20.0.0 should have trimmed surrounding quotes: {header!r}"
            )

    def test_verify_mode_counts_duplicate_suffix_renames_as_unsafe(self):
        """Verify counts now include header positions that would be renamed
        by the duplicate-suffix pass. For 4 columns all named 'col', at
        least 3 positions must be reported unsafe (positions 2/3/4 are all
        renamed to col_2/col_3/col_4).

        Under qsv ≤ 19.1.0 the verify count was 0 for this input.
        """
        verify = _safenames_verify_json("safenames_dupe_suffix.csv")
        count = verify.get("unsafe_count")
        if count is None:
            count = len(verify.get("unsafe_headers", []))
        assert count >= 3, (
            f"qsv 20.0.0 must report ≥ 3 unsafe positions for 4 duplicate "
            f"'col' headers (got {count}); verify={verify!r}"
        )

    def test_verify_mode_duplicate_headers_sorted_alphabetically(self):
        """duplicate_headers is now sorted alphabetically rather than appearing
        in undefined HashMap iteration order."""
        verify = _safenames_verify_json("safenames_dupe_suffix.csv")
        dup = verify.get("duplicate_headers", [])
        if dup:
            assert dup == sorted(dup), (
                f"qsv 20.0.0 contract: duplicate_headers must be sorted; got {dup}"
            )


# ---------------------------------------------------------------------------
# §3 — stats percentile-label-prefix regression (qsv 12.0.0)
# ---------------------------------------------------------------------------


@requires_qsv_20
class TestStatsPercentilePrefixV12:
    """qsv 12.0.0 changed the `--percentiles` output: the consolidated
    `percentiles` column now emits values as `<percentile>: <value>` separated
    by `|`, e.g. `5: 1|10: 1|40: 2|60: 3|90: 5|95: 5` instead of `1|1|2|3|5|5`.

    DP+ does not pass `--percentiles` by default (SUMMARY_STATS_OPTIONS in
    config.py is unset by default). The risk surface is deployments that set
    `ckanext.datapusher_plus.summary_stats_options = "--percentiles"` AND
    parse the consolidated `percentiles` column in scheming `formula:` /
    `suggest_formula:` expressions.
    """

    @pytest.fixture
    def stats_with_percentiles(self, tmp_path) -> Path:
        out = tmp_path / "stats.csv"
        _run([
            "stats", str(FIXTURES_DIR / "numeric.csv"),
            "--infer-dates", "--dates-whitelist", "all",
            "--cardinality",
            "--percentiles",
            "--output", str(out),
        ])
        return out

    def test_percentiles_column_uses_label_prefix_format(self, stats_with_percentiles: Path):
        """The consolidated `percentiles` column entries are `<N>: <val>`
        pairs joined by `|`."""
        with open(stats_with_percentiles) as f:
            rows = list(csv.DictReader(f))
        price_row = next(r for r in rows if r["field"] == "price")
        assert "percentiles" in price_row, (
            f"qsv stats with --percentiles missing the consolidated column; "
            f"got {list(price_row)}"
        )
        pct_value = price_row["percentiles"]
        assert pct_value, f"percentiles cell is empty: {pct_value!r}"

        entries = pct_value.split("|")
        entry_re = re.compile(r"^\d+(?:\.\d+)?:\s+\S+$")
        for entry in entries:
            assert entry_re.match(entry), (
                f"qsv 12.0.0+ contract: each percentile entry must be "
                f"'<N>: <value>'; got {entry!r} from full value {pct_value!r}"
            )

    def test_default_stats_has_no_percentile_columns(self, tmp_path: Path):
        """When DP+ does NOT pass --percentiles (the default), there is no
        `percentiles` column in the output and no per-column pN labels —
        so the v12.0.0 prefix change has no impact on default deployments."""
        out = tmp_path / "stats.csv"
        _run([
            "stats", str(FIXTURES_DIR / "numeric.csv"),
            "--infer-dates", "--dates-whitelist", "all",
            "--cardinality",
            "--output", str(out),
        ])
        with open(out) as f:
            rows = list(csv.DictReader(f))
        price_row = next(r for r in rows if r["field"] == "price")
        assert "percentiles" not in price_row, list(price_row)
        # Confirm there are no per-percentile columns either (e.g. p25, p50).
        for col in price_row:
            assert not re.fullmatch(r"p\d+", col), col

    def test_non_percentile_numeric_columns_unprefixed(self, tmp_path: Path):
        """min/max/mean/stddev remain bare numeric strings so DP+'s downstream
        `float()` casts at jinja2_helpers.py:99,117,451-453,511-514,837-840
        keep working under qsv 20.0.0."""
        out = tmp_path / "stats.csv"
        _run([
            "stats", str(FIXTURES_DIR / "numeric.csv"),
            "--infer-dates", "--dates-whitelist", "all",
            "--cardinality",
            "--output", str(out),
        ])
        with open(out) as f:
            rows = list(csv.DictReader(f))
        price_row = next(r for r in rows if r["field"] == "price")
        for col in ("min", "max", "mean", "stddev"):
            val = price_row.get(col, "")
            if not val:
                continue
            float(val)  # raises ValueError if v20 ever extends prefix to these


# ---------------------------------------------------------------------------
# §1 — version-gate sanity (pure-Python; doesn't require qsv or CKAN)
# ---------------------------------------------------------------------------


class TestMinimumQsvVersion:
    """Pure-text check that the floor was bumped in config.py. Avoids
    importing `ckanext.*` so the test runs in any env (CI, local) without
    a CKAN bootstrap."""

    def test_minimum_qsv_version_constant_is_20(self):
        config_py = REPO_ROOT / "ckanext" / "datapusher_plus" / "config.py"
        text = config_py.read_text(encoding="utf-8")
        m = re.search(
            r'^MINIMUM_QSV_VERSION\s*=\s*"([^"]+)"',
            text,
            re.MULTILINE,
        )
        assert m, f"MINIMUM_QSV_VERSION constant not found in {config_py}"
        assert m.group(1) == "20.0.0", (
            f"MINIMUM_QSV_VERSION must be '20.0.0' (got {m.group(1)!r})"
        )
