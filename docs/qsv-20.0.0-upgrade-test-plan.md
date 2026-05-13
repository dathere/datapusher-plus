# qsv 20.0.0 Upgrade — DP+ Regression Test Plan

This plan covers the validation steps for bumping `MINIMUM_QSV_VERSION` from `4.0.0` to `20.0.0` (see `CHANGELOG.md` → `[Unreleased]`). It exercises the qsv command surface area that DP+ uses and the specific edge cases changed by qsv releases 5.0.0 → 20.0.0.

## Scope

The qsv command surface DP+ depends on (per `ckanext/datapusher_plus/qsv_utils.py`):

`excel`, `geoconvert`, `geocode`, `input`, `validate`, `sortcheck`, `extdedup`, `headers`, `safenames`, `index`, `count`, `stats`, `frequency`, `slice`, `datefmt`, `searchset`.

Code paths exercised (v2.0 pipeline):
- `ckanext/datapusher_plus/jobs/stages/format_converter.py` — `excel`, `geoconvert`, `input`
- `ckanext/datapusher_plus/jobs/stages/validation.py` — `validate`, `sortcheck`, `extdedup`
- `ckanext/datapusher_plus/jobs/stages/analysis.py` — `headers`, `safenames`, `index`, `count`, `stats`, `frequency`, `slice`, `datefmt`
- `ckanext/datapusher_plus/jobs/stages/metadata.py` — `stats`
- `ckanext/datapusher_plus/pii_screening.py` — `searchset`, `stats`

## Test environment

- Fresh CKAN 2.10+ instance with `ckanext-datapusher-plus` installed from this branch
- `ckanext.datapusher_plus.qsv_bin` pointing at `qsv 20.0.0` (verify with `qsv --version`)
- Empty datastore database
- Redis Queue worker running
- A baseline environment with `qsv 19.1.0` available for differential comparison (recommended: parallel container)

---

## 1. Version-gate sanity

| # | Check | Expected |
|---|-------|----------|
| 1.1 | Start CKAN with `qsv_bin` pointing at `qsv 19.1.0` | DP+ refuses to start; `JobError: At least qsv version 20.0.0 required. Found 19.1.0.` |
| 1.2 | Start CKAN with `qsv_bin` pointing at `qsv 20.0.0` | DP+ starts cleanly; `qsv version found: 20.0.0` in logs |
| 1.3 | Start CKAN with `qsv_bin` pointing at `qsv 21.0.0` (when available) | DP+ starts cleanly (no upper bound) |

Code under test: `qsv_utils.py:154-175` (`check_version`).

---

## 2. `safenames` — the high-risk path (qsv 20.0.0 byte-cap change)

This is the single most likely place for behavioural drift. Test with the four header categories the qsv 20.0.0 byte-cap change differentiates.

Build the following fixture CSV files (header row only; one data row of dummy values is sufficient for ingestion):

**`fixtures/safenames_ascii_short.csv`**
```
id,name,description,created_at
1,foo,bar,2026-01-01
```
Expected sanitized headers: `id,name,description,created_at` (no change).

**`fixtures/safenames_ascii_duplicate_long.csv`**
A header containing two columns whose sanitized form lands at exactly 60 chars, then duplicates. For example:
```
the_quick_brown_fox_jumps_over_the_lazy_sleeping_dog_at_noon,the quick brown fox jumps over the lazy sleeping dog at noon
```
Under qsv ≤ 19.1.0, the second column would become `the_quick_brown_fox_jumps_over_the_lazy_sleeping_dog_at_noon_2` (63 chars; over the Postgres limit only after suffixing).
Under qsv 20.0.0, the suffix is applied *before* the 60-byte truncation, so the second column lands at ≤ 60 bytes (e.g. `the_quick_brown_fox_jumps_over_the_lazy_sleeping_dog_at_no_2`).

**`fixtures/safenames_cjk.csv`**
```
顧客識別子,商品名称,価格,登録日時
1,foo,1000,2026-01-01
```
Each CJK char is 3 bytes in UTF-8. Under qsv ≤ 19.1.0, these would pass through to the rewrite step and could yield names > 60 bytes if combined with the unsafe-prefix. Under qsv 20.0.0, the prefix-then-truncate-then-char-boundary path produces ≤ 60-byte names.

**`fixtures/safenames_emoji.csv`**
```
🦀 column,🚀 column,column with leading whitespace ,"column ""with"" quotes"
1,2,3,4
```
Tests the v20.0.0 verify-mode trim corrections (`"`/whitespace stripped from `unsafe_headers` strings).

**`fixtures/safenames_dupe_suffix.csv`**
```
col,col,col,col
1,2,3,4
```
Verify-mode count must now report **3** unsafe headers (positions 2, 3, 4 — all renamed by the duplicate-suffix pass), not 0 as in qsv ≤ 19.1.0.

### Test matrix

> [!NOTE]
> DP+'s production path calls `safenames --mode conditional` (see `ckanext/datapusher_plus/jobs/stages/analysis.py:177`), which **preserves "quoted identifier"-safe headers verbatim** — including embedded spaces and non-ASCII alphabetic chars (CJK, accented Latin alphabetic forms). Only headers with truly-unsafe chars (emoji, leading digits, special chars) or exact duplicates get rewritten. The `safe_header_names` byte-cap still applies *when* a rewrite triggers. The qsv contract behaviors (always-rewrite path, byte-cap, etc.) are pinned in `tests/test_qsv_v20_regression.py` — this manual matrix documents what an operator sees end-to-end through DP+'s default path.
>
> Rows below note `[mode c]` (DP+ default) or `[mode a]` (always-rewrite, exercised only by the regression suite, not the production pipeline) where it matters.

| # | Fixture | Action | Expected (under DP+'s `--mode c` path) |
|---|---------|--------|----------|
| 2.1 | `safenames_ascii_short.csv` | Upload via DRUF / submit via `ckan datapusher_plus submit` | Datastore table has columns `id,name,description,created_at`; no log line saying unsafe headers found |
| 2.2 | `safenames_ascii_duplicate_long.csv` | Upload | Datastore table has 2 columns, both preserved verbatim as quoted identifiers (snake_case form + space-separated form coexist — qsv treats them as distinct). Log line "0 unsafe header names found" because both forms are valid quoted PG identifiers. (Under `--mode a` the second column would collide with the first and get `_<n>`-suffixed at ≤ 60 bytes — pinned by `test_long_ascii_duplicate_collision_fits_60_bytes`.) |
| 2.3 | `safenames_cjk.csv` | Upload | Datastore columns are quoted Unicode identifiers (e.g. `"顧客識別子"`, `"商品名称"`, etc.) — CJK preserved verbatim. Log line "0 unsafe header names found". (Under `--mode a` the same headers would be rewritten to `unsafe_*` ASCII forms ≤ 60 bytes — pinned by `test_cjk_headers_under_always_mode_are_postgres_safe`.) |
| 2.4 | `safenames_emoji.csv` | Upload | Mixed outcome: emoji headers → `unsafe_*` rewrite; trailing-whitespace header → trimmed-and-preserved as `whitespace`; literal-quote-wrapped header → trimmed-and-preserved. DP+ logs `unsafe_headers` (verify-mode JSON) with leading/trailing whitespace and surrounding `"` already trimmed from the displayed strings (qsv 20.0.0 contract — pinned by `test_verify_mode_trims_surrounding_whitespace_and_quotes`). |
| 2.5 | `safenames_dupe_suffix.csv` | Upload | Datastore columns are `col, col_2, col_3, col_4` (qsv suffixes duplicates even in conditional mode). DP+ logs **3** unsafe header names found — positions 2/3/4 (qsv 20.0.0 count-correction vs. ≤ 19.1.0 which reported 0 — pinned by `test_verify_mode_counts_duplicate_suffix_renames_as_unsafe`). |

### Differential check (recommended)

For 2.2, 2.3, 2.4, 2.5: ingest the same fixture under qsv 19.1.0 in a parallel environment, dump the resulting `_pg_attribute` for the datastore table, and diff against the qsv 20.0.0 result. Document every column-name delta in the upgrade notes.

---

## 3. `stats` — percentile-label-prefix regression (qsv 12.0.0)

DP+ does not pass `--percentiles` by default (`SUMMARY_STATS_OPTIONS` in `config.py` is unset). Default deployments are unaffected by the v12.0.0 change. This section verifies that **and** exercises the new consolidated-column format for deployments that have explicitly opted into `--percentiles` via `ckanext.datapusher_plus.summary_stats_options`.

**`fixtures/numeric.csv`** (any CSV with a numeric column):
```
id,price
1,10.0
2,20.0
3,30.0
4,40.0
5,50.0
```

| # | Action | Expected |
|---|--------|----------|
| 3.1 | Run `qsv stats --infer-dates --dates-whitelist all --cardinality --output qsv_stats.csv fixtures/numeric.csv` (no `--percentiles`) | Output has no `percentiles` column and no `p<N>` columns; numeric columns (`min`, `max`, `mean`, `stddev`) are bare values like `"30.0"` — `float()`-parseable as-is |
| 3.2 | Run the same with `--percentiles` appended | Output has a single `percentiles` column with value of the form `"5: 1|10: 1|40: 2|60: 3|90: 5|95: 5"` — pipe-delimited entries, each prefixed with `<N>: ` (this is the v12.0.0 change) |
| 3.3 | Submit `fixtures/numeric.csv` to a default DP+ install | Job succeeds; no `ValueError: could not convert string to float` errors in worker logs; datastore table populated correctly |
| 3.4 | Set `ckanext.datapusher_plus.summary_stats_options = "--percentiles"` in `ckan.ini` and resubmit | Job succeeds; the `percentiles` consolidated column appears in qsv stats output; verify any scheming formulas that parse it cope with the new `<N>: ` prefix per entry |

### Companion audit (one-shot, before merging)

If you've configured `summary_stats_options` to enable percentiles, search downstream scheming YAMLs for references to `percentiles`:
```bash
grep -rn "percentiles" /path/to/ckan/scheming/*.yaml
```
Document any matches that parse the consolidated value as bare numbers and update the parsing to strip the `<N>: ` prefix from each `|`-delimited entry.

---

## 4. End-to-end smoke tests

Run a full ingestion of each fixture below under both qsv 19.1.0 (baseline) and qsv 20.0.0 (this branch). The pipeline must complete without errors and produce equivalent datastore tables (column types, row counts, sample rows).

| # | Fixture | Coverage |
|---|---------|----------|
| 4.1 | Small CSV (< 100 rows, mixed types) | `validate`, `sortcheck`, `extdedup`, `headers`, `safenames`, `stats`, `frequency`, `slice`, `datefmt`, `index`, `count` |
| 4.2 | Larger CSV (> 1M rows) | qsv 11.0.2 dynamic parallel chunk sizing path; verify no OOM, sane wall time |
| 4.3 | XLSX with multiple sheets | `excel --sheet N --trim`; default sheet 0 + an alternate sheet via `ckanext.datapusher_plus.preview_rows` or override |
| 4.4 | ODS file | `excel` (calamine 0.32 path from qsv 10.0.0) |
| 4.5 | Shapefile (.zip) | `geoconvert` shp → CSV |
| 4.6 | GeoJSON | `geoconvert` geojson → CSV |
| 4.7 | CSV with bad encoding (e.g. Windows-1252) | `input --trim-headers` + encoding detection in `validation.py` |
| 4.8 | CSV with dates in DMY and MDY mix | `stats --infer-dates --prefer-dmy` |
| 4.9 | CSV containing PII (SSN-pattern column) | `pii_screening.py` → `searchset` + `stats` |
| 4.10 | CSV failing RFC-4180 validation | `validate` raises `JobError`; job marked failed |

### Negative cases

| # | Action | Expected |
|---|--------|----------|
| 4.11 | Submit Excel file with `--sheet -4` against a 3-sheet workbook | qsv 20.0.0 errors with `negative sheet index -4 is out of range for 3 sheets`; DP+ surfaces this as `JobError` (qsv ≤ 19.1.0 silently returned the wrong sheet) |
| 4.12 | Submit Excel file with an empty sheet selected | qsv 20.0.0 errors with "sheet is empty" instead of the misleading "larger than sheet" message |

---

## 5. Re-ingestion / schema-drift checks (the operator pain point)

This validates the migration story for production deployments that have already ingested resources with multibyte or long-duplicate headers under older qsv.

| # | Setup | Action | Expected |
|---|-------|--------|----------|
| 5.1 | Ingest `safenames_cjk.csv` under qsv 19.1.0; record the resulting Postgres column names | Upgrade qsv binary in place to 20.0.0, resubmit the same resource via `ckan datapusher_plus resubmit -y` | DP+ either (a) succeeds with the new (shorter) column names and rebuilds the table, or (b) fails with a clear `column name mismatch` error and rolls back. **Document which behaviour occurs** — this dictates the operator runbook |
| 5.2 | Same as 5.1 but for `safenames_ascii_duplicate_long.csv` | Resubmit | Same expectation; capture the column-name delta |
| 5.3 | Same as 5.1 but for an ASCII-only resource with no duplicates | Resubmit | No-op delta; resubmit completes with byte-identical column names |

If 5.1/5.2 result in failure rather than a clean rebuild, decide whether to:
- ship a one-shot CLI command (`ckan datapusher_plus rename-columns-for-qsv20`) that pre-renames datastore columns to match the new sanitization, **or**
- document a drop-and-resubmit operator runbook in the release notes.

---

## 6. CI

Update `.github/workflows/main.yml` to install qsv 20.0.0 (instead of whatever version it currently pins). Verify the full integration matrix passes.

---

## 7. Documentation

- [x] `CHANGELOG.md` `[Unreleased]` entry (this PR)
- [x] `README.md` install snippet bumped to qsv 20.0.0 (this PR)
- [ ] User-facing 3.0.0 release notes call out the `dpps.<field>.p50` formula-audit step
- [ ] Operator runbook for re-ingestion (depends on outcome of section 5)

---

## Sign-off checklist

Before merging, confirm:

- [ ] All tests in sections 1–4 pass against qsv 20.0.0
- [ ] Section 5 outcomes documented; operator runbook drafted if needed
- [ ] CI green on the bump commit
- [ ] No regressions in `pytest tests/` (existing unit + acceptance + mocked tests)
- [ ] Manual smoke of DRUF flow with at least one fixture from each of §2 and §4
