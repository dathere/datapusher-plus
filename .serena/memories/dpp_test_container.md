# `dpp-test` — pre-built ckan-dev container for DP+ work

A persistent Docker container kept around so DP+ **unit** tests/builds can
run without a local CKAN env. **Reuse it for future DP+ work** instead of
rebuilding.

> For **integration** tests, use the docker-compose stack via
> `scripts/integration-up` instead — see "Integration stack" section
> at the bottom. The `dpp-test` container is unit-only because the
> integration suite needs Prefect, Postgres, Redis, Solr, and the
> separately-built worker image.

## What it is
- Name: `dpp-test`
- Image: `ckan/ckan-dev:2.11`, started with `--platform linux/amd64`
  (host is Apple Silicon → amd64 is emulated, so it's slowish).
- Started with `--user root ... sleep infinity` (image entrypoint bypassed).
- Repo mounted **live** at `/repo` (host edits are visible immediately;
  `git checkout` on the host changes what the container sees).
- CKAN 2.11.5, Python 3.10.20.

## What's installed (took ~5 min, mirrors `.github/workflows/ci.yml`)
- Geo system libs: `gdal-bin libgdal-dev libspatialindex-dev libgeos-dev
  libproj-dev` + `build-essential` etc.
- GDAL python 3.6.2, `requirements.txt` + `requirements-dev.txt`,
  `pip install -e .` (→ `datapusher-plus 3.0.0a0`, editable).
- qsv 20.1.0 at `/usr/local/bin/qsvdp` (bumped from 20.0.0 in PR #315 —
  no breaking changes per qsv 20.1.0 release notes; pipelines built
  against 20.0.0 upgrade in place).
- `b3sum` CLI at `/usr/local/bin/b3sum` (added by PR #309 for the
  configurable file-hash feature — required by tests that exercise the
  `blake3` algorithm via the external `b3sum` binary path).
- `babel>=2.9` (declared explicitly in `requirements.txt` since PR #320
  — DP+ now imports `babel.numbers` directly for locale-aware number
  parsing in `AnalysisStage._normalize_locale_numbers`).
- `importlib_metadata>=4.6` (declared explicitly in `requirements.txt`
  since PR #323 — prefect 3.7.1's `workers/base.py` imports it
  unconditionally but doesn't list it as a direct dep; it was satisfied
  transitively by `opentelemetry-api` until 1.42.0 dropped that. See
  "Integration stack" note below.)

## Run the unit suite
```bash
docker exec -e PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 -e QSV_BIN=/usr/local/bin/qsvdp \
  -e CKAN_INI=/srv/app/src/ckan/test-core.ini -w /repo dpp-test \
  python3 -m pytest tests/ --ignore=tests/integration -o addopts= -p no:cacheprovider -q
```
Required env, and why:
- `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1` — ckan-dev's site-packages registers a
  pytest plugin that calls `make_app()` in `pytest_sessionstart`, needing a
  fully-configured CKAN. Disable autoload so plain pytest runs.
- `QSV_BIN=/usr/local/bin/qsvdp` — for `test_qsv_v20_regression.py`,
  `test_issue_173_date_format_inference.py`, and
  `test_issue_112_decimal_comma.py::test_end_to_end_german_sample_inference_with_real_qsv`.
- `CKAN_INI=/srv/app/src/ckan/test-core.ini` — the image's default
  `/srv/app/ckan.ini` is NOT populated when the entrypoint is bypassed;
  `test-core.ini` has a real `SECRET_KEY`.
- `-o addopts=` — overrides `setup.cfg`'s `--pdbcls=IPython...` addopt.
- `tests/integration/` excluded — needs the integration stack (see below).

> The full-suite tail prints noisy Prefect server-shutdown logging
> (`ValueError: I/O operation on closed file` from
> `prefect/logging/handlers.py`). Cosmetic — not a test failure. Grep
> the summary line: `... 2>&1 | grep -E "^[0-9]+ passed|^[0-9]+ failed"`.

## Known result (as of 2026-05-20, `main` @ `5bac296`)

**277/277 Python unit tests pass** on `main` after the #323 → #324 arc.

Test count history:
- 218 → 251 over the 2026-05-17 → 2026-05-19 arc (#299 → #322, net **+33**).
- 251 → 277 over the 2026-05-19 → 2026-05-20 arc (#323 → #324, net **+26**).

The breakdown isn't a simple per-file sum because refactors also removed or
consolidated pre-existing tests. Reproducible via
`pytest tests/ --ignore=tests/integration --collect-only -q`.

New-file gross additions, #299 → #322 arc:
- `tests/test_dictionary_stash.py` — 21 tests (PR #307)
- `tests/test_utcnow_naive.py` — 4 tests (PR #308)
- `tests/test_file_hash_algorithm.py` — 10 tests (PR #309)
- `tests/test_metadata_hash_persistence.py` — 3 tests (PR #312, regression for #310)
- `tests/test_rehydrate_resource_identity.py` — 4 tests (PR #313, regression for #311)
- `tests/test_date_without_timestamp.py` — 5 tests (PR #314, regression for #179)
- `tests/test_issue_173_date_format_inference.py` — 4 tests (PR #315, regression for #173)
- `tests/test_issue_111_version_in_log.py` — 5 tests (PR #316, regression for #111)
- `tests/test_issue_142_auto_index_threshold.py` — 8 tests (PR #317, regression for #142)
- `tests/test_issue_112_decimal_comma.py` — 11 tests (PR #320, regression for #112)
- `tests/test_issue_261_empty_date_range.py` — 6 tests (PR #322, regression for #261)

New-file gross additions, #323 → #324 arc:
- `tests/test_issue_61_download_always_whitelist.py` — 23 tests (PR #323, feature #61)
- `tests/test_pii_screening_config_key.py` — 3 tests (PR #324, regression for the
  `pii_screening` config-key typo found by the documentation audit)

The JS unit suite (Vitest + jsdom, PR #304) is unchanged at **12 tests**
for `scheming-ai-suggestions.js`. Runs on the host, not in `dpp-test`:
```bash
npm install                     # first time only
npx vitest run                  # one-shot
npx vitest                      # watch mode
```
`vitest.config.js` is scoped to `tests/js/**/*.test.js`. Setup at
`tests/js/setup.js` loads jQuery into the jsdom realm via `fs + new
Function` (the ESM `import jquery from 'jquery'` shape returns the
namespace, not the callable selector) and captures `originalAjax = $.ajax`
at module scope so `beforeEach` can restore it between tests. The SUT
itself is loaded fresh per test via the same `new Function` trick so
module registration state doesn't bleed.

The legacy `scheming-suggestions.js` (NOT the `-ai-` variant) still has
no JS test coverage. PR #322 changes there are validated only by Python
regression tests + manual screenshots in the issue thread.

## If the container is gone (Docker restart / removed)
- Restart: `docker start dpp-test` (state persists across stops).
- Recreate: `docker run -d --name dpp-test --platform linux/amd64 --user root
  -v <repo>:/repo -w /repo ckan/ckan-dev:2.11 sleep infinity`, then re-run
  the `ci.yml`-style install (apt geo libs → `pip install GDAL==$(gdal-config
  --version)` → `pip install -r requirements.txt -r requirements-dev.txt -e .`
  → download qsv 20.1.0 musl zip → `qsvdp` to `/usr/local/bin/` → download
  `b3sum` musl binary to `/usr/local/bin/` and `chmod +x`).

## Integration stack (separate from `dpp-test`)

PR #300 landed `scripts/integration-up` / `scripts/integration-down` to
manage the full docker-compose integration stack
(`docker-compose.integration.yaml`):

- **Worker image**: built from `Dockerfile.worker`, now based on
  `--platform=linux/amd64 ckan/ckan-dev:2.11` (same base as the CKAN
  service, and same as `dpp-test`). The earlier `prefecthq/prefect:3-latest`
  base crashed flow runs with `ModuleNotFoundError: No module named 'ckan'`
  because DP+'s flow code does an import-time
  `_bootstrap_ckan_app_context()` → `make_app()`.
- **Bring it up**: `scripts/integration-up` (idempotent, ~5–8 min cold,
  ~30 s warm). Auto-detects cold start vs. post-restart vs.
  post-down-with-volume-kept and does the right thing.
- **After edits to `Dockerfile.worker`**: `scripts/integration-up --rebuild`
  forces a `--no-cache` rebuild of the worker image. Plain
  `scripts/integration-up` will re-use the cached worker layer.
- **Token**: `scripts/integration-up` writes the admin JWT to
  `./.integration-token` (gitignored, chmod 600). `conftest.py` reads it
  automatically when `CKAN_API_KEY` is unset — keeps the JWT out of the
  process command line.
- **Run integration tests**:
  `INTEGRATION=1 CKAN_URL=http://localhost:5050 pytest tests/integration/ -v`
- **Tear down**: `scripts/integration-down` (keeps postgres volume),
  `scripts/integration-down --wipe` (nukes everything).

The integration stack and the `dpp-test` container can co-exist; they
don't share ports (`dpp-test` doesn't publish any). Use `dpp-test` for
quick unit-test iteration; use the integration stack only when you need
a real end-to-end flow run.

> **Prefect-worker `importlib_metadata` gotcha (fixed PR #323)**: on
> 2026-05-19 the "DataPusher+ Integration CI" workflow started failing
> at the "Start Prefect worker" step with `ModuleNotFoundError: No
> module named 'importlib_metadata'`. Root cause: prefect 3.7.1's
> `workers/base.py` imports `importlib_metadata` but doesn't declare
> it; `opentelemetry-api` 1.42.0 (released that morning) dropped the
> transitive that had been satisfying it. Fixed by pinning
> `importlib_metadata>=4.6` directly in `requirements.txt`. If you
> rebuild the worker image or `dpp-test`, the pin handles it.

> **2026-05-19 stack state** (may be stale — re-check): the
> `prefect-server`, `prefect-worker`, `postgres`, `redis`, `solr`
> containers were healthy/Up, but the **CKAN service itself was
> stopped or crashed**. Run `scripts/integration-up` to bring CKAN
> back before any integration work.

## Test patterns established (worth following)

1. **AST-parse `config.py`, YAML-parse `config_declaration.yaml`** — a
   recurring drift-guard pattern. `test_issue_142_auto_index_threshold.py`,
   `test_issue_112_decimal_comma.py`, `test_issue_61_download_always_whitelist.py`,
   and `test_pii_screening_config_key.py` all walk the source files to
   confirm inline fallbacks and declaration defaults/keys agree. Catches
   #179-style declaration-vs-code drift without the CKAN bootstrap.
   When YAML-parsing the declaration, **iterate all `groups`/`options`**
   to find the key by name (don't index `groups[0]` — brittle if the
   declaration is restructured; Copilot caught this on PR #323). Guard
   `import yaml` with `try/except ImportError → pytest.skip(...)` so
   dep-light CI degrades cleanly.
2. **Bypass `FormulaProcessor.__init__` via `__new__`** — its constructor
   does heavy lat/lon/date inference + pulls CKAN config. For unit
   tests of `process_formulae` (e.g. `test_issue_261_empty_date_range.py`),
   build the object via `FormulaProcessor.__new__(FormulaProcessor)` and
   set attributes directly. Same pattern as
   `test_security.test_formula_processor_uses_sandboxed_environment`.
3. **End-to-end with the real qsv binary** — locale-aware number
   normalization (PR #320) and date-format inference (#173) both have
   tests that shell out to qsv with the actual sample data and assert
   on `qsv stats --typesonly`. `pytest.mark.skipif` on missing `QSV_BIN`
   keeps the suite portable.
4. **Pin asymmetry, not just behavior** — the #322 gating test feeds the
   SAME `{{ none }}` template through `formula_type="formula"` and
   `"suggestion_formula"` and asserts they differ. The #61 tests pin
   subdomain non-matching in BOTH directions (parent doesn't match
   sub, sub doesn't match parent) so a future "support wildcards"
   change breaks a test rather than silently changing semantics.
5. **Anchor test fixtures to actual schema strings** — Copilot caught
   on PR #322 that `SUGGEST_FORMULA = "suggest_formula"` was a phantom
   — the production key is `"suggestion_formula"`. Verify constants
   against actual usage in `dataset-druf.yaml`, `docs/dataset_schema.yaml`,
   `jobs/stages/formula.py`.
6. **Live-config-read beats `importlib.reload` for `editable: true`
   keys** — for config that should honor admin-UI runtime edits, read
   `tk.config` live in a small helper (`_get_file_hasher` #221,
   `_get_download_always_whitelist` #61) rather than snapshotting into
   a module-level constant. Tests then just `monkeypatch.setitem(
   tk.config, ...)` and call — no reload, no state leak.
7. **Pair `importlib.reload(config)` with an autouse teardown reload**
   — when a constant genuinely must be import-time (e.g. `PII_SCREENING`),
   a test that reloads `config` to recompute it leaks the monkeypatched
   snapshot into later tests (`reload` mutates the module in place;
   `monkeypatch` restores `tk.config` but doesn't re-run `config.py`).
   Fix: an `autouse=True` fixture that reloads `config` on teardown.
   Because autouse fixtures set up before the test's `monkeypatch`,
   their finalizer runs AFTER `monkeypatch` restores `tk.config` — so
   the teardown reload recomputes from pristine config. See
   `test_pii_screening_config_key.py::_reset_config_module`. (Copilot
   caught the leak on PR #324.)

## DOWNLOAD_ALWAYS_WHITELIST (PR #323, issue #61)

`ckanext.datapusher_plus.download_always_whitelist` — a
whitespace-separated list of hostnames whose resources always get
re-downloaded + re-analyzed, bypassing the file-hash upload-skip
optimization in `DownloadStage._should_skip_upload`. For hosts that
update content in place without changing the byte hash, or local/peered
hosts where re-download is cheap.

- Issue #61 (2023) originally paired this with a never-built
  `DOWNLOAD_PREVIEW_ONLY` partial-download mode; DP+'s architecture is
  full-file download for comprehensive metadata inference, so that
  parent is moot. The name was kept, semantics repurposed as a
  re-processing trigger.
- `_get_download_always_whitelist()` reads `tk.config` live (mirrors
  `_get_file_hasher`), parses into a `frozenset` of lowercased hosts.
- `_host_in_always_whitelist(url)` returns `Optional[str]` — the matched
  lowercased host (port stripped) or `None`. Returning the host (not a
  bool) lets `_should_skip_upload` log it without a second `urlparse`.
- Matching is **exact-hostname**, case-insensitive — `data.gov` does NOT
  match `subdomain.data.gov`.
- 23 tests in `tests/test_issue_61_download_always_whitelist.py`.

## pii_screening config-key typo (PR #324, documentation audit)

A documentation audit (2026-05-19) found `config.py` read
`PII_SCREENING` from `ckanext.datastore_plus.pii_screening` — a typo
(`datastore_plus` ≠ `datapusher_plus`). The documented key never
populated `PII_SCREENING`; PII screening couldn't be enabled as
documented. Fixed in `config.py`; 3 regression tests in
`tests/test_pii_screening_config_key.py` (documented key works, old
typo'd key has no effect, AST drift-guard).

PR #324 also carried doc-audit corrections: `preview_rows` `config.py`
fallback aligned `"1000"` → `"0"` to match `config_declaration.yaml`
(behavior-preserving — the declaration default already wins under CKAN
2.10+ declarative config); stale `describeGPT_api_key` README line
removed; `formats` README example typo (`xlsxb`/`xlsm`) dropped to
match `config.py`'s FORMATS default; CONFIG.md DRUF template-override
list completed (3 → 5); illustrative-values notes added above the
README ckan.ini example blocks. Still-open for a maintainer pass: the
README example blocks have other stale values (`auto_index_threshold = 3`
should be 10 per #142, `chunk_size = 16384` should be 1048576).

## AI suggestions feature (PRs #301 → #304)

End-to-end shape of the AI suggestions feature now on `main`:

1. **Backend** — `jobs/stages/ai_suggestions.py` calls
   `QSVCommand.describegpt()` (`qsv_utils.py`) which shells out to
   `qsv describegpt --format JSON --api-key NONE --base-url <url>
   --model <model>`. `--format JSON` (not `--json`) and `--api-key NONE`
   for non-localhost endpoints are both real `qsv` requirements caught
   by E2E with LM Studio (`host.docker.internal:1234/v1`).
2. **Envelope** — qsv emits a PascalCase wrapped envelope
   `{Dictionary, Description, Tags}` (each wrapped in `response` /
   `reasoning` / `token_usage`). `_reshape_for_ui` walks it and writes
   per-field `ai_suggestions[fieldName] = {value, source}` plus
   `STATUS=DONE` for polling termination. `_RESERVED_AI_KEYS` blocks
   column names that would collide with the envelope keys.
3. **Polling JS** — `assets/js/scheming-ai-suggestions.js` reads
   `package_show` and polls until
   `dpp_suggestions.ai_suggestions.STATUS` is in
   `['DONE', 'ERROR', 'FAILED']`. Production bug caught by JS tests:
   the JS originally read STATUS from `dpp_suggestions.STATUS` (top
   level) — the stage writes it nested inside `ai_suggestions`.
4. **Fixture** — `tests/fixtures/qsv_describegpt_sample.json` is a real
   captured response from a LM Studio gemma-4-e4b run; use it for
   shape-of-envelope assertions in Python tests.

## Empty-suggestion handling (PR #322, issue #261)

DRUF (the legacy `scheming-suggestions.js`, NOT the AI-suggestions
variant) handles null/empty/whitespace suggestion values:

1. **Python — `FormulaProcessor.process_formulae`** coerces stringified
   `"None"` / `""` / whitespace-only renders back to Python `None`.
   Coercion is **gated on `formula_type == "suggestion_formula"`** —
   direct `formula` fields write straight into the package/resource
   dict and must preserve verbatim output for CKAN's validators.
2. **JS — `scheming-suggestions.js`** greys out the per-field button
   for null/empty values + re-enables on re-render.
3. **CSS — `suggestions.css`** has a `.suggestion-btn-disabled` rule.

Cherry-picked from Minhajuddin's orphan commit `62c18ea`; 6 Python
regression tests in `tests/test_issue_261_empty_date_range.py`.

## Locale-aware number parsing (PR #320, issue #112)

DP+ supports parsing comma-decimal / locale-specific number formats
opt-in. Resolution order per resource: `dpp_locale` resource field →
`conf.DEFAULT_LOCALE` → `conf.DECIMAL_SEPARATOR` → no-op. Babel path
uses `babel.numbers.parse_decimal(value, locale=id, strict=True)` —
`strict=True` is critical (without it German `12.06.1994` silently
coerces to `Decimal('12061994')`). 11 regression tests in
`tests/test_issue_112_decimal_comma.py`.

## Data Dictionary stash/restore (PR #307, issue #265)

`dictionary_stash.py` is the on-disk persistence layer; `_parse_stats`
stashes existing column `info` before delete, rollback/retry paths
restore from the stash, finally clears. 21 tests in
`test_dictionary_stash.py`.

## UTC timestamps (PR #308, issue #145)

`utils.utcnow_naive()` is the single helper for every persisted
timestamp; 4 regression tests in `test_utcnow_naive.py`.

## Configurable file-hash algorithm (PR #309, issue #221)

`blake3` (default), `sha256`, `md5` selectable via
`ckanext.datapusher_plus.file_hash_algorithm`. Read live from
`tk.config` by `_get_file_hasher` (the canonical live-read pattern).
`blake3` uses the external `b3sum` CLI (installed in worker + CI +
`dpp-test`).

## qsv-related arc (PRs #314 / #315, issues #173 / #179)

PR #314 fixed the `Date → date` mapping (was wrongly stored as
`timestamp`); PR #315 bumped qsv pin to 20.1.0 with regression
coverage. Some qsv 20.1.0 inference gaps documented (DD-MM-YYYY →
String, Unix epoch → Integer).

## Auto-index threshold (PRs #317 / #318, issue #142)

`AUTO_INDEX_THRESHOLD` is a CLOSED range `[AUTO_INDEX_MIN_THRESHOLD,
AUTO_INDEX_THRESHOLD]`, defaults `[3, 10]`. Single-value columns no
longer get useless 10-40 MB indexes. 8 regression tests in
`tests/test_issue_142_auto_index_threshold.py`.

## Version banner in flow log (PR #316, issue #111)

The "JOB DONE!" capstone log line surfaces the DP+ version from
`pyproject.toml`. 5 regression tests in `tests/test_issue_111_version_in_log.py`.
