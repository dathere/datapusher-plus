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

## What's installed (took ~5 min, mirrors `.github/workflows/test.yml`)
- Geo system libs: `gdal-bin libgdal-dev libspatialindex-dev libgeos-dev
  libproj-dev` + `build-essential` etc.
- GDAL python pinned to `gdal-config --version`, `requirements.txt` +
  `requirements-dev.txt`, `pip install -e .` (→ `datapusher-plus 3.0.0a0`,
  editable).
- qsv 20.1.0 at `/usr/local/bin/qsvdp` (bumped from 20.0.0 in PR #315 —
  no breaking changes per qsv 20.1.0 release notes; pipelines built
  against 20.0.0 upgrade in place).
- `b3sum` CLI at `/usr/local/bin/b3sum` (added by PR #309 for the
  configurable file-hash feature — required by tests that exercise the
  `blake3` algorithm via the external `b3sum` binary path).
- `babel>=2.9` (declared explicitly in `requirements.txt` since PR #320 —
  DP+ now imports `babel.numbers` directly for locale-aware number
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
- `-o addopts=` — overrides `pyproject.toml`'s `[tool.pytest.ini_options]`
  `--pdbcls=IPython...` addopt (pytest config moved from `setup.cfg` to
  `pyproject.toml`).
- `tests/integration/` excluded — needs the integration stack (see below).

For coverage, add `-p pytest_cov --cov=ckanext/datapusher_plus` —
`PYTEST_DISABLE_PLUGIN_AUTOLOAD=1` disables `--cov` autodiscovery, so
`-p pytest_cov` must be explicit. `.coveragerc` was fixed on the
`docs-readme-testing-section` branch (was stale `source = datapusher`
from the pre-extension era; now correctly `source = ckanext/datapusher_plus`).

> The full-suite tail prints noisy Prefect server-shutdown logging
> (`ValueError: I/O operation on closed file` from
> `prefect/logging/handlers.py`). Cosmetic — not a test failure. Grep
> the summary line: `... 2>&1 | grep -E "^[0-9]+ passed|^[0-9]+ failed"`.

## Known result (as of 2026-05-20, `main` @ `eca276d`, PR #326 merged)

~280 Python unit tests pass on `main` (the `grep -c "^    def test_\|^def test_"`
file-by-file count totals 259 functions across 28 files, but pytest
collection expands parametrized tests further — the actual collected
count is the ~280 reported by the PR descriptions).

Test count history:
- 218 → 251 over the 2026-05-17 → 2026-05-19 arc (#299 → #322, net **+33**).
- 251 → 277 over the 2026-05-19 → 2026-05-20 arc (#323 → #324, net **+26**).
- 277 → ~280 over the 2026-05-20 arc (#326 added `test_formats_config.py`
  with 3 tests).

The breakdown isn't a simple per-file sum because refactors also removed
or consolidated pre-existing tests (the v3 refactor retired
`test_unit.py`, `test_mocked.py`, `test_acceptance.py`, `test_web.py`).
Reproducible via
`pytest tests/ --ignore=tests/integration --collect-only -q`.

Most recent regression test files:
- `tests/test_formats_config.py` — 3 tests (PR #326: xlsm/xlsb in FORMATS,
  AST drift guard that every `FormatConverterStage.SPREADSHEET_EXTENSIONS`
  entry is gated in by FORMATS, lowercase-key regression guard for the
  spatial-tolerance setting).
- `tests/test_pii_screening_config_key.py` — 3 tests (PR #324: regression
  for the `datastore_plus` → `datapusher_plus` typo).
- `tests/test_issue_61_download_always_whitelist.py` — 23 tests (PR #323,
  feature #61).
- `tests/test_issue_261_empty_date_range.py` — 6 tests (PR #322,
  regression for #261).
- `tests/test_issue_112_decimal_comma.py` — 11 tests (PR #320,
  regression for #112).
- `tests/test_issue_142_auto_index_threshold.py` — 8 tests (PR #317,
  regression for #142).

The JS unit suite (Vitest + jsdom, PR #304) is unchanged at **12 tests**
for `scheming-ai-suggestions.js`. Runs on the host, not in `dpp-test`:
```bash
npm install                     # first time only
npm test                        # one-shot (alias for npx vitest run)
npm run test:watch              # watch mode
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
  the `test.yml`-style install (apt geo libs → `pip install GDAL==$(gdal-config
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

## CI workflows (post-PR #326)

- **`test.yml` — "Unit Tests"** (NEW in PR #326, replaces the dead
  Python-2.7-era cruft of the same name). Runs the full unit suite
  (`pytest tests/ --ignore=tests/integration`) inside
  `ckan/ckan-dev:2.11` on push to `main`/`dev` and PR to `main`. Python
  3.10 only — a 3.11-3.13 matrix would require building CKAN from
  source per version, left as a follow-up.
- **`ci.yml` — "DataPusher+ Integration CI"**. Runs the qsv contract
  regression (`test_qsv_v20_regression.py`) + the integration suite on
  push to `main`/`dev` and PR to `main`. Does NOT run the full unit
  suite. PR #326 also fixed `xlsxb` → `xlsb` typos in the
  `ckanext.datapusher_plus.formats` / `ckan.datapusher.formats` lines
  and the test-file `case` branch.
- **`main.yml`** — `workflow_dispatch`-only end-to-end run.

## Test patterns established (worth following)

1. **AST-parse `config.py`, YAML-parse `config_declaration.yaml`** — a
   recurring drift-guard pattern. `test_issue_142_auto_index_threshold.py`,
   `test_issue_112_decimal_comma.py`, `test_issue_61_download_always_whitelist.py`,
   `test_pii_screening_config_key.py`, and `test_formats_config.py` all
   walk the source files to confirm inline fallbacks and declaration
   defaults/keys agree. Catches #179-style declaration-vs-code drift
   without the CKAN bootstrap.
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

## Recent feature/regression highlights (cross-reference)

For the prior detailed write-ups of these arcs, see the git log
commit messages — they were inlined in earlier versions of this
memory but moved out to avoid duplication with CHANGELOG.md / PR
descriptions. Quick index:

- **PR #326** — unit-test CI revived; xlsm/xlsb in FORMATS;
  spatial-tolerance config key lowercased; README ckan.ini examples
  reconciled to actual defaults.
- **PR #325** — CLAUDE.md refreshed for v3.0 (Prefect, not v2 pipeline).
- **PR #324** — `pii_screening` config-key typo; preview_rows fallback
  aligned to declaration; doc-audit corrections to README/CONFIG.md.
- **PR #323** — `DOWNLOAD_ALWAYS_WHITELIST` (operator-controlled
  re-processing for hosts that update in place).
- **PR #322** — empty-date-range suggestion handling (Python coercion
  + JS grey-out + CSS).
- **PR #320** — locale-aware number normalization (Babel).
- **PR #317/#318** — `AUTO_INDEX_MIN_THRESHOLD` floor + bump 3 → 10.
- **PR #316** — DP+ version in flow log banner.
- **PR #315** — qsv pin 20.0.0 → 20.1.0.
- **PR #314** — qsv `Date` → Postgres `date` (not `timestamp`).
- **PR #313** — don't apply DownloadResult.resource on rehydrate.
- **PR #312** — preserve file_hash on metadata stage's resource re-fetch.
- **PR #309** — configurable file-hash algorithm (blake3 default).
- **PR #308** — naive-UTC timestamps everywhere.
- **PR #307** — Data Dictionary stash/restore on rollback.
- **PR #304** — Vitest + jsdom JS unit tests for AI suggestions.
- **PRs #301-#303** — `qsv describegpt` AI-suggestions stage end-to-end.
- **PR #300** — `Dockerfile.worker` rebased onto `ckan/ckan-dev:2.11`;
  `scripts/integration-up`/`-down`.
- **PR #299** — `resubmit` CLI robustness.
