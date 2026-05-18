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
- qsv 20.0.0 at `/usr/local/bin/qsvdp`.
- `b3sum` CLI at `/usr/local/bin/b3sum` (added by PR #309 for the
  configurable file-hash feature — required by tests that exercise the
  `blake3` algorithm via the external `b3sum` binary path).

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
- `QSV_BIN=/usr/local/bin/qsvdp` — for `test_qsv_v20_regression.py`.
- `CKAN_INI=/srv/app/src/ckan/test-core.ini` — the image's default
  `/srv/app/ckan.ini` is NOT populated when the entrypoint is bypassed;
  `test-core.ini` has a real `SECRET_KEY`.
- `-o addopts=` — overrides `setup.cfg`'s `--pdbcls=IPython...` addopt.
- `tests/integration/` excluded — needs the integration stack (see below).

## Known result (as of 2026-05-18, `main` @ `9a8a6c7` + WIP)
**222/222 Python unit tests pass** on `main` after the #299 → #314 merges
plus the in-flight `tests/test_issue_173_date_format_inference.py`.
Net delta from the prior handoff (196 tests): +26 tests, mainly from
- `tests/test_dictionary_stash.py` (21 tests, PR #307)
- `tests/test_utcnow_naive.py` (4 tests, PR #308)
- `tests/test_file_hash_algorithms.py` (PR #309)
- `tests/test_metadata_stage_file_hash.py` (PR #312, regression for #310)
- `tests/test_download_result_rehydrate.py` (PR #313, regression for #311)
- `tests/test_date_without_timestamp.py` (PR #314, regression for #179)
- `tests/test_issue_173_date_format_inference.py` (4 tests, regression
  for #173 — qsv date-format inference coverage + malformed-CSV
  quarantine precondition; needs `QSV_BIN` env var to run)

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

## If the container is gone (Docker restart / removed)
- Restart: `docker start dpp-test` (state persists across stops).
- Recreate: `docker run -d --name dpp-test --platform linux/amd64 --user root
  -v <repo>:/repo -w /repo ckan/ckan-dev:2.11 sleep infinity`, then re-run
  the `ci.yml`-style install (apt geo libs → `pip install GDAL==$(gdal-config
  --version)` → `pip install -r requirements.txt -r requirements-dev.txt -e .`
  → download qsv 20.0.0 musl zip → `qsvdp` to `/usr/local/bin/` → download
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
   level) — the stage writes it nested inside `ai_suggestions`. Fix
   pinned by `tests/js/scheming-ai-suggestions.test.js` test
   `stops polling when dpp_suggestions.ai_suggestions.STATUS is in
   terminalStatuses`.
4. **Fixture** — `tests/fixtures/qsv_describegpt_sample.json` is a real
   captured response from a LM Studio gemma-4-e4b run; use it for
   shape-of-envelope assertions in Python tests instead of hand-rolling
   one.

## Data Dictionary stash/restore (PR #307, issue #265)

End-to-end shape of the Data Dictionary preservation across DP+ job
failures now on `main`:

1. **Module** — `ckanext/datapusher_plus/dictionary_stash.py` is a tiny
   on-disk persistence layer (`save` / `load` / `clear` / `stash_path`).
   Atomic writes via `os.replace` with `.tmp` cleanup on failure. Stash
   dir is configurable via `ckanext.datapusher_plus.dictionary_stash_dir`
   (defaults to `<tempdir>/dpp_dict_stash`). Path-traversal guard on
   `resource_id`. `load`/`clear` deliberately do NOT bootstrap the
   directory — only `save` does.
2. **Stash** — `AnalysisStage._parse_stats` writes `existing_info` to
   the stash BEFORE deleting the existing datastore resource. Best-effort:
   a stash failure logs a warning but does not block ingestion.
3. **Restore — in-transaction failure** — `_rollback_database`
   (`@database_task.on_rollback`) drops the half-written table and, if a
   stash exists, re-creates the datastore resource with the stashed
   `info` dicts and zero rows. Each field's Postgres `type` is derived
   from `info["type_override"]` (mapped through `conf.TYPE_MAPPING.values()`,
   falling back to `text`) — otherwise CKAN's `datastore_create` defaults
   columns to `text` and `numeric`/`timestamp` annotations get silently
   downgraded.
4. **Restore — failure outside the transaction** — `analyze_task`,
   `ai_suggestions_task`, and `_maybe_suspend_for_pii_review` run BEFORE
   the `with transaction():` block, so failures there don't fire any
   rollback hook. On retry, `AnalysisStage._parse_stats` checks for a
   stash when no live datastore exists and loads it as `existing_info`
   — the merge logic then propagates it onto the rebuilt headers, and
   the success-finally clears the stash.
5. **Cleanup** — `datapusher_plus_flow`'s `finally` clears the stash on
   any successful exit (including `_StageAbort` complete-with-skip).
   Error paths leave the stash for the rollback hook or a subsequent
   retry. Stash mtime is surfaced in restore logs so operators can
   distinguish genuine retry-after-failure from stale-restore caused by
   an orphaned stash being applied to an unrelated upload.

Test coverage: `tests/test_dictionary_stash.py` has 21 tests pinning
the module, the `_parse_stats` retry-restore branch, and the
`_rollback_database` type-mapping behavior.

## UTC timestamps (PR #308, issue #145)

DP+ now uses a single helper `ckanext.datapusher_plus.utils.utcnow_naive()`
for every persisted timestamp. It returns
`datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)` —
naive (so it fits the `TIMESTAMP WITHOUT TIME ZONE` columns) AND UTC
(so a worker running in a non-UTC tz doesn't silently record local
time). All seven call sites that previously used
`datetime.datetime.now()` (local — wrong) or `datetime.datetime.utcnow()`
(deprecated in Python 3.12+) have been migrated:

- `helpers.py:345,364` — `mark_job_as_completed` / `mark_job_as_errored`
  `finished_timestamp`. **The actual bug** — these were on local time.
- `cli.py:452` — `migrate_from_rq` `ts.last_updated`.
- `logic/action.py:101,132,230,264` — `task_status.last_updated` reads
  and writes through the submit / hook actions. Reads switched from
  `strptime("%Y-%m-%dT%H:%M:%S.%f")` to `fromisoformat(...)`; writes
  switched from `str(dt)` to `dt.isoformat()` — symmetric, microsecond-
  safe, no T-vs-space mismatch.

The resource-data UI template now surfaces "UTC" suffix in timestamp
tooltips. Test coverage: `tests/test_utcnow_naive.py` (4 tests, including
a real `TZ=Pacific/Auckland` + `time.tzset()` flip that proves the
helper returns UTC even when the process tz is not UTC).

## Configurable file-hash algorithm (PR #309, issue #221)

DP+ now supports three file-hash algorithms for resource integrity
checks, selectable via `ckanext.datapusher_plus.file_hash_algorithm`:

- `blake3` (default) — ~10x faster than sha256, computed via the
  `b3sum` CLI installed in worker / CI / `dpp-test`.
- `sha256` — for DCAT3 / Croissant interoperability (those specs
  require sha256).
- `md5` — legacy compatibility only.

Implementation lives in `ckanext/datapusher_plus/file_hash.py` with a
single `compute_file_hash(path, algorithm)` dispatcher. `blake3` shells
out to `b3sum --no-names`; `sha256`/`md5` use Python `hashlib` with
chunked reads. Tests in `tests/test_file_hash_algorithms.py` pin all
three algorithms against known fixtures.

## file_hash preservation across metadata-stage re-fetch (PR #312, issue #310)

Caught during #309 smoke testing: `MetadataStage` re-fetches the resource
dict (to get the latest CKAN state before applying suggestions) and that
re-fetched dict OVERWRITES `ctx.resource`, discarding the `file_hash`
that the download stage had just computed. Fix is a single line in
`jobs/stages/metadata.py` that restores `file_hash` from `context.file_hash`
after the re-fetch:

```python
ctx.resource = refreshed
if ctx.file_hash:
    ctx.resource["file_hash"] = ctx.file_hash
```

`context.file_hash` is the authoritative value (set by `DownloadStage`),
so the re-fetched resource dict's stale/missing `file_hash` should never
win. Regression test: `tests/test_metadata_stage_file_hash.py`.

## DownloadResult rehydrate cross-resource cache leak (PR #313, issue #311)

Also caught during #309 smoke testing. The `_apply_result` codepath for
the cached `DownloadResult` was wholesale-replacing `ctx.resource` with
the CACHED resource dict — which had a different `resource_id` than the
job currently being processed. Downstream `TRUNCATE` then targeted the
wrong table (or failed with "relation does not exist" if the cached
resource had been deleted). Fix:

- `_apply_result` for `DownloadResult` no longer overwrites `ctx.resource`
  on a cache hit. Only the truly download-derived fields
  (`file_path`, `file_hash`, `mime_type`, `file_size`) are applied.
- `ctx.resource` keeps the live resource dict that the orchestrator
  populated at job start.

Regression test: `tests/test_download_result_rehydrate.py` asserts that
two consecutive runs with different `resource_id`s but identical content
hash don't cross-pollinate.

## qsv date-format inference gaps (issue #173 baseline)

Captured against qsv 20.0.0 (pinned in `Dockerfile.worker` and CI).
The matrix is pinned in `tests/test_issue_173_date_format_inference.py`;
when the production qsv version moves, update both the test and the
issue #173 comment.

Known qsv 20.0.0 gaps on the reporter's 9-format CSV:
- `ISO 8601 (YYYY-MM-DDTHH:MM:SS)` values like `2024-10-11T14:30:00`
  (no tz, T-separator) → inferred as **String**. Recognized in qsv ≥ 20.1.
- `DD-MM-YYYY` (dash-separated, day-first, e.g. `11-10-2024`) →
  **String**. qsv has no heuristic for this format regardless of
  `--prefer-dmy`.
- `Unix Timestamp` (bare epoch integers) → **Integer**. qsv has no
  heuristic to flag a 10-digit integer column as epoch seconds.

What works on qsv 20.0.0:
- RFC 2822 (`Fri, 11 Oct 2024 14:30:00 +0000`) → DateTime ✓
- MM/DD/YYYY → Date ✓
- YYYY/MM/DD → Date ✓
- DD/MM/YYYY HH:MM → DateTime (interpreted as MDY by default — set
  `ckanext.datapusher_plus.prefer_dmy = True` to flip)
- YYYY-MM-DD HH:MM:SS → DateTime ✓

The reporter's actual CSV in #173 is malformed (unquoted comma inside
RFC 2822 values → header has 10 fields, every data row has 11).
v3.0's `ValidationStage` catches this cleanly via the quarantine pass
(see #265 stash / quarantine sections + `tests/test_validation_quarantine.py`).
The original "wrong format" symptom in v1.0.3 was column shifting
masquerading as a date-format issue.

## qsv Date → Postgres date (PR #314, issue #179)

`config_declaration.yaml`'s default for `qsv_dp_type_to_pg_type` had
`"Date": "timestamp"` while `config.py`'s inline fallback (which always
loses against the declaration) had `"Date": "date"`. Result: every
column qsv inferred as `Date` got stored as Postgres `timestamp` with
midnight time-of-day, defeating the point of having a separate `Date`
type. Fix is a one-line change to `config_declaration.yaml`:
`"Date": "date"`.

Knock-on change: `_get_date_like_columns` in `jobs/stages/indexing.py`
previously only matched `timestamp` columns when picking candidates for
`AUTO_INDEX_DATES`. It now matches both `date` and `timestamp`, so the
indexer still creates dates indexes after the type-mapping fix.

Regression test: `tests/test_date_type_mapping.py` asserts the
declaration default and the indexer's column-type filter agree.
