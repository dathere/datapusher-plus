# Codebase Structure

## Top-level layout
```
/Users/joelnatividad/GitHub/datapusher-plus/
├── ckanext/datapusher_plus/         # The CKAN extension (main package)
├── datapusher/                      # Legacy/companion package (kept for reference)
├── tests/                           # pytest suite (~280 unit tests as of 2026-05-20)
├── docs/                            # dataset_schema.yaml, RESOURCE_FIRST_WORKFLOW.md, SQL helper
│   └── images/                      # In-repo README assets
│       └── druf-suggestions-demo.gif  # PR #321, 6.4 MB ffmpeg-optimized demo
├── images/                          # Older README screenshots (predates docs/images/)
├── scripts/                         # integration-up / integration-down
├── .github/workflows/               # CI: test.yml (unit, PR #326), ci.yml (qsv regression + integration),
│                                    #     main.yml (manual workflow_dispatch e2e), codeql, python-publish
├── pyproject.toml                   # Project metadata + setuptools config
│                                    # NOTE: pytest config lives here under
│                                    # [tool.pytest.ini_options] — moved from setup.cfg
├── setup.py / setup.cfg / MANIFEST.in
├── requirements.txt / requirements-dev.txt
├── dot-env.template                 # Sample env vars
├── default-pii-regexes.txt          # Default PII regex patterns
├── test_config.py                   # Tests configuration helper
├── wsgi.py                          # WSGI entry shim
├── .coveragerc                      # source = ckanext/datapusher_plus (fixed in PR/branch
│                                    # `docs-readme-testing-section`; previously pointed at the
│                                    # pre-extension `datapusher` package and measured nothing)
├── Containerfile                    # OCI image
├── Dockerfile.worker                # Prefect worker image (base: ckan/ckan-dev:2.11)
├── docker-compose.integration.yaml  # Integration stack (postgres + redis + solr + prefect + ckan)
├── CHANGELOG.md / README.md / CONFIG.md / LICENSE
└── CLAUDE.md                        # Project-specific Claude guidance (refreshed in PR #325 for v3.0)
```

## Main package: `ckanext/datapusher_plus/`

> NOTE: the v2-era `jobs.py`, `jobs_legacy.py`, and `jobs/pipeline.py` files
> no longer exist. The v3.0 Prefect refactor (commit `22db8ab`) replaced
> them with `jobs/prefect_flow.py`. Anywhere old docs/memories still
> reference those filenames, they are stale.

```
plugin.py             # CKAN plugin entry point — DatapusherPlusPlugin (SingletonPlugin)
config.py             # ~50+ ckanext.datapusher_plus.* settings, including:
                      #   DEFAULT_LOCALE / DECIMAL_SEPARATOR (PR #320, issue #112)
                      #   AUTO_INDEX_MIN_THRESHOLD (PR #317, issue #142)
                      #   USE_TRUNCATE_FREEZE, COPY_READBUFFER_SIZE
                      #   FORMATS default includes xlsm/xlsb (PR #326)
                      #   PII_SCREENING reads from the correct
                      #     ckanext.datapusher_plus.* key (PR #324 typo fix)
                      #   SPATIAL_SIMPLIFICATION_RELATIVE_TOLERANCE key string
                      #     lowercased to match the other ~50 keys (PR #326)
config_declaration.yaml # CKAN 2.10+ declarative config. Note: string-typed
                      # settings OMIT `type:` entirely (CKAN's loader only
                      # accepts type ∈ {base, bool, int, dynamic, list}).
qsv_utils.py          # QSV CLI wrapper (stats, frequency, type inference, validation,
                      # describegpt, replace, safenames, ...)
jinja2_helpers.py     # FormulaProcessor + custom filters/functions. process_formulae
                      # coerces empty/None outputs to Python None when
                      # formula_type == "suggestion_formula" (PR #322, issue #261).
prefect_client.py     # Thin wrapper around the Prefect 3 client; single place
                      # the codebase touches `prefect.*` from CKAN admin paths.
datastore_utils.py    # PostgreSQL datastore operations
spatial_helpers.py    # Shapefile/GeoJSON + geometry simplification
pii_screening.py      # PII detection with configurable regexes
helpers.py            # Template helpers for job-status UI in CKAN
cli.py                # CKAN CLI commands: resubmit, submit, prefect-deploy, migrate-from-rq
logging_utils.py      # Custom TRACE log level (5)
interfaces.py         # IDataPusher external-plugin hook interface
job_exceptions.py     # Custom exceptions: DataTooBigError, JobError, HTTPError, ...
views.py              # Flask blueprints
druf_view.py          # DRUF-specific view handling
dictionary_stash.py   # Data Dictionary stash/restore across DP+ job failures (PR #307)
utils.py              # utcnow_naive() + general helpers
dataset-druf.yaml     # Bundled scheming schema for DRUF.
                      # Resource_fields include the dpp_* namespace:
                      #   dpp_locale (PR #320, issue #112)
                      #   data_dictionary
                      # Dataset_fields include:
                      #   dpp_suggestions (compound JSON, populated by suggestion_formula)
                      #   dpp_spatial_extent (written by metadata stage)
assets/               # CKAN static (CSS + JS)
  js/
    scheming-suggestions.js    # Legacy DRUF suggestions UI (no JS tests yet)
    scheming-ai-suggestions.js # AI suggestions UI (12 vitest tests, PR #304)
  styles/
    suggestions.css            # Includes .suggestion-btn-disabled (PR #322)
templates/            # Jinja templates (resource_data.html, scheming form snippets, ...)
logic/
  action.py           # datapusher_submit, datapusher_hook, datapusher_status
  auth.py             # Authorization functions
  schema.py           # Validation schemas
model/
  model.py            # Jobs, Metadata, Logs SQLAlchemy models + get_job_details()
migration/datapusher_plus/   # Alembic migrations
jobs/                  # v3.0 Prefect-orchestrated flow
  __init__.py          # PEP 562 lazy __getattr__ — defers the Prefect import so
                       # CKAN admin commands don't spin up a Prefect server.
                       # Exposes datapusher_plus_flow, push_to_datastore (v2 shim),
                       # datapusher_plus_to_datastore (alias), callback_datapusher_hook.
  prefect_flow.py      # Orchestration. Per-stage @task functions (each delegates to
                       # a BaseStage.process() body) + entry-point @flow
                       # `datapusher_plus_flow`. Wraps datastore-mutating tasks in
                       # `with transaction()` for atomic rollback; owns Jobs row state
                       # transitions; fires the datapusher_hook HTTP callback.
  context.py           # ProcessingContext — per-run mutable state shared across stages.
                       # Carries `resource` dict; per-resource fields like dpp_locale
                       # are read via context.resource.get(...).
  runtime_context.py   # JobInput (frozen, JSON-serializable flow input), the per-stage
                       # *Result dataclasses, RuntimeContext ContextVar (set/get/reset),
                       # `rehydrate`.
  subflows.py          # @flow-wrapped subflows (pii_screening_subflow,
                       # spatial_processing_subflow) for custom flow composition.
  events.py            # Custom Prefect events for downstream Automations.
  caching.py           # Task result-persistence + cache-key configuration.
  blocks.py            # Prefect Block registration (result-storage config).
  artifacts.py         # Human-readable Prefect run-page artifacts (data-quality summaries).
  quarantine.py        # Bad-row quarantine for the validation task.
  file_persistence.py  # Persists task working files to result storage so cached task
                       # results stay valid across runs.
  utils/               # (helpers used by stages / flow)
  stages/
    base.py            # Abstract BaseStage
    download.py        # Download with retries / proxy / timeout. _should_skip_upload
                       # honors DOWNLOAD_ALWAYS_WHITELIST (PR #323).
    format_converter.py # Excel/ODS/Shapefile/GeoJSON/ZIP → CSV. Advertises
                       # SPREADSHEET_EXTENSIONS (includes .xlsm, .xlsb).
    validation.py      # RFC-4180 CSV validation, encoding detection/normalization
    analysis.py        # QSV-based type inference + summary stats + frequency tables.
                       # Includes _normalize_locale_numbers (PR #320) between
                       # _sanitize_headers and _create_index.
    database.py        # PostgreSQL COPY ops, smartint type selection.
                       # TRUNCATE+COPY FREEZE branch gated on USE_TRUNCATE_FREEZE.
    indexing.py        # Auto-index creation over cardinality range
                       # [AUTO_INDEX_MIN_THRESHOLD, AUTO_INDEX_THRESHOLD] (PR #317).
                       # Picks date + timestamp columns for AUTO_INDEX_DATES.
    formula.py         # Jinja2 formula evaluation (FormulaProcessor.process_formulae
                       # with formula_type="formula" or "suggestion_formula")
    metadata.py        # Datastore resource dict updates, dpp_suggestions write,
                       # file_hash restoration after re-fetch (PR #312)
    ai_suggestions.py  # AI suggestions stage (PR #301, `qsv describegpt`)
```

## Tests: `tests/`

Current files (28 Python test files; ~280 unit tests collected):
```
test_ai_suggestions.py                      # PR #301 backend (44 tests)
test_cli_migrate_from_rq.py                 # CLI v2→v3 migration (4 tests)
test_cli_resubmit.py                        # CLI resubmit (11 tests)
test_csv_spatial_extent.py                  # Spatial extent helpers (14 tests)
test_database_copy_strategy.py              # COPY+FREEZE vs concurrent-reads (4 tests)
test_date_without_timestamp.py              # PR #314, regression for #179 (5 tests)
test_delimiter_detection.py                 # Delimiter sniffer (7 tests)
test_dictionary_stash.py                    # PR #307 (19 tests)
test_file_hash_algorithm.py                 # PR #309 (6 tests)
test_file_persistence.py                    # File cache layer (10 tests)
test_formats_config.py                      # PR #326 — FORMATS drift guards (3 tests)
test_issue_111_version_in_log.py            # PR #316, regression for #111 (4 tests)
test_issue_112_decimal_comma.py             # PR #320, regression for #112 (11 tests)
test_issue_142_auto_index_threshold.py      # PR #317, regression for #142 (8 tests)
test_issue_173_date_format_inference.py     # PR #315, regression for #173 (4 tests)
test_issue_261_empty_date_range.py          # PR #322, regression for #261 (6 tests)
test_issue_61_download_always_whitelist.py  # PR #323, feature #61 (23 tests)
test_metadata_hash_persistence.py           # PR #312, regression for #310 (3 tests)
test_metadata_stats_cleanup.py              # Metadata stage stats cleanup (3 tests)
test_pii_screening_config_key.py            # PR #324, typo regression (3 tests)
test_prefect_client.py                      # Prefect client wrapper (1 test)
test_prefect_flow.py                        # Prefect orchestration (20 tests)
test_qsv_v20_regression.py                  # qsv v20 regression suite (14 tests)
test_rehydrate_resource_identity.py         # PR #313, regression for #311 (4 tests)
test_security.py                            # FormulaProcessor sandbox + LIKE-escape (16 tests)
test_subflows.py                            # Prefect subflows (5 tests)
test_utcnow_naive.py                        # PR #308 (4 tests)
test_validation_quarantine.py               # Validation + quarantine pass (3 tests)

log_analyzer.py                              # Helper / analytics (not a pytest module)
README.md                                    # Tests reference
static/                                      # Test fixtures (static assets)
custom/                                      # Custom data files used in CI (FILES_DIR=custom)
fixtures/                                    # JSON fixtures (e.g. qsv_describegpt_sample.json)
js/                                          # Vitest specs (scheming-ai-suggestions.test.js + setup.js)
integration/                                 # Integration suite (run via scripts/integration-up)
```

> The historical `test_unit.py` / `test_mocked.py` / `test_acceptance.py` /
> `test_web.py` files no longer exist — they were retired during the v3.0
> refactor in favor of per-feature regression files.

## Pipeline architecture (v3.0)

Entry point: `jobs.prefect_flow.datapusher_plus_flow` (the @flow).
`datapusher_plus_to_datastore` is kept as an alias on `jobs.__init__`
(PEP 562 lazy __getattr__) for v2 callers.

Per-stage `@task` functions in `prefect_flow.py` each call into a
`BaseStage.process()` body and mutate the shared `ProcessingContext`
(`jobs/context.py`). The datastore-mutating tasks run inside
`with transaction()` for atomic rollback.

Stage order (high level):
1. **DownloadStage** — fetch source file with hashing; honors
   `DOWNLOAD_ALWAYS_WHITELIST` (PR #323).
2. **FormatConverterStage** — Excel / ODS / Shapefile / GeoJSON / ZIP → CSV.
3. **ValidationStage** — RFC-4180, encoding detection, quarantine.
4. **AnalysisStage** — sanitize headers, **locale normalization (PR #320)**,
   qsv index, qsv stats, header dicts, frequency tables, preview, date norm.
5. **DatabaseStage** — TRUNCATE + COPY (optionally with FREEZE).
6. **IndexingStage** — AUTO-INDEXING over cardinality range.
7. **FormulaStage** — Jinja2 formulas → direct field updates and
   suggestions (`FormulaProcessor.process_formulae`).
8. **MetadataStage** — datastore resource dict updates, dpp_suggestions
   write, file_hash restoration.
9. **AISuggestionsStage** — `qsv describegpt`, gated on
   `ckanext.datapusher_plus.enable_ai_suggestions`.

## Database models (`model/model.py`)
- `Jobs` — job_id, status, data, error, timestamps (UTC via `utcnow_naive()`)
- `Metadata` — formula evaluation results
- `Logs` — detailed processing logs
- `get_job_details()` — retrieval helper

## Formula system
Three namespaces available in scheming-YAML formulas:
- `dpps` — per-field summary stats (type, min/max, cardinality, stddev, …)
- `dppf` — per-field frequency tables (top N values w/ counts)
- `dpp` — inferred metadata (RECORD_COUNT, DATE_FIELDS, LAT_FIELD,
  LON_FIELD, NO_DATE_FIELDS, NO_LAT_LON_FIELDS, dataset_stats, …)

Two formula kinds (NOTE the production keys — Copilot caught a phantom
constant `suggest_formula` in PR #322 tests; the real key is
`suggestion_formula`):
- `formula` — evaluated immediately, assigned to the field.
- `suggestion_formula` — stored in `dpp_suggestions` for UI suggestions.
  When this renders to None/empty/whitespace, `FormulaProcessor`
  coerces to Python `None` (PR #322, issue #261) so the front-end
  greys out the suggestion button.
