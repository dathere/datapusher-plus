# Codebase Structure

## Top-level layout
```
/Users/joelnatividad/GitHub/datapusher-plus/
├── ckanext/datapusher_plus/         # The CKAN extension (main package)
├── datapusher/                      # Legacy/companion package
├── tests/                           # pytest suite (251 tests as of 2026-05-19)
├── docs/                            # dataset_schema.yaml, RESOURCE_FIRST_WORKFLOW.md, SQL helper
│   └── images/                      # In-repo README assets
│       └── druf-suggestions-demo.gif  # PR #321, 6.4 MB ffmpeg-optimized demo
├── images/                          # Older README screenshots (predates docs/images/)
├── scripts/                         # integration-up / integration-down
├── .github/workflows/               # CI: main.yml (integration), test.yml, codeql, python-publish
├── pyproject.toml                   # Project metadata + setuptools config
├── setup.py / setup.cfg / MANIFEST.in
├── requirements.txt / requirements-dev.txt
├── dot-env.template                 # Sample env vars
├── default-pii-regexes.txt          # Default PII regex patterns
├── test_config.py                   # Tests configuration helper
├── wsgi.py                          # WSGI entry shim
├── Containerfile                    # OCI image
├── Dockerfile.worker                # Prefect worker image (uses ckan/ckan-dev:2.11 base)
├── docker-compose.integration.yaml  # Integration stack (postgres + redis + solr + prefect + ckan)
├── CHANGELOG.md / README.md / CONFIG.md / LICENSE
└── CLAUDE.md                        # Project-specific Claude guidance (read this!)
```

## Main package: `ckanext/datapusher_plus/`
```
plugin.py             # CKAN plugin entry point — DatapusherPlusPlugin (SingletonPlugin)
config.py             # ~50+ ckanext.datapusher_plus.* settings, including:
                      #   DEFAULT_LOCALE / DECIMAL_SEPARATOR (PR #320, issue #112)
                      #   AUTO_INDEX_MIN_THRESHOLD (PR #317, issue #142)
                      #   USE_TRUNCATE_FREEZE, COPY_READBUFFER_SIZE
config_declaration.yaml # CKAN 2.10+ declarative config. Note: string-typed
                      # settings OMIT `type:` entirely (CKAN's loader only
                      # accepts type ∈ {base, bool, int, dynamic, list}).
qsv_utils.py          # QSV CLI wrapper (stats, frequency, type inference, validation,
                      # describegpt, replace, safenames, ...)
jinja2_helpers.py     # FormulaProcessor + custom filters/functions. process_formulae
                      # coerces empty/None outputs to Python None when
                      # formula_type == "suggestion_formula" (PR #322, issue #261).
datastore_utils.py    # PostgreSQL datastore operations
spatial_helpers.py    # Shapefile/GeoJSON + geometry simplification
pii_screening.py      # PII detection with configurable regexes
helpers.py            # Template helpers for job-status UI in CKAN
cli.py                # CKAN CLI commands (resubmit, submit)
logging_utils.py      # Custom TRACE log level (5)
interfaces.py         # IDataPusher external-plugin hook interface
job_exceptions.py     # Custom exceptions: DataTooBigError, JobError, HTTPError, ...
views.py              # Flask blueprints
druf_view.py          # DRUF-specific view handling
jobs.py               # (entry / glue)
jobs_legacy.py        # Old monolithic implementation (kept for reference)
dictionary_stash.py   # Data Dictionary stash/restore across DP+ job failures (PR #307)
utils.py              # utcnow_naive() + general helpers
dataset-druf.yaml     # Bundled scheming schema for DRUF.
                      # Resource_fields include the dpp_* namespace:
                      #   dpp_locale (PR #320, issue #112)
                      #   data_dictionary
                      # Dataset_fields include:
                      #   dpp_suggestions (compound JSON, populated by suggest_formula)
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
jobs/                  # v2.0 modular pipeline (replaces jobs_legacy.py)
  pipeline.py          # Orchestration entry point: datapusher_plus_to_datastore
  context.py           # ProcessingContext — shared state across stages.
                       # Carries `resource` dict; per-resource fields like
                       # dpp_locale are read via context.resource.get(...)
  utils/               # (helpers used by stages)
  stages/
    base.py            # Abstract BaseStage
    download.py        # Download with retries / proxy / timeout
    format_converter.py # Excel/ODS/Shapefile/GeoJSON/ZIP → CSV
    validation.py      # RFC-4180 CSV validation, encoding detection/normalization
    analysis.py        # QSV-based type inference + summary stats + frequency tables.
                       # Includes _normalize_locale_numbers (PR #320) which runs
                       # between _sanitize_headers and _create_index.
    database.py        # PostgreSQL COPY ops, smartint type selection.
                       # TRUNCATE+COPY FREEZE branch gated on USE_TRUNCATE_FREEZE.
    indexing.py        # Auto-index creation based on cardinality range
                       # [AUTO_INDEX_MIN_THRESHOLD, AUTO_INDEX_THRESHOLD] (PR #317).
                       # Picks date + timestamp columns for AUTO_INDEX_DATES.
    formula.py         # Jinja2 formula evaluation (calls FormulaProcessor.process_formulae
                       # with formula_type="formula" or "suggestion_formula")
    metadata.py        # Datastore resource dict updates, dpp_suggestions write,
                       # file_hash restoration after re-fetch (PR #312)
    ai_suggestions.py  # AI suggestions stage (PR #301)
```

## Tests: `tests/`
```
test_unit.py                                # Unit tests
test_mocked.py                              # Mocked integration
test_acceptance.py                          # End-to-end / acceptance
test_web.py                                 # Web endpoint tests
test_security.py                            # FormulaProcessor sandbox + LIKE-escape
test_qsv_v20_regression.py                  # qsv v20 regression suite
test_dictionary_stash.py                    # PR #307 (21 tests)
test_utcnow_naive.py                        # PR #308 (4 tests)
test_file_hash_algorithm.py                 # PR #309 (10 tests)
test_metadata_hash_persistence.py           # PR #312, regression for #310 (3 tests)
test_rehydrate_resource_identity.py         # PR #313, regression for #311 (4 tests)
test_date_without_timestamp.py              # PR #314, regression for #179 (5 tests)
test_issue_173_date_format_inference.py     # PR #315, regression for #173 (4 tests)
test_issue_111_version_in_log.py            # PR #316, regression for #111 (5 tests)
test_issue_142_auto_index_threshold.py      # PR #317, regression for #142 (8 tests)
test_issue_112_decimal_comma.py             # PR #320, regression for #112 (11 tests)
test_issue_261_empty_date_range.py          # PR #322, regression for #261 (6 tests)
test_database_copy_strategy.py              # COPY+FREEZE vs concurrent-reads
test_csv_spatial_extent.py                  # Spatial extent helpers
test_ai_suggestions.py                      # PR #301 backend
test_validation_quarantine.py               # Validation + quarantine pass
test_prefect_flow.py                        # Prefect orchestration
test_prefect_client.py                      # Prefect client wrapper
test_subflows.py                            # Prefect subflows
test_file_persistence.py                    # File cache layer
test_cli_resubmit.py / test_cli_migrate_from_rq.py  # CLI commands
test_metadata_stats_cleanup.py              # Metadata stage stats cleanup
test_delimiter_detection.py                 # Delimiter sniffer
settings_test.py                            # Test settings
log_analyzer.py                             # Helper / analytics
README.md                                   # Tests reference
static/                                     # Test fixtures (static assets)
custom/                                     # Custom data files used in CI (FILES_DIR=custom)
fixtures/                                   # JSON fixtures (e.g. qsv_describegpt_sample.json)
js/                                         # Vitest specs (scheming-ai-suggestions.test.js + setup.js)
integration/                                # Integration suite (run via scripts/integration-up)
```

## Pipeline architecture (v2.0)
Entry point `datapusher_plus_to_datastore` (in `jobs/pipeline.py`)
orchestrates an ordered sequence of stages from `jobs/stages/`, all
subclasses of `BaseStage`. State flows through a shared
`ProcessingContext` (`jobs/context.py`) that also holds the per-job
logger.

Stage order (high level):
1. **DownloadStage** — fetch source file with hashing
2. **FormatConverterStage** — Excel / ODS / Shapefile / GeoJSON / ZIP → CSV
3. **ValidationStage** — RFC-4180, encoding detection, quarantine
4. **AnalysisStage** — sanitize headers, **locale normalization (PR #320)**,
   qsv index, qsv stats, header dicts, frequency tables, preview, date norm
5. **DatabaseStage** — TRUNCATE + COPY (optionally with FREEZE)
6. **IndexingStage** — AUTO-INDEXING over cardinality range
7. **FormulaStage** — Jinja2 formulas → direct field updates and
   suggestions (via `FormulaProcessor.process_formulae`)
8. **MetadataStage** — datastore resource dict updates,
   dpp_suggestions write, file_hash restoration
9. **AISuggestionsStage** — `qsv describegpt` (gated on
   `ckanext.datapusher_plus.enable_ai_suggestions`)

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
constant in PR #322 tests):
- `formula` — evaluated immediately, assigned to the field.
- `suggestion_formula` — stored in `dpp_suggestions` for UI suggestions.
  When this renders to None/empty/whitespace, `FormulaProcessor`
  coerces to Python `None` (PR #322, issue #261) so the front-end
  greys out the suggestion button.
