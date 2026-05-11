# Codebase Structure

## Top-level layout
```
/Users/joelnatividad/GitHub/datapusher-plus/
├── ckanext/datapusher_plus/         # The CKAN extension (main package)
├── datapusher/                      # Legacy/companion package
├── tests/                           # pytest suite
├── docs/                            # dataset_schema.yaml, RESOURCE_FIRST_WORKFLOW.md, SQL helper
├── images/                          # README screenshots
├── .github/workflows/               # CI: main.yml (integration), test.yml, codeql, python-publish
├── pyproject.toml                   # Project metadata + setuptools config
├── setup.py / setup.cfg / MANIFEST.in
├── requirements.txt / requirements-dev.txt
├── dot-env.template                 # Sample env vars
├── default-pii-regexes.txt          # Default PII regex patterns
├── test_config.py                   # Tests configuration helper
├── wsgi.py                          # WSGI entry shim
├── Containerfile                    # OCI image
├── CHANGELOG.md / README.md / CONFIG.md / LICENSE
└── CLAUDE.md                        # Project-specific Claude guidance (read this!)
```

## Main package: `ckanext/datapusher_plus/`
```
plugin.py             # CKAN plugin entry point — DatapusherPlusPlugin (SingletonPlugin)
config.py             # ~50 ckanext.datapusher_plus.* settings
config_declaration.yaml # CKAN 2.10+ declarative config
qsv_utils.py          # QSV CLI wrapper (stats, frequency, type inference, validation)
jinja2_helpers.py     # FormulaProcessor + custom filters/functions
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
utils.py
dataset-druf.yaml
assets/  templates/   # CKAN static + Jinja templates
logic/
  action.py           # datapusher_submit, datapusher_hook, datapusher_status
  auth.py             # Authorization functions
  schema.py           # Validation schemas
model/
  model.py            # Jobs, Metadata, Logs SQLAlchemy models + get_job_details()
migration/datapusher_plus/   # Alembic migrations
jobs/                  # v2.0 modular pipeline (replaces jobs_legacy.py)
  pipeline.py          # Orchestration entry point: datapusher_plus_to_datastore
  context.py           # ProcessingContext — shared state across stages
  utils/               # (helpers used by stages)
  stages/
    base.py            # Abstract BaseStage
    download.py        # Download with retries / proxy / timeout
    format_converter.py # Excel/ODS/Shapefile/GeoJSON/ZIP → CSV
    validation.py      # RFC-4180 CSV validation, encoding detection/normalization
    analysis.py        # QSV-based type inference, summary stats, frequency tables
    database.py        # PostgreSQL COPY ops, smartint type selection
    indexing.py        # Auto-index creation based on cardinality / dates
    formula.py         # Jinja2 formula evaluation
    metadata.py        # Datastore resource dict updates, dpp_suggestions
```

## Tests: `tests/`
```
test_unit.py           # Unit tests
test_mocked.py         # Mocked integration
test_acceptance.py     # End-to-end / acceptance
test_web.py            # Web endpoint tests
settings_test.py       # Test settings
log_analyzer.py        # Helper / analytics
README.md              # Tests reference (scoring algorithms, CSV schemas, etc.)
static/                # Test fixtures (static assets)
custom/                # Custom data files used in CI (FILES_DIR=custom)
```

## Pipeline architecture (v2.0)
Entry point `datapusher_plus_to_datastore` (in `jobs/pipeline.py`) orchestrates an ordered sequence of stages from `jobs/stages/`, all subclasses of `BaseStage`. State flows through a shared `ProcessingContext` (`jobs/context.py`) that also holds the per-job logger. Each stage isolates one concern: download → format conversion → validation → analysis → database → indexing → formula evaluation → metadata update.

## Database models (`model/model.py`)
- `Jobs` — job_id, status, data, error, timestamps
- `Metadata` — formula evaluation results
- `Logs` — detailed processing logs
- `get_job_details()` — retrieval helper

## Formula system
Three namespaces available in scheming-YAML formulas:
- `dpps` — per-field summary stats (type, min/max, cardinality, stddev, …)
- `dppf` — per-field frequency tables (top N values w/ counts)
- `dpp` — inferred metadata (RECORD_COUNT, DATE_FIELDS, LAT_FIELD, LON_FIELD, …)

Two formula kinds:
- `formula` — evaluated immediately, assigned to the field.
- `suggest_formula` — stored in `dpp_suggestions` for UI suggestions.
