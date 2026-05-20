# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DataPusher+ is a CKAN extension (v3.0.0a0) for ultra-fast, robust data ingestion into CKAN's datastore. It replaces the legacy Datapusher webservice with a full CKAN extension that leverages [qsv](https://github.com/dathere/qsv) (a Rust-based CSV data-wrangling toolkit) for blazing-fast type inference and data analysis.

**Key differentiators:**
- Guaranteed data type inference by scanning entire files (not just first few rows)
- PostgreSQL COPY for direct data loading (no API overhead)
- Jinja2 formula system for metadata inference/suggestion (`formula` and `suggestion_formula` in scheming YAML)
- DRUF (Dataset Resource Upload First) workflow support
- v3.0: [Prefect](https://www.prefect.io/)-orchestrated ingestion flow (replaces the v2 in-process pipeline loop)

## Build & Test Commands

```bash
# Run the unit suite (integration tests need the docker-compose stack — see below)
pytest tests/ --ignore=tests/integration

# Run a specific test file
pytest tests/test_security.py

# Run with coverage
pytest --cov=ckanext/datapusher_plus tests/ --ignore=tests/integration

# Debug with IPython
pytest --pdbcls=IPython.terminal.debugger:TerminalPdb tests/
```

Integration tests (`tests/integration/`) require the full docker-compose stack (CKAN, Postgres, Redis, Solr, Prefect server + worker). Bring it up with `scripts/integration-up`, then run with `INTEGRATION=1 CKAN_URL=http://localhost:5050 pytest tests/integration/ -v`.

## CKAN CLI Commands

```bash
# Resubmit all updated datastore resources
ckan -c /etc/ckan/default/ckan.ini datapusher_plus resubmit -y

# Submit a specific package's resources
ckan -c /etc/ckan/default/ckan.ini datapusher_plus submit {dataset_id}

# Register / update the DataPusher+ flow as a Prefect deployment (idempotent)
ckan -c /etc/ckan/default/ckan.ini datapusher_plus prefect-deploy

# One-shot v2 (RQ) → v3 (Prefect) migration: drains the RQ queue,
# resets stale pending task_status rows, verifies the Prefect deployment
ckan -c /etc/ckan/default/ckan.ini datapusher_plus migrate-from-rq

# Database migrations
ckan -c /etc/ckan/default/ckan.ini db upgrade -p datapusher_plus
```

## Architecture

### Prefect Flow Architecture (v3.0)

The v3.0 release replaces the v2 in-process `DataProcessingPipeline` loop with a Prefect-orchestrated flow. The jobs module lives in `ckanext/datapusher_plus/jobs/`:

```
prefect_flow.py    → Orchestration. Per-stage @task functions (each delegates to a
                     BaseStage.process() body) + the entry-point @flow
                     `datapusher_plus_flow`. Wraps the datastore-mutating tasks in
                     `with transaction()` for atomic rollback; owns the Jobs row
                     state transitions; fires the datapusher_hook HTTP callback.
__init__.py        → Public surface via PEP 562 lazy __getattr__ (defers the Prefect
                     import so CKAN admin commands don't spin up a Prefect server).
                     Exposes `datapusher_plus_flow`, `push_to_datastore` (v2 shim),
                     `datapusher_plus_to_datastore` (alias), `callback_datapusher_hook`.
context.py         → ProcessingContext — per-run mutable state shared across stages.
runtime_context.py → JobInput (frozen, JSON-serializable flow input), the per-stage
                     `*Result` dataclasses (DownloadResult, AnalyzeResult, …), the
                     RuntimeContext ContextVar (set/get/reset) and `rehydrate`.
subflows.py        → @flow-wrapped subflows (pii_screening_subflow,
                     spatial_processing_subflow) for custom flow composition.
events.py          → Custom Prefect events emitted for downstream Automations.
caching.py         → Task result-persistence + cache-key configuration.
blocks.py          → Prefect Block registration (result-storage config).
artifacts.py       → Human-readable Prefect run-page artifacts (data-quality summaries).
quarantine.py      → Bad-row quarantine for the validation task (route rejects to a
                     sibling CSV, continue if under `max_quarantine_pct`).
file_persistence.py→ Persists task working files to result storage so cached task
                     results stay valid across runs.
stages/
  base.py            → Abstract BaseStage class
  download.py        → File download with retries, proxy support, timeout handling
  format_converter.py → Excel/ODS/Shapefile/GeoJSON/ZIP → CSV conversion
  validation.py      → RFC-4180 CSV validation, encoding detection/normalization
  analysis.py        → QSV-based type inference, summary stats, frequency tables
  ai_suggestions.py  → AI-assisted metadata suggestions via `qsv describegpt`
  database.py        → PostgreSQL COPY operations, smartint type selection
  indexing.py        → Auto-index creation based on cardinality/dates
  formula.py         → Jinja2 formula evaluation (package/resource metadata)
  metadata.py        → Datastore resource dict updates, dpp_suggestions
```

Operators can register a custom flow via `ckanext.datapusher_plus.prefect_flow`; the per-stage `@task` functions in `prefect_flow.py` are the public composable primitives.

### Key Modules

- **plugin.py** — CKAN plugin entry point, implements IConfigurer, IConfigurable, IActions, IAuthFunctions, IPackageController, IResourceUrlChange, IResourceController, ITemplateHelpers, IBlueprint, IClick (+ IFormRedirect conditionally)
- **config.py** — ~50 configuration parameters (all `ckanext.datapusher_plus.*` settings)
- **config_declaration.yaml** — CKAN 2.10+ declarative config definitions
- **qsv_utils.py** — QSV CLI wrapper (stats, frequency, type inference, validation)
- **prefect_client.py** — Thin wrapper around the Prefect 3 client; the single place the codebase touches `prefect.*`
- **jinja2_helpers.py** — FormulaProcessor and custom filters/functions for metadata formulas
- **datastore_utils.py** — PostgreSQL datastore operations
- **dictionary_stash.py** — On-disk stash/restore of per-column Data Dictionary annotations across job runs/failures
- **spatial_helpers.py** — Shapefile/GeoJSON processing with geometry simplification
- **pii_screening.py** — PII detection with configurable regex patterns
- **helpers.py** — Template helpers for job status display in CKAN UI
- **cli.py** — CKAN CLI command implementations (resubmit, submit, prefect-deploy, migrate-from-rq)
- **logging_utils.py** — Custom TRACE logging level (level 5)
- **interfaces.py** — `IDataPusher` interface for external plugin hooks
- **job_exceptions.py** — Custom exception hierarchy (`DataTooBigError`, `JobError`, `HTTPError`, etc.)
- **utils.py** — Shared helpers (e.g. `utcnow_naive()` for naive-UTC timestamps)
- **logic/action.py** — Actions: `datapusher_submit`, `datapusher_hook`, `datapusher_status`
- **logic/schema.py** — Validation schemas for action functions
- **logic/auth.py** — Authorization functions
- **views.py** — Flask blueprints for web endpoints
- **druf_view.py** — DRUF-specific view handling

### Database Models (model/model.py)

- `Jobs` — Job tracking (job_id, status, data, error, timestamps)
- `Metadata` — Formula evaluation results storage
- `Logs` — Detailed processing logs
- `get_job_details()` — Helper function for retrieving job info

### Formula System

Formulas in scheming YAML have access to three namespaces:
- `dpps` — Summary statistics per field (type, min/max, cardinality, stddev, etc.)
- `dppf` — Frequency tables per field (top N values with counts)
- `dpp` — Inferred metadata (RECORD_COUNT, DATE_FIELDS, LAT_FIELD, LON_FIELD, etc.)

Formula types:
- `formula` — Evaluated and assigned to the field immediately
- `suggestion_formula` — Stored in the `dpp_suggestions` field for UI suggestions

## Coding Conventions

- **Python 3.10+** — Uses `from __future__ import annotations` throughout
- **Import organization** — stdlib → third-party → CKAN → local; `import ckan.plugins as p`, `tk = p.toolkit`
- **Type hints** — Used throughout; `from typing import Any, Optional`, etc.
- **Docstrings** — Google-style
- **Naming** — snake_case functions, UPPERCASE constants, PascalCase classes
- **Logging** — Custom TRACE level (5) via `logging_utils.py`; f-string log messages; pipeline stages use `ProcessingContext.logger`
- **Error handling** — Custom exception hierarchy in `job_exceptions.py`
- **Linting** — Flake8 with E501 disabled (long lines allowed): `# flake8: noqa: E501`
- **CI** — `.github/workflows/` (`main.yml`, `ci.yml`) runs unit + integration tests

## External Dependencies

- **Python 3.10, 3.11, 3.12, 3.13**
- **qsv v20.1.0+** — Must be installed at path specified by `ckanext.datapusher_plus.qsv_bin` (`MINIMUM_QSV_VERSION` is enforced at startup)
- **CKAN 2.10+** with ckanext-scheming
- **PostgreSQL** datastore
- **Prefect 3.7+** — Orchestrates the v3.0 ingestion flow (replaces the v2 RQ-based job runner; `migrate-from-rq` handles the upgrade)

## Configuration Reference

Key settings in `ckan.ini` (see config.py and config_declaration.yaml for the full list):
- `ckanext.datapusher_plus.qsv_bin` — Path to qsv binary
- `ckanext.datapusher_plus.formats` — Supported file formats
- `ckanext.datapusher_plus.preview_rows` — Number of preview rows; `0` = load all rows (default: 0)
- `ckanext.datapusher_plus.auto_index_threshold` — Cardinality threshold for auto-indexing
- `ckanext.datapusher_plus.prefer_dmy` — Date format preference (DMY vs MDY)
- `ckanext.datapusher_plus.enable_druf` — Enable DRUF workflow
- `ckanext.datapusher_plus.enable_form_redirect` — Enable IFormRedirect interface
