# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DataPusher+ is a CKAN extension (v2.0.0) for ultra-fast, robust data ingestion into CKAN's datastore. It replaces the legacy Datapusher webservice with a full CKAN extension that leverages [qsv](https://github.com/dathere/qsv) (a Rust-based CSV data-wrangling toolkit) for blazing-fast type inference and data analysis.

**Key differentiators:**
- Guaranteed data type inference by scanning entire files (not just first few rows)
- PostgreSQL COPY for direct data loading (no API overhead)
- Jinja2 formula system for metadata inference/suggestion (`formula` and `suggest_formula` in scheming YAML)
- DRUF (Dataset Resource Upload First) workflow support

## Build & Test Commands

```bash
# Run all tests
pytest tests/

# Run specific test file
pytest tests/test_unit.py

# Run with coverage
pytest --cov=ckanext/datapusher_plus tests/

# Debug with IPython
pytest --pdbcls=IPython.terminal.debugger:TerminalPdb tests/
```

## CKAN CLI Commands

```bash
# Resubmit all resources to datapusher
ckan -c /etc/ckan/default/ckan.ini datapusher_plus resubmit -y

# Submit specific package resources
ckan -c /etc/ckan/default/ckan.ini datapusher_plus submit {dataset_id}

# Database migrations
ckan -c /etc/ckan/default/ckan.ini db upgrade -p datapusher_plus
```

## Architecture

### Pipeline Stage Pattern (v2.0)

The refactored jobs module uses a modular stage-based pipeline in `ckanext/datapusher_plus/jobs/`:

```
pipeline.py          → Main orchestration, entry point (datapusher_plus_to_datastore)
context.py           → ProcessingContext state management across stages
stages/
  base.py            → Abstract BaseStage class
  download.py        → File download with retries, proxy support, timeout handling
  format_converter.py → Excel/ODS/Shapefile/GeoJSON/ZIP → CSV conversion
  validation.py      → RFC-4180 CSV validation, encoding detection/normalization
  analysis.py        → QSV-based type inference, summary stats, frequency tables
  database.py        → PostgreSQL COPY operations, smartint type selection
  indexing.py        → Auto-index creation based on cardinality/dates
  formula.py         → Jinja2 formula evaluation (package/resource metadata)
  metadata.py        → Datastore resource dict updates, dpp_suggestions
```

### Key Modules

- **plugin.py** — CKAN plugin entry point, implements IConfigurer, IConfigurable, IActions, IAuthFunctions, IPackageController, IResourceUrlChange, IResourceController, ITemplateHelpers, IBlueprint, IClick (+ IFormRedirect conditionally)
- **config.py** — ~50 configuration parameters (all `ckanext.datapusher_plus.*` settings)
- **config_declaration.yaml** — CKAN 2.10+ declarative config definitions
- **qsv_utils.py** — QSV CLI wrapper (stats, frequency, type inference, validation)
- **jinja2_helpers.py** — FormulaProcessor and custom filters/functions for metadata formulas
- **datastore_utils.py** — PostgreSQL datastore operations
- **spatial_helpers.py** — Shapefile/GeoJSON processing with geometry simplification
- **pii_screening.py** — PII detection with configurable regex patterns
- **helpers.py** — Template helpers for job status display in CKAN UI
- **cli.py** — CKAN CLI command implementations (resubmit, submit)
- **logging_utils.py** — Custom TRACE logging level (level 5)
- **interfaces.py** — `IDataPusher` interface for external plugin hooks
- **job_exceptions.py** — Custom exception hierarchy (`DataTooBigError`, `JobError`, `HTTPError`, etc.)
- **logic/action.py** — Actions: `datapusher_submit`, `datapusher_hook`, `datapusher_status`
- **logic/schema.py** — Validation schemas for action functions
- **logic/auth.py** — Authorization functions
- **views.py** — Flask blueprints for web endpoints
- **druf_view.py** — DRUF-specific view handling
- **jobs_legacy.py** — Legacy monolithic implementation (preserved for reference)

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
- `formula` — Evaluated and assigned to field immediately
- `suggest_formula` — Stored in `dpp_suggestions` field for UI suggestions

## Coding Conventions

- **Python 3.10+** — Uses `from __future__ import annotations` throughout
- **Import organization** — stdlib → third-party → CKAN → local; `import ckan.plugins as p`, `tk = p.toolkit`
- **Type hints** — Used throughout; `from typing import Any, Optional`, etc.
- **Docstrings** — Google-style
- **Naming** — snake_case functions, UPPERCASE constants, PascalCase classes
- **Logging** — Custom TRACE level (5) via `logging_utils.py`; f-string log messages; pipeline stages use `ProcessingContext.logger`
- **Error handling** — Custom exception hierarchy in `job_exceptions.py`
- **Linting** — Flake8 with E501 disabled (long lines allowed): `# flake8: noqa: E501`
- **CI** — `.github/workflows/main.yml` runs integration tests

## External Dependencies

- **Python 3.10, 3.11, 3.12, 3.13**
- **qsv v4.0.0+** — Must be installed at path specified by `ckanext.datapusher_plus.qsv_bin`
- **CKAN 2.10+** with ckanext-scheming
- **PostgreSQL** datastore
- **RQ (Redis Queue)** for background job processing

## Configuration Reference

Key settings in `ckan.ini` (see config.py and config_declaration.yaml for full list):
- `ckanext.datapusher_plus.qsv_bin` — Path to qsv binary
- `ckanext.datapusher_plus.formats` — Supported file formats
- `ckanext.datapusher_plus.preview_rows` — Number of preview rows (default: 1000)
- `ckanext.datapusher_plus.auto_index_threshold` — Cardinality threshold for auto-indexing
- `ckanext.datapusher_plus.prefer_dmy` — Date format preference (DMY vs MDY)
- `ckanext.datapusher_plus.enable_druf` — Enable DRUF workflow
- `ckanext.datapusher_plus.enable_form_redirect` — Enable IFormRedirect interface
