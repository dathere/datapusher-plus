# DataPusher+ (datapusher-plus)

## Purpose
DataPusher+ is a **CKAN extension (v2.0.0)** for ultra-fast, robust data ingestion into CKAN's datastore. It replaces the legacy Datapusher webservice and combines the speed/robustness of `ckanext-xloader` with the data-type guessing of Datapusher — supercharged with metadata inference/suggestion via Jinja2 formulas defined in scheming YAML.

Key differentiators:
- **Guaranteed type inference** — scans the entire file (not first N rows) via [qsv](https://github.com/dathere/qsv), a Rust CSV toolkit.
- **PostgreSQL COPY** for direct datastore loading (no API overhead).
- **Jinja2 formula system** for metadata inference/suggestion (`formula` and `suggest_formula` in scheming YAML).
- **DRUF** (Dataset Resource Upload First) workflow support.
- Three formula namespaces available: `dpps` (per-field stats), `dppf` (per-field freq tables), `dpp` (inferred metadata: RECORD_COUNT, DATE_FIELDS, LAT_FIELD, LON_FIELD, etc.).
- No longer a separate webservice — it is now a full CKAN extension (`ckan.plugins` entry point: `datapusher_plus = ckanext.datapusher_plus.plugin:DatapusherPlusPlugin`).

## Repository
- GitHub: https://github.com/dathere/datapusher-plus
- License: AGPL-3.0-or-later
- Maintainer: datHere Engineering <info@dathere.com>
- Current branch defaults: `main`
- This project lives at `/Users/joelnatividad/GitHub/datapusher-plus` on the host (Darwin/macOS).

## External runtime dependencies
- **Python 3.10, 3.11, 3.12, 3.13** (`requires-python = ">=3.10"`)
- **qsv v4.0.0+** (path configured via `ckanext.datapusher_plus.qsv_bin`)
- **CKAN 2.10+** with `ckanext-scheming`
- **PostgreSQL** datastore
- **RQ (Redis Queue)** for background job processing
