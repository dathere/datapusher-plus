# Tech Stack

## Language / Runtime
- Python 3.10+ (CI tests run inside `ckan/ckan-dev:2.11` container, default CKAN_VERSION=2.11)
- Targets Python 3.10 / 3.11 / 3.12 / 3.13

## Frameworks / Platforms
- **CKAN 2.10+** plugin (extends `p.SingletonPlugin`, implements many CKAN interfaces: `IConfigurer`, `IConfigurable`, `IActions`, `IAuthFunctions`, `IPackageController`, `IResourceUrlChange`, `IResourceController`, `ITemplateHelpers`, `IBlueprint`, `IClick`, and conditionally `IFormRedirect`).
- **Flask** blueprints (`views.py`, `druf_view.py`) for web endpoints.
- **RQ (Redis Queue)** for background job processing.
- **PostgreSQL** datastore using direct `COPY` for ingestion.
- **ckanext-scheming** integration for declarative dataset/resource schemas with Jinja2 formulas.

## Core Libraries (runtime, requirements.txt)
- `semver==3.0.4`
- `datasize==1.0.0`
- `jinja2>=3.1.4`
- `fiona==1.10.1` (shapefile/geo I/O)
- `pandas==2.2.3`
- `shapely==2.1.0`
- `pyproj>=3.7.1`

## Dev / Test (requirements-dev.txt)
- `pytest`
- `pytest-cov`
- `httpretty==1.1.4`

## External CLI / Tooling
- **qsv** (Rust-based CSV toolkit) — invoked via `qsv_utils.py`, path is `ckanext.datapusher_plus.qsv_bin`.
- **GDAL / geospatial system libs** (libxml2, libxslt1, libpq, libgeos, libproj, libspatialindex, gdal-bin, libgdal-dev) — required for `fiona`/`shapely`/`pyproj` in CI; installed via apt in `.github/workflows/main.yml`.
- **uchardet** — used for encoding detection (installed in CI).

## Build System
- `setuptools >= 62.6.0` via `pyproject.toml`.
- Packages discovered with `find` (excluding `tests*`).
- `requirements.txt` is the dynamic source of dependencies; `requirements-dev.txt` feeds the `dev` optional-dependencies extra.
