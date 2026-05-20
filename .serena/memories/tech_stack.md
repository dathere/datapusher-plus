# Tech Stack

## Language / Runtime
- Python 3.10+ (CI tests run inside `ckan/ckan-dev:2.11` container, default CKAN_VERSION=2.11)
- Targets Python 3.10 / 3.11 / 3.12 / 3.13

## Frameworks / Platforms
- **CKAN 2.10+** plugin (extends `p.SingletonPlugin`, implements many CKAN interfaces: `IConfigurer`, `IConfigurable`, `IActions`, `IAuthFunctions`, `IPackageController`, `IResourceUrlChange`, `IResourceController`, `ITemplateHelpers`, `IBlueprint`, `IClick`, and conditionally `IFormRedirect`).
- **Flask** blueprints (`views.py`, `druf_view.py`) for web endpoints.
- **Prefect 3.7+** for background job orchestration (`prefect_flow.py`, `prefect_client.py`, `subflows.py`). The integration stack uses a separately-built worker image from `Dockerfile.worker`.
- **PostgreSQL** datastore using direct `COPY` for ingestion.
- **ckanext-scheming** integration for declarative dataset/resource schemas with Jinja2 formulas (`formula` and `suggestion_formula`).

## Core Libraries (runtime, requirements.txt)
- `semver==3.0.4`
- `datasize==1.0.0`
- `jinja2>=3.1.4`
- `fiona==1.10.1` (shapefile / geo I/O)
- `pandas==2.2.3`
- `shapely==2.1.0`
- `pyproj>=3.7.1`
- `prefect>=3.7,<3.8`
- `blake3>=1.0.7` — default file-hash algorithm (PR #309, issue #221).
  Pure-Python fallback is in stdlib hashlib for sha256/md5; blake3 is
  the only one that needs an external package. The `b3sum` CLI is also
  installed in the worker / CI / `dpp-test` containers — used as the
  external binary path for blake3 file hashing.
- `babel>=2.9` — used by `AnalysisStage._normalize_locale_numbers` for
  CLDR-backed locale-aware number parsing (PR #320, issue #112).
  Already transitive via CKAN's i18n stack; declared explicitly because
  DP+ imports `babel.numbers` directly.

## Dev / Test (requirements-dev.txt)
- `pytest`
- `pytest-cov`
- `httpretty==1.1.4`

## External CLI / Tooling
- **qsv 20.1.0+** (Rust-based CSV toolkit) — invoked via `qsv_utils.py`,
  path is `ckanext.datapusher_plus.qsv_bin`. PR #315 bumped the pin from
  20.0.0 → 20.1.0 (no breaking changes). `qsv` subcommands DP+ uses:
  `stats`, `frequency`, `safenames`, `index`, `count`, `slice`,
  `datefmt`, `searchset`, `replace`, `describegpt`, `excel`,
  `geoconvert`, `geocode`, `input`, `headers`, `extdedup`, `sortcheck`,
  `validate`.
- **GDAL / geospatial system libs** (libxml2, libxslt1, libpq, libgeos,
  libproj, libspatialindex, gdal-bin, libgdal-dev) — required for
  `fiona` / `shapely` / `pyproj` in CI; installed via apt in
  `.github/workflows/main.yml`.
- **`b3sum`** — external blake3 CLI (PR #309). Installed in worker, CI,
  and `dpp-test` containers.
- **uchardet** — used for encoding detection (installed in CI).
- **ffmpeg** (host-side only, optional) — used once for #321 to convert
  the slide-11 MOV to GIF for the README asset.

## Build System
- `setuptools >= 62.6.0` via `pyproject.toml`.
- Packages discovered with `find` (excluding `tests*`).
- `requirements.txt` is the dynamic source of dependencies; `requirements-dev.txt` feeds the `dev` optional-dependencies extra.

## Integration stack (separate from unit-test `dpp-test` container)
Docker compose stack at `docker-compose.integration.yaml`. 6 services:
**ckan** (2.11), **prefect-server**, **prefect-worker** (built from
`Dockerfile.worker`), **postgres** (15), **redis** (7),
**solr** (`ckan/ckan-solr:2.11-solr9`). Managed via
`scripts/integration-up` / `scripts/integration-down`. CKAN runs on
`localhost:5050` (5000 collides with macOS AirPlay).
