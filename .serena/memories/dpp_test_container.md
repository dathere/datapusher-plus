# `dpp-test` — pre-built ckan-dev container for DP+ work

A persistent Docker container kept around so DP+ tests/builds can run
without a local CKAN env. **Reuse it for future DP+ work** instead of
rebuilding.

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
- `tests/integration/` excluded — needs the full Prefect+CKAN+Postgres+Solr
  stack (what `main.yml` / `ci.yml` bring up).

## Known result (as of 2026-05-15)
**42/43 unit tests pass.** The 1 error
(`test_flow_completes_and_marks_job_complete`) is pre-existing: the first
flow test triggers an import-time `_bootstrap_ckan_app_context()` →
`make_app()` that needs a full CKAN env. See `HANDOFF.md` → *Known
cosmetic / minor* for the full diagnosis and the deferred fix.

## If the container is gone (Docker restart / removed)
- Restart: `docker start dpp-test` (state persists across stops).
- Recreate: `docker run -d --name dpp-test --platform linux/amd64 --user root
  -v <repo>:/repo -w /repo ckan/ckan-dev:2.11 sleep infinity`, then re-run
  the `ci.yml`-style install (apt geo libs → `pip install GDAL==$(gdal-config
  --version)` → `pip install -r requirements.txt -r requirements-dev.txt -e .`
  → download qsv 20.0.0 musl zip → `qsvdp` to `/usr/local/bin/`).
