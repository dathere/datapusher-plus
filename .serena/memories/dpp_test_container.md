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

## Known result (as of 2026-05-17)
**127/127 unit tests pass** on `main` (post-PR-#298 / #299 / #300 merges).
Earlier handoff noted a flow-test that pre-imported a CKAN context; that
path is no longer in the unit suite.

## If the container is gone (Docker restart / removed)
- Restart: `docker start dpp-test` (state persists across stops).
- Recreate: `docker run -d --name dpp-test --platform linux/amd64 --user root
  -v <repo>:/repo -w /repo ckan/ckan-dev:2.11 sleep infinity`, then re-run
  the `ci.yml`-style install (apt geo libs → `pip install GDAL==$(gdal-config
  --version)` → `pip install -r requirements.txt -r requirements-dev.txt -e .`
  → download qsv 20.0.0 musl zip → `qsvdp` to `/usr/local/bin/`).

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
