# Suggested Commands

> Host OS: **Darwin (macOS)**. Project root: `/Users/joelnatividad/GitHub/datapusher-plus`.

## Testing

```bash
# Run the full unit suite (integration tests need the docker-compose stack)
pytest tests/ --ignore=tests/integration

# Run one file
pytest tests/test_qsv_v20_regression.py

# Coverage of the extension package — note `-p pytest_cov`: the unit
# suite needs PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 to avoid CKAN's pytest
# plugin calling make_app() in pytest_sessionstart, which means `--cov`
# is an unrecognized argument unless pytest-cov is loaded explicitly.
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 QSV_BIN=/path/to/qsvdp \
  CKAN_INI=/srv/app/src/ckan/test-core.ini \
  pytest -p pytest_cov --cov=ckanext/datapusher_plus tests/ \
  --ignore=tests/integration

# Debug a failure with IPython's pdb (preset via pyproject.toml addopt)
pytest --pdbcls=IPython.terminal.debugger:TerminalPdb tests/

# Integration tests (require scripts/integration-up beforehand)
INTEGRATION=1 CKAN_URL=http://localhost:5050 pytest tests/integration/ -v

# JavaScript suite (Vitest + jsdom, host-side)
npm install                     # first time only
npm test                        # one-shot (alias for npx vitest run)
npm run test:watch              # watch mode
```

> **Pytest config lives in `pyproject.toml`** under
> `[tool.pytest.ini_options]` (warning filters + the IPython `--pdbcls`
> addopt). It moved from `setup.cfg` during the v3.0 refactor — comments
> referencing `setup.cfg` for pytest config are stale.

## Installing dependencies
```bash
pip install -r requirements.txt          # runtime
pip install -r requirements-dev.txt      # dev/test extras (pytest, pytest-cov, httpretty)
pip install -e .                          # editable install of the extension
pip install -e ".[dev]"                  # editable + dev extras
```

## CKAN CLI (run inside the CKAN environment)
```bash
# Resubmit all updated datastore resources
ckan -c /etc/ckan/default/ckan.ini datapusher_plus resubmit -y

# Submit one dataset's resources
ckan -c /etc/ckan/default/ckan.ini datapusher_plus submit {dataset_id}

# Register / update the DP+ flow as a Prefect deployment (idempotent)
ckan -c /etc/ckan/default/ckan.ini datapusher_plus prefect-deploy

# One-shot v2 (RQ) → v3 (Prefect) migration
ckan -c /etc/ckan/default/ckan.ini datapusher_plus migrate-from-rq

# Apply Alembic migrations for this extension
ckan -c /etc/ckan/default/ckan.ini db upgrade -p datapusher_plus
```

## Linting / formatting
No formatter is enforced; flake8 is the implicit linter with `E501`
disabled. There is **no** project-level `make lint`, `ruff`, or `black`
config — match existing style.

## CI

- `.github/workflows/test.yml` — **Unit Tests** (PR #326). Runs the full
  unit suite (`pytest tests/ --ignore=tests/integration`) inside
  `ckan/ckan-dev:2.11` on push to `main`/`dev` and PR to `main`. Python
  3.10 only (the only Python the dev image ships). Mirrors the
  `dpp-test` container setup: geo libs, qsv 20.1.0, `PYTEST_DISABLE_PLUGIN_AUTOLOAD`
  / `QSV_BIN` / `CKAN_INI` envs.
- `.github/workflows/ci.yml` — **DataPusher+ Integration CI**. Runs the
  qsv contract regression (`test_qsv_v20_regression.py`) + the
  integration suite on push to `main`/`dev` and PR to `main`. Does NOT
  run the full unit suite.
- `.github/workflows/main.yml` — **Automated DataPusher+ Testing Run**.
  `workflow_dispatch`-only manual end-to-end run.
- `.github/workflows/codeql-analysis.yml` — CodeQL.
- `.github/workflows/python-publish.yml` — package publish.

> The JS suite is **not** in CI; it's a host-side `npm test`.

## Common git operations
```bash
git status
git diff
git log --oneline -20
git checkout -b feature/<name>
gh pr create
gh pr view <n>
gh pr checks <n>
```

## macOS-specific system command notes
The system is **Darwin**, so a few BSD-vs-GNU gotchas:
- `sed -i` requires an empty string argument: `sed -i '' 's/foo/bar/' file`
  (not `sed -i 's/.../'`). Prefer the Edit tool anyway.
- `find` accepts BSD flags; long-form `-iregex` etc. still work, but
  `-printf` does not — use `-exec` or `xargs`.
- `date` flags differ from GNU coreutils (`date -v-1d` for "yesterday"
  instead of `date -d 'yesterday'`).
- `readlink -f` is not BSD-native; use `greadlink -f` (from `coreutils`)
  or `python -c 'import os,sys; print(os.path.realpath(sys.argv[1]))'`.
- `ls` colors via `ls -G`, not `--color`.
- Use `pbcopy` / `pbpaste` for clipboard.
- Prefer `rg` (ripgrep) and `fd` if installed; otherwise `grep -R` / `find`.

## qsv (the runtime dependency)
- Binary path configured by `ckanext.datapusher_plus.qsv_bin` in `ckan.ini`.
- Must be **qsv v20.1.0+** (`MINIMUM_QSV_VERSION`).
- Local invocation goes through `ckanext/datapusher_plus/qsv_utils.py`.
- CI installs from the dathere/qsv release archive
  (`qsv-${QSV_VER}-x86_64-unknown-linux-musl.zip`) to `/usr/local/bin/qsvdp`.
