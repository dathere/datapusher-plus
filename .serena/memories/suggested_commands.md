# Suggested Commands

> Host OS: **Darwin (macOS)**. Project root: `/Users/joelnatividad/GitHub/datapusher-plus`.

## Testing
```bash
# Run the full test suite
pytest tests/

# Run one file
pytest tests/test_unit.py

# Run with coverage of the extension package
pytest --cov=ckanext/datapusher_plus tests/

# Debug a failure with IPython's pdb (configured globally in setup.cfg)
pytest --pdbcls=IPython.terminal.debugger:TerminalPdb tests/
```
Pytest defaults (from `setup.cfg`): SQLAlchemy and `DeprecationWarning` warnings are filtered; `--pdbcls=IPython.terminal.debugger:TerminalPdb` is preset.

## Installing dependencies
```bash
pip install -r requirements.txt          # runtime
pip install -r requirements-dev.txt      # dev/test extras (pytest, pytest-cov, httpretty)
pip install -e .                          # editable install of the extension
pip install -e ".[dev]"                  # editable + dev extras
```

## CKAN CLI (run inside the CKAN environment)
```bash
# Resubmit all resources to datapusher
ckan -c /etc/ckan/default/ckan.ini datapusher_plus resubmit -y

# Submit one dataset's resources
ckan -c /etc/ckan/default/ckan.ini datapusher_plus submit {dataset_id}

# Apply Alembic migrations for this extension
ckan -c /etc/ckan/default/ckan.ini db upgrade -p datapusher_plus
```

## Linting / formatting
No formatter is enforced; flake8 is the implicit linter with `E501` disabled. There is **no** project-level `make lint`, `ruff`, or `black` config — match existing style.

## CI
- Integration workflow: `.github/workflows/main.yml` (manual dispatch; spins up CKAN 2.11 + Solr + Postgres + Redis containers).
- Unit-test workflow: `.github/workflows/test.yml` (note: this file targets older Python versions — read carefully before relying on it).
- CodeQL: `.github/workflows/codeql-analysis.yml`.
- Publish: `.github/workflows/python-publish.yml`.

## Common git operations
```bash
git status
git diff
git log --oneline -20
git checkout -b feature/<name>
gh pr create   # GitHub CLI
gh pr view <n>
gh pr checks <n>
```

## macOS-specific system command notes
The system is **Darwin**, so a few BSD-vs-GNU gotchas:
- `sed -i` requires an empty string argument: `sed -i '' 's/foo/bar/' file` (not `sed -i 's/.../'`). Prefer the Edit tool anyway.
- `find` accepts BSD flags; long-form `-iregex` etc. still work, but `-printf` does not — use `-exec` or `xargs`.
- `date` flags differ from GNU coreutils (`date -v-1d` for "yesterday" instead of `date -d 'yesterday'`).
- `readlink -f` is not BSD-native; use `greadlink -f` (from `coreutils`) or `python -c 'import os,sys; print(os.path.realpath(sys.argv[1]))'`.
- `ls` colors via `ls -G`, not `--color`.
- Use `pbcopy` / `pbpaste` for clipboard.
- Prefer `rg` (ripgrep) and `fd` if installed; otherwise `grep -R` / `find`.

## Quick exploration aliases (none configured; standard tools)
```bash
ls ckanext/datapusher_plus/
ls ckanext/datapusher_plus/jobs/stages/
rg "datapusher_plus_to_datastore" ckanext/   # if rg available
grep -R "datapusher_plus_to_datastore" ckanext/
```

## qsv (the runtime dependency)
- Binary path configured by `ckanext.datapusher_plus.qsv_bin` in `ckan.ini`.
- Must be **qsv v4.0.0+**. The CI uses `QSV_VER=7.1.0` (set in `main.yml`).
- Local invocation goes through `ckanext/datapusher_plus/qsv_utils.py`.
