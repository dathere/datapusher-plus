# Task Completion Checklist

Before declaring a code task "done" on datapusher-plus:

## 1. Run the tests
```bash
pytest tests/ --ignore=tests/integration
```
If the change is scoped, run the relevant test file first
(e.g. `pytest tests/test_qsv_v20_regression.py`), then the full unit
suite if time allows. Use coverage when relevant — remember the
`-p pytest_cov` because `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1` is set:
```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 QSV_BIN=/path/to/qsvdp \
  CKAN_INI=/srv/app/src/ckan/test-core.ini \
  pytest -p pytest_cov --cov=ckanext/datapusher_plus tests/ \
  --ignore=tests/integration
```

> Integration tests need the docker-compose stack — see
> `mem:dpp_test_container` for the full command + stack notes.

## 2. Lint
There is no enforced formatter. The project uses flake8 with E501
disabled. If you have flake8 installed, run it on touched files:
```bash
flake8 ckanext/datapusher_plus/<changed_file>.py --ignore=E501
```
Don't introduce trailing whitespace, unused imports, or undefined names.

## 3. Verify symbol-level edits
When using Serena's symbolic edits (`replace_symbol_body`,
`insert_*_symbol`), re-open the file with `get_symbols_overview`
afterwards to confirm:
- No duplicated tails or lost function bodies.
- Symbol boundaries (decorators, async, type hints) preserved.

## 4. LSP / Serena diagnostics
After edits, prefer LSP/Serena navigation to confirm references still
resolve. If you renamed or moved a symbol, run `find_referencing_symbols`
on it and update callers (this project does not enforce
backward-compat shims unless asked).

## 5. Pipeline/flow stage changes specifically
If you touched anything under `ckanext/datapusher_plus/jobs/stages/` or
`jobs/prefect_flow.py`:
- Confirm `ProcessingContext` invariants still hold (the logger, run-id,
  paths, `resource` dict).
- Confirm `BaseStage` contract is respected (entry/exit hooks if defined
  there).
- If a stage joins/leaves the datastore-mutating set, update the
  `with transaction()` block in `datapusher_plus_flow` accordingly.
- Verify the matching `@task` wrapper in `prefect_flow.py` was updated
  (per-stage `@task` functions delegate to `BaseStage.process()` —
  they should not embed business logic).

## 6. Database / model changes
If `model/model.py` changed:
- Create a new Alembic migration under
  `ckanext/datapusher_plus/migration/datapusher_plus/`.
- Document the migration in CHANGELOG.md (only if the user asks).

## 7. Config changes
If you added a setting:
- Add it to `config.py`.
- Declare it in `config_declaration.yaml` (CKAN 2.10+).
- Keep the config key string **lowercase** with the
  `ckanext.datapusher_plus.` prefix — the spatial-tolerance key bug
  (PR #326) and the `datastore_plus` typo (PR #324) both stemmed from
  drifting from this rule.
- Reference it from `CONFIG.md` / `README.md` only when the user asks.
- Consider an AST drift-guard test (the
  `test_issue_142_auto_index_threshold.py` /
  `test_issue_112_decimal_comma.py` /
  `test_pii_screening_config_key.py` / `test_formats_config.py` pattern):
  AST-parse `config.py` + YAML-parse `config_declaration.yaml` to assert
  the inline fallback and the declaration default/key agree.

## 8. Documentation
**Do NOT** create or update `*.md` / README files unless the user
explicitly asks. The CLAUDE.md in the repo (refreshed in PR #325 for
v3.0) is the source of truth for AI workflow.

## 9. Git
- Don't commit unless explicitly told to.
- Don't run `git add -A` / `git add .` — stage by name.
- Don't push without an explicit request.
- Never `--amend` published commits; create a new commit instead.

## 10. Sanity check the assumptions
- New file >10MB inputs? Verify behavior with qsv stats cache
  (`mcp__qsv__qsv_stats` if testing data wrangling).
- Did the change cross a Python-version boundary (3.10 → 3.13)? Avoid
  3.11-only syntax until verified.
- Touched CKAN plugin interfaces? Update the `p.implements(...)` list
  in `plugin.py` to match.
