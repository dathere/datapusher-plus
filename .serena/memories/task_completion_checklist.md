# Task Completion Checklist

Before declaring a code task "done" on datapusher-plus:

## 1. Run the tests
```bash
pytest tests/
```
If the change is scoped, run the relevant test file first (e.g. `pytest tests/test_unit.py`), then the full suite if time allows. Use coverage when relevant:
```bash
pytest --cov=ckanext/datapusher_plus tests/
```

## 2. Lint
There is no enforced formatter. The project uses flake8 with E501 disabled. If you have flake8 installed, run it on touched files:
```bash
flake8 ckanext/datapusher_plus/<changed_file>.py --ignore=E501
```
Don't introduce trailing whitespace, unused imports, or undefined names.

## 3. Verify symbol-level edits
When using Serena's symbolic edits (`replace_symbol_body`, `insert_*_symbol`), re-open the file with `get_symbols_overview` afterwards to confirm:
- No duplicated tails or lost function bodies.
- Symbol boundaries (decorators, async, type hints) preserved.

## 4. LSP / Serena diagnostics
After edits, prefer LSP/Serena navigation to confirm references still resolve. If you renamed or moved a symbol, run `find_referencing_symbols` on it and update callers (this project does not enforce backward-compat shims unless asked).

## 5. Pipeline stage changes specifically
If you touched anything under `ckanext/datapusher_plus/jobs/stages/` or `jobs/pipeline.py`:
- Confirm `ProcessingContext` invariants still hold (the logger, run-id, paths).
- Confirm `BaseStage` contract is respected (entry/exit hooks if defined there).
- Check that `jobs_legacy.py` was NOT modified — it is preserved for reference.

## 6. Database / model changes
If `model/model.py` changed:
- Create a new Alembic migration under `ckanext/datapusher_plus/migration/datapusher_plus/`.
- Document the migration in CHANGELOG.md (only if the user asks).

## 7. Config changes
If you added a setting:
- Add it to `config.py`.
- Declare it in `config_declaration.yaml` (CKAN 2.10+).
- Reference it from `CONFIG.md` only when the user asks.

## 8. Documentation
**Do NOT** create or update `*.md` / README files unless the user explicitly asks. The CLAUDE.md in the repo is the source of truth for AI workflow.

## 9. Git
- Don't commit unless explicitly told to.
- Don't run `git add -A` / `git add .` — stage by name.
- Don't push without an explicit request.
- Never `--amend` published commits; create a new commit instead.

## 10. Sanity check the assumptions
- New file >10MB inputs? Verify behavior with qsv stats cache (`mcp__qsv__qsv_stats` if testing data wrangling).
- Did the change cross a Python-version boundary (3.10 → 3.13)? Avoid 3.11-only syntax until verified.
- Touched CKAN plugin interfaces? Update the `p.implements(...)` list in `plugin.py` to match.
