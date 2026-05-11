# Serena Usage Notes for this Project

## Activation
Project is named **datapusher-plus** at `/Users/joelnatividad/GitHub/datapusher-plus`. Language: Python only. File encoding: utf-8.

## Preferred entry points for navigation
- **Plugin entry**: `ckanext/datapusher_plus/plugin.py` → `DatapusherPlusPlugin`
- **Pipeline entry**: `ckanext/datapusher_plus/jobs/pipeline.py` → `datapusher_plus_to_datastore`
- **Shared state**: `ckanext/datapusher_plus/jobs/context.py` → `ProcessingContext`
- **Stage base**: `ckanext/datapusher_plus/jobs/stages/base.py` → `BaseStage`
- **Actions API surface**: `ckanext/datapusher_plus/logic/action.py` (e.g. `datapusher_submit`, `datapusher_hook`, `datapusher_status`)
- **Data models**: `ckanext/datapusher_plus/model/model.py` (`Jobs`, `Metadata`, `Logs`)

## Workflow recipes
- Understanding a CKAN action handler: `find_symbol` with name_path `datapusher_submit` (or other action) in `logic/action.py`, `include_body=True`.
- Adding a pipeline stage:
  1. `get_symbols_overview` on `jobs/stages/base.py`.
  2. `find_symbol` on `BaseStage`, `include_body=True`.
  3. Create new module under `jobs/stages/` using `replace_symbol_body` on a placeholder, or write a new file with the editor.
  4. Hook the new stage into the pipeline by editing `jobs/pipeline.py` with `replace_symbol_body` on the orchestration function.
- Renaming or moving a function: prefer `rename` / `move` Serena tools so all references update. Use `find_referencing_symbols` first to scope blast radius.
- Inspecting how a CKAN interface is wired: `find_symbol` on `DatapusherPlusPlugin` with `depth=1`, then drill into the method that implements the interface hook (`update_config`, `get_actions`, etc.).

## Known oddities
- `plugin.py` defines `DatastoreException` twice (lines around the top) — likely vestigial; leave alone unless asked to clean up.
- `jobs.py` and `jobs_legacy.py` both exist at the package root in addition to the modular `jobs/` package. The legacy module is kept for reference; do not edit it.
- `# flake8: noqa: E501` is conventional at the top of many modules; preserve when editing.
