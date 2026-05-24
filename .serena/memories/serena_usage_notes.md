# Serena Usage Notes for this Project

## Activation
Project is named **datapusher-plus** at `/Users/joelnatividad/GitHub/datapusher-plus`.
Language: Python only. File encoding: utf-8.

## Preferred entry points for navigation
- **Plugin entry**: `ckanext/datapusher_plus/plugin.py` → `DatapusherPlusPlugin`
- **Flow entry (v3.0)**: `ckanext/datapusher_plus/jobs/prefect_flow.py` →
  `datapusher_plus_flow` (the @flow) and the per-stage @task functions.
  Use this — `jobs/pipeline.py` from the v2 era no longer exists.
- **Shared state**: `ckanext/datapusher_plus/jobs/context.py` → `ProcessingContext`
- **Runtime/flow input**: `ckanext/datapusher_plus/jobs/runtime_context.py` →
  `JobInput` (frozen, JSON-serializable), the per-stage `*Result` dataclasses,
  the `RuntimeContext` ContextVar.
- **Stage base**: `ckanext/datapusher_plus/jobs/stages/base.py` → `BaseStage`
- **Actions API surface**: `ckanext/datapusher_plus/logic/action.py`
  (`datapusher_submit`, `datapusher_hook`, `datapusher_status`)
- **Data models**: `ckanext/datapusher_plus/model/model.py` (`Jobs`,
  `Metadata`, `Logs`)
- **Prefect plumbing**: `prefect_client.py` is the single place CKAN admin
  paths touch `prefect.*`; `jobs/__init__.py` uses PEP 562 lazy
  `__getattr__` to defer the Prefect import so CKAN CLI commands don't
  spin up a Prefect server.

## Workflow recipes
- Understanding a CKAN action handler: `find_symbol` with name_path
  `datapusher_submit` (or other action) in `logic/action.py`,
  `include_body=True`.
- Adding a pipeline stage:
  1. `get_symbols_overview` on `jobs/stages/base.py`.
  2. `find_symbol` on `BaseStage`, `include_body=True`.
  3. Create new module under `jobs/stages/` and add the matching
     `@task` wrapper in `jobs/prefect_flow.py`.
  4. Hook the new task into the flow by editing `datapusher_plus_flow`
     in `jobs/prefect_flow.py` (use `replace_symbol_body`).
- Renaming or moving a function: prefer `rename` / `move` Serena tools so
  all references update. Use `find_referencing_symbols` first to scope
  blast radius.
- Inspecting how a CKAN interface is wired: `find_symbol` on
  `DatapusherPlusPlugin` with `depth=1`, then drill into the method that
  implements the interface hook (`update_config`, `get_actions`, etc.).

## Known oddities
- `plugin.py` defines `DatastoreException` twice (lines around the top) —
  likely vestigial; leave alone unless asked to clean up.
- The v2-era `jobs.py`, `jobs_legacy.py`, and `jobs/pipeline.py` files
  have all been removed; if you see references to them in old docs or
  memories, they are stale.
- `jobs/__init__.py` re-exports `datapusher_plus_to_datastore` as an
  alias for `datapusher_plus_flow` — preserves the v2 import path.
- `# flake8: noqa: E501` is conventional at the top of many modules;
  preserve when editing.
