# Code Style & Conventions

## Python baseline
- **Target Python 3.10+** — uses `from __future__ import annotations` throughout.
- Type hints used widely: `from typing import Any, Optional, Callable, Literal`, etc.
- Google-style docstrings.
- `# encoding: utf-8` and `# flake8: noqa: E501` headers in CKAN-style modules.

## Naming
- `snake_case` for functions and variables.
- `PascalCase` for classes (e.g. `DatapusherPlusPlugin`, `ProcessingContext`, `BaseStage`).
- `UPPERCASE` for module-level constants.
- Stage classes live in `jobs/stages/<concern>.py` and inherit from `BaseStage`.

## Imports
Order:
1. Stdlib
2. Third-party
3. CKAN (`import ckan.plugins as p`, then `tk = p.toolkit`)
4. Local (`import ckanext.datapusher_plus.<...> as alias`)

Idioms seen in `plugin.py`:
```python
import ckan.plugins as p
from ckan.plugins import toolkit as tk   # or: tk = p.toolkit
import ckanext.datapusher_plus.views as views
import ckanext.datapusher_plus.helpers as dph
import ckanext.datapusher_plus.logic.action as action
import ckanext.datapusher_plus.logic.auth as auth
import ckanext.datapusher_plus.cli as cli
```

## Logging
- Standard `logging` module with custom **TRACE level (5)** defined in `logging_utils.py`.
- Pipeline stages use `ProcessingContext.logger` rather than module-level loggers — keeps per-job context.
- Prefer f-string log messages.

## Error handling
- Custom exception hierarchy in `job_exceptions.py`:
  `DataTooBigError`, `JobError`, `HTTPError`, etc.
- Raise specific exceptions from those classes instead of generic `Exception`.

## Linting
- Flake8, with **E501 disabled** project-wide (the `# flake8: noqa: E501` comment is conventional at the top of many files). Long lines are accepted.

## Architectural patterns
- **Pipeline stage pattern** (v2.0): each stage subclasses `BaseStage` and mutates `ProcessingContext`. Add new behaviour as a new stage, not by extending an existing one. Keep stages single-responsibility.
- Prefer editing the modular `jobs/` package — `jobs_legacy.py` is kept only for reference.
- CKAN plugin interfaces are wired in `plugin.py`; new actions go through `logic/action.py` + `logic/schema.py` + `logic/auth.py`.

## Comments
- Keep comments focused on **why**, not what (per default Claude guidance + project style).
- Do not create new `*.md`/README files unless explicitly asked.
