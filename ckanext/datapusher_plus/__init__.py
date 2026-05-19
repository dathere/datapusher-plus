# -*- coding: utf-8 -*-
"""DataPusher+ — ultra-fast CKAN datastore ingestion.

This module exposes ``__version__`` (resolved via ``importlib.metadata``
from the installed package metadata so it stays in lockstep with
``pyproject.toml`` rather than drifting via a hand-maintained constant).
Surfaced in the per-flow startup banner and the "JOB DONE!" completion
log line — see issue #111.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("datapusher-plus")
except PackageNotFoundError:
    # Editable install in some niche test environments may not register
    # package metadata. Falling back to "unknown" rather than reading
    # pyproject.toml directly keeps this module free of file I/O at
    # import time. The startup banner will read "DATAPUSHER+ vunknown"
    # in that case — recognizable enough to debug without crashing the
    # flow on the way up.
    __version__ = "unknown"

__all__ = ["__version__"]
