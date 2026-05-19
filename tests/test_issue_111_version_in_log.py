# -*- coding: utf-8 -*-
"""
Regression coverage for issue #111: surface the DP+ version in the
flow log banner.

The reporter asked for the version to appear in the post-job log so
operators can confirm which DP+ is running without having to open
the CKAN container and inspect package metadata. The implementation
surfaces it in two places:

1. **Startup banner** — emitted at the top of ``datapusher_plus_flow``
   before any stage runs. Format: ``"DATAPUSHER+ v<X.Y.Z> starting
   flow for resource <id>"``. This is the one-line grep-target an
   operator needs.

2. **Capstone "JOB DONE!" line** — emitted just before the success-path
   ``return None``. Format: ``"DATAPUSHER+ v<X.Y.Z> JOB DONE! Total
   elapsed time: <N> seconds."``. Matches the wording the reporter
   suggested verbatim (originally a v1.x log line that was lost in the
   v2/v3 rewrite — restored on the new pipeline).

The version itself is resolved via ``importlib.metadata`` from the
installed package, so it stays in lockstep with ``pyproject.toml``
rather than drifting via a hand-maintained constant.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# __version__ is exposed and matches pyproject.toml
# ---------------------------------------------------------------------------


def test_dpp_version_attribute_is_exposed():
    """``ckanext.datapusher_plus.__version__`` must be importable."""
    pytest.importorskip("ckanext.datapusher_plus")
    from ckanext.datapusher_plus import __version__

    assert isinstance(__version__, str) and __version__, (
        "ckanext.datapusher_plus.__version__ must be a non-empty string"
    )


def test_dpp_version_matches_pyproject():
    """``__version__`` (resolved via importlib.metadata) must agree with
    ``pyproject.toml:[project].version``.

    Catches a setup-tools-side regression where the installed package
    metadata drifts from the source-of-truth pyproject. Without this
    pin, a botched build could ship a banner reading
    ``"DATAPUSHER+ v0.0.0 starting ..."`` without any test failing.
    """
    pytest.importorskip("ckanext.datapusher_plus")
    from ckanext.datapusher_plus import __version__

    pyproject = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    m = re.search(
        r'^\s*version\s*=\s*"([^"]+)"',
        pyproject,
        re.MULTILINE,
    )
    assert m, "version field not found in pyproject.toml"
    declared = m.group(1)

    # Editable installs that don't register metadata fall back to
    # "unknown" — flag that as a setup issue rather than silently
    # passing the contract test below.
    assert __version__ != "unknown", (
        "ckanext.datapusher_plus.__version__ resolved to 'unknown' — "
        "the editable-install fallback fired. Re-run `pip install -e .` "
        "in the test environment so importlib.metadata can find the "
        "package metadata."
    )
    assert __version__ == declared, (
        f"ckanext.datapusher_plus.__version__ ({__version__!r}) does "
        f"not match pyproject.toml version ({declared!r})"
    )


# ---------------------------------------------------------------------------
# Banner + capstone log lines are wired in the flow
# ---------------------------------------------------------------------------


def test_startup_banner_format_is_wired_in_flow_source():
    """Source-grep pin for the startup banner format.

    Asserts on the literal f-string template in
    ``ckanext/datapusher_plus/jobs/prefect_flow.py`` rather than
    running the full flow (which requires Prefect + a bunch of
    fixtures). A unit-level source pin is enough to catch the most
    likely regression — someone "cleaning up" the import or the
    wording without realizing it's user-facing.

    Tolerates whitespace / line-break variations: we grep for the
    two anchoring fragments ``DATAPUSHER+ v`` and ``starting flow
    for resource``, both of which must appear together in the file.
    """
    flow_py = (
        REPO_ROOT
        / "ckanext"
        / "datapusher_plus"
        / "jobs"
        / "prefect_flow.py"
    )
    text = flow_py.read_text(encoding="utf-8")
    assert "DATAPUSHER+ v" in text, (
        "Startup banner template missing 'DATAPUSHER+ v' anchor — "
        "issue #111 banner may have been removed."
    )
    assert "starting flow for resource" in text, (
        "Startup banner template missing 'starting flow for resource' "
        "anchor — issue #111 banner wording may have drifted."
    )


def test_capstone_job_done_format_is_wired_in_flow_source():
    """Source-grep pin for the capstone JOB DONE banner.

    Same rationale as above: we don't need to drive the full flow to
    catch the regression class this test is for ("someone deleted the
    banner during a refactor"). Asserts that the literal ``JOB DONE!``
    template and the elapsed-time anchor are both present.
    """
    flow_py = (
        REPO_ROOT
        / "ckanext"
        / "datapusher_plus"
        / "jobs"
        / "prefect_flow.py"
    )
    text = flow_py.read_text(encoding="utf-8")
    assert "JOB DONE!" in text, (
        "Capstone banner ('JOB DONE!') missing — issue #111 capstone "
        "may have been removed."
    )
    assert "Total elapsed time:" in text, (
        "Capstone banner missing 'Total elapsed time:' anchor — "
        "issue #111 capstone wording may have drifted from the "
        "reporter's verbatim request."
    )
