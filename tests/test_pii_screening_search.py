# -*- coding: utf-8 -*-
"""``screen_for_pii`` against a real qsv, with the default regex set.

Regression coverage for three bugs that together kept PII screening from
working with its default configuration:

* the default regex file lived at the repo root while the code looked for
  it next to ``pii_screening.py`` (quick mode always failed), and full mode
  passed a bare file name that only resolved when the worker's current
  directory was the repo root;
* ``searchset --quick`` exits 1 when nothing matches, so a quick screen of
  a file with no PII failed the job;
* ``JobError`` was called with logging-style arguments, which raised
  ``TypeError`` instead.

The tests run from a temporary directory, as a worker would, so a relative
regex path cannot pass by accident.
"""

import os
import shutil
from pathlib import Path
from unittest import mock

import pytest


def _locate_qsv():
    for c in (os.environ.get("QSV_BIN"), shutil.which("qsvdp"), shutil.which("qsv")):
        if c and Path(c).is_file():
            return c
    return None


QSV_BIN = _locate_qsv()
requires_qsv = pytest.mark.skipif(QSV_BIN is None, reason="needs a qsv binary (QSV_BIN)")


def test_default_regex_file_ships_next_to_the_module():
    pytest.importorskip("ckan")
    from ckanext.datapusher_plus import pii_screening

    assert Path(pii_screening.__file__).with_name("default-pii-regexes.txt").is_file()


@pytest.fixture
def screen(tmp_path, monkeypatch):
    """Run ``screen_for_pii`` on CSV text, in quick or full mode."""
    pytest.importorskip("ckan")
    from ckanext.datapusher_plus import pii_screening, qsv_utils

    monkeypatch.chdir(tmp_path)

    def run(csv_text, quick, abort=True, show_candidates=False, regex_resource=None,
            logger=None):
        csv_path = tmp_path / "data.csv"
        csv_path.write_text(csv_text)
        conf = pii_screening.conf
        with mock.patch.object(qsv_utils.conf, "QSV_BIN", Path(QSV_BIN)), \
             mock.patch.object(conf, "PII_REGEX_RESOURCE_ID", regex_resource), \
             mock.patch.object(conf, "PII_QUICK_SCREEN", quick), \
             mock.patch.object(conf, "PII_FOUND_ABORT", abort), \
             mock.patch.object(conf, "PII_SHOW_CANDIDATES", show_candidates):
            qsv = qsv_utils.QSVCommand(logger=mock.Mock())
            return pii_screening.screen_for_pii(
                str(csv_path), {"id": "res"}, qsv, str(tmp_path), logger or mock.Mock()
            )

    return run


CLEAN = "name,city\nAlice,NYC\nBob,LA\n"
WITH_SSN = "name,ssn\nAlice,none\nBob,123-45-6789\n"


@requires_qsv
@pytest.mark.parametrize("quick", [True, False], ids=["quick", "full"])
def test_a_file_without_pii_passes(screen, quick):
    assert screen(CLEAN, quick) == (False, 0)


@requires_qsv
def test_quick_screen_aborts_on_pii_with_the_row(screen):
    from ckanext.datapusher_plus import utils

    with pytest.raises(utils.JobError, match=r"PII CANDIDATE FOUND on row \d+! Job aborted\."):
        screen(WITH_SSN, quick=True)


@requires_qsv
def test_full_screen_aborts_on_pii_with_the_counts(screen):
    from ckanext.datapusher_plus import utils

    with pytest.raises(
        utils.JobError, match=r"Found 1 PII candidate/s in 1 row/s\."
    ):
        screen(WITH_SSN, quick=False)


@requires_qsv
@pytest.mark.parametrize(
    "quick, message",
    [(True, "Cannot quickly search CSV for PII"), (False, "Cannot search CSV for PII")],
    ids=["quick", "full"],
)
def test_a_failed_search_raises_job_error(screen, quick, message, tmp_path):
    from ckanext.datapusher_plus import pii_screening, utils

    # a regex file qsv cannot read makes searchset itself fail. Moving the module's
    # __file__ points the default regex path (Path(__file__).with_name(...)) at a
    # missing file; patching pathlib.Path itself would also hijack qsv-client,
    # which resolves the qsv binary with Path.absolute().
    assert not (tmp_path / "default-pii-regexes.txt").exists()
    with mock.patch.object(pii_screening, "__file__", str(tmp_path / "pii_screening.py")), \
         pytest.raises(utils.JobError, match=message):
        screen(CLEAN, quick)


@pytest.mark.parametrize(
    "returncode, stderr, expected",
    [
        (1, "", True),  # qsv's text mode prints nothing for "no match"
        (1, "\n", True),
        (1, '{"error":{"kind":"no_match","level":"error","message":"","exit_code":1}}\n', True),
        (1, 'a warning\n{"error":{"kind":"no_match","exit_code":1}}', True),
        (1, '{"error":{"kind":"io","message":"No such file","exit_code":1}}', False),
        (1, "io error: No such file or directory (os error 2)", False),
        (0, "", False),  # a success is not a "no match"
        (2, "", False),
    ],
)
def test_is_searchset_no_match(returncode, stderr, expected):
    pytest.importorskip("ckan")
    import subprocess

    from ckanext.datapusher_plus.pii_screening import _is_searchset_no_match

    result = subprocess.CompletedProcess(args=[], returncode=returncode, stdout="", stderr=stderr)
    assert _is_searchset_no_match(result) is expected


def test_a_missing_custom_regex_resource_is_a_clear_job_error(tmp_path):
    """A configured regex resource that doesn't exist used to leave the regex
    path unbound and crash with NameError."""
    pytest.importorskip("ckan")
    import ckan.plugins.toolkit as tk

    from ckanext.datapusher_plus import pii_screening, utils

    with mock.patch.object(pii_screening.conf, "PII_REGEX_RESOURCE_ID", "no-such-resource"), \
         mock.patch.object(pii_screening.dsu, "get_resource", side_effect=tk.ObjectNotFound), \
         pytest.raises(utils.JobError, match="PII regex resource 'no-such-resource' not found"):
        pii_screening.screen_for_pii(
            str(tmp_path / "data.csv"), {"id": "res"}, mock.Mock(), str(tmp_path), mock.Mock()
        )


@requires_qsv
@pytest.mark.parametrize("quick", [True, False], ids=["quick", "full"])
def test_a_custom_regex_resource_need_not_be_in_the_datastore(screen, quick, tmp_path):
    """A plain uploaded regex file works: it is fetched with resource_show and
    downloaded into the job's own temp dir, not into the shared package dir."""
    from ckanext.datapusher_plus import pii_screening, utils

    response = mock.Mock(content=b"^Bob$\n")
    package_copy = Path(pii_screening.__file__).with_name("user-pii-regexes.txt")
    with mock.patch.object(
        pii_screening.dsu, "get_resource",
        return_value={"url": "https://ckan.example/download/regexes.txt"},
    ) as get_resource, \
         mock.patch.object(pii_screening.dsu, "datastore_resource_exists") as in_datastore, \
         mock.patch.object(pii_screening.requests, "get", return_value=response), \
         pytest.raises(utils.JobError, match="PII CANDIDATE"):
        screen(CLEAN, quick, regex_resource="custom-regexes")

    get_resource.assert_called_once_with("custom-regexes")
    in_datastore.assert_not_called()
    assert (tmp_path / "user-pii-regexes.txt").read_bytes() == b"^Bob$\n"
    assert not package_copy.exists()


@requires_qsv
def test_quick_screen_aborts_even_when_candidates_would_be_shown(screen):
    """Quick screening has no candidate rows to preview, so
    pii_show_candidates must not stop pii_found_abort from aborting."""
    from ckanext.datapusher_plus import utils

    with pytest.raises(utils.JobError, match=r"PII CANDIDATE FOUND on row \d+! Job aborted\."):
        screen(WITH_SSN, quick=True, abort=True, show_candidates=True)


@requires_qsv
@pytest.mark.parametrize("show_candidates", [True, False])
def test_quick_screen_without_abort_warns_and_proceeds(screen, show_candidates):
    logger = mock.Mock()
    assert screen(
        WITH_SSN, quick=True, abort=False, show_candidates=show_candidates, logger=logger
    ) == (True, 1)
    assert any(
        "PII CANDIDATE FOUND on row" in str(c.args[0]) for c in logger.warning.call_args_list
    )
