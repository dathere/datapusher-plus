# -*- coding: utf-8 -*-
"""``QSVCommand`` runs qsv through qsv-client.

These tests run ``QSVCommand`` against a fake ``qsvdp`` (a Python script) and
check the contract callers rely on: a ``subprocess.CompletedProcess`` on
success, ``JobError`` on failure (or the ``{"stdout", "stderr"}`` dict with
``uses_stdio``), the result itself with ``check=False``, and a timeout that
kills qsv's children too.
"""

import json
import os
import subprocess
import sys
import time
from unittest import mock

import pytest

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="the fake qsv is a POSIX script"
)

FAKE_QSV = r'''
import json, os, subprocess, sys, time

args = sys.argv[1:]
cmd = args[0] if args else ""
if cmd == "--capabilities":
    print(json.dumps({"binary": "qsvdp", "version": os.environ.get("FAKE_QSV_VERSION", "99.0.0"),
                      "features": [], "commands": ["count"], "error_formats": ["text", "json"]}))
elif cmd == "ok":
    print("out")
    print("progress", file=sys.stderr)
elif cmd == "fail-json":
    print("out-before-failure")
    print(json.dumps({"error": {"kind": "csv", "level": "error", "message": "ragged row 3",
                                "exit_code": 1, "command": "fail-json", "qsv_version": "99.0.0"}}),
          file=sys.stderr)
    sys.exit(1)
elif cmd == "fail-text":
    print("plain failure text", file=sys.stderr)
    sys.exit(1)
elif cmd == "fail-silent":
    sys.exit(1)
elif cmd == "warn":
    print("broken pipe", file=sys.stderr)
    sys.exit(255)
elif cmd == "env":
    print(json.dumps({k: os.environ.get(k) for k in ("FOO", "PATH", "QSV_ERROR_FORMAT")}))
elif cmd == "hang":
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    with open(args[1], "w") as f:
        f.write(str(child.pid))
    time.sleep(60)
'''


@pytest.fixture
def fake_qsv(tmp_path):
    path = tmp_path / "qsvdp"
    path.write_text(f"#!{sys.executable}\n{FAKE_QSV}")
    path.chmod(0o755)
    return path


@pytest.fixture
def qsv(fake_qsv):
    pytest.importorskip("ckan")
    from ckanext.datapusher_plus import qsv_utils

    with mock.patch.object(qsv_utils.conf, "QSV_BIN", fake_qsv):
        yield qsv_utils.QSVCommand(logger=mock.Mock())


def _job_error():
    from ckanext.datapusher_plus import utils

    return utils.JobError


def test_constructor_checks_the_version_via_capabilities(qsv):
    assert qsv.version() == "99.0.0"


def test_constructor_rejects_a_too_old_qsv(fake_qsv, monkeypatch):
    pytest.importorskip("ckan")
    from ckanext.datapusher_plus import qsv_utils

    monkeypatch.setenv("FAKE_QSV_VERSION", "1.0.0")
    with mock.patch.object(qsv_utils.conf, "QSV_BIN", fake_qsv), pytest.raises(
        _job_error(), match="At least qsv version"
    ):
        qsv_utils.QSVCommand(logger=mock.Mock())


def test_success_returns_a_completed_process(qsv, fake_qsv):
    result = qsv._run_command(["ok"])
    assert isinstance(result, subprocess.CompletedProcess)
    assert result.returncode == 0
    assert result.stdout == "out\n"
    assert result.stderr == "progress\n"
    assert result.args == [str(fake_qsv), "ok"]


def test_failure_raises_job_error_with_the_structured_error(qsv, fake_qsv):
    with pytest.raises(_job_error()) as exc:
        qsv._run_command(["fail-json", "/data/in.csv", "--flag"])
    msg = str(exc.value)
    assert msg.startswith("qsv command failed: ")
    assert "csv" in msg and "ragged row 3" in msg and "exit 1" in msg
    # operators reading the job log need to see what ran
    assert f"[command: {fake_qsv} fail-json /data/in.csv --flag]" in msg


def test_failure_raises_job_error_with_text_stderr(qsv):
    with pytest.raises(_job_error(), match="plain failure text"):
        qsv._run_command(["fail-text"])


def test_failure_with_empty_stderr_still_raises(qsv):
    # an earlier DP+ bug: empty stderr meant check=True failures slipped through
    with pytest.raises(_job_error(), match="exited with code 1"):
        qsv._run_command(["fail-silent"])


def test_exit_255_is_a_failure(qsv):
    # qsv-client counts 255 (qsv's "warning") as success; QSVCommand keeps
    # subprocess.run(check=True)'s rule that any non-zero exit is a failure
    with pytest.raises(_job_error(), match="broken pipe"):
        qsv._run_command(["warn"])


def test_failure_with_uses_stdio_returns_the_raw_output(qsv):
    out = qsv._run_command(["fail-json"], uses_stdio=True)
    assert out["stdout"] == "out-before-failure\n"
    assert '"kind": "csv"' in out["stderr"]


def test_failure_with_check_false_returns_the_completed_process(qsv):
    result = qsv._run_command(["fail-text"], check=False)
    assert isinstance(result, subprocess.CompletedProcess)
    assert result.returncode == 1
    assert "plain failure text" in result.stderr


def test_env_is_merged_into_the_workers_environment(qsv):
    # subprocess.run(env=...) used to REPLACE the environment, dropping PATH
    seen = json.loads(qsv._run_command(["env"], env={"FOO": "bar"}).stdout)
    assert seen["FOO"] == "bar"
    assert seen["PATH"] == os.environ["PATH"]
    assert seen["QSV_ERROR_FORMAT"] == "json"


def test_text_false_returns_bytes(qsv):
    result = qsv._run_command(["ok"], text=False)
    assert result.stdout == b"out\n"
    assert result.stderr == b"progress\n"


def test_timeout_raises_job_error_and_kills_qsvs_children(qsv, tmp_path):
    pid_file = tmp_path / "child.pid"
    started = time.monotonic()
    with pytest.raises(_job_error(), match=r"timed out after 1s"):
        qsv._run_command(["hang", str(pid_file)], timeout=1)
    assert time.monotonic() - started < 30

    child = int(pid_file.read_text())
    deadline = time.monotonic() + 10
    while _alive(child):
        if time.monotonic() > deadline:
            os.kill(child, 9)
            pytest.fail("qsv's child process survived the timeout")
        time.sleep(0.05)


def _alive(pid):
    """True while ``pid`` runs. A zombie counts as dead: an orphan is only
    reaped if PID 1 reaps, and a container's PID 1 may not."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    stat = f"/proc/{pid}/stat"
    if os.path.exists(stat):
        with open(stat) as f:
            return f.read().rsplit(")", 1)[-1].split()[0] != "Z"
    state = subprocess.run(
        ["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True
    ).stdout.strip()
    return bool(state) and not state.startswith("Z")
