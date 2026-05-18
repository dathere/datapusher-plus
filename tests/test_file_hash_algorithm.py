# -*- coding: utf-8 -*-
"""
Unit coverage for ``ckanext.datapusher_plus.jobs.stages.download._get_file_hasher``.

This is the seam introduced by #221 that picks the per-job hasher
from ``conf.FILE_HASH_ALGORITHM``. Three things matter and are pinned
here:

1. The default algorithm IS blake3 — operators upgrading to v3 with no
   config change must get the faster algorithm by default, not silently
   stay on md5 because someone deleted the default value.
2. Each supported algorithm produces the canonical hex digest for a
   known input — proves the factory wires up the right primitive
   (not, say, sha256 for both "sha256" and "blake3" via a typo'd map).
3. Unknown algorithm raises a clear error rather than silently falling
   back, so a typo in ``ckan.ini`` surfaces at the first download
   instead of producing inscrutable hash mismatches downstream.

Round-trip is tested via ``hasher.update(bytes)`` + ``hexdigest()``
matching reference digests computed via the stdlib (or, for blake3,
via the ``blake3`` package directly) on a fixed payload — there's no
external file or network dependency.
"""

from __future__ import annotations

import hashlib

import pytest


# Fixed payload + reference digests. ``HELLO_DPP`` is the bytes literal;
# the three constants below are the canonical lower-case hex digests
# of that payload under each algorithm. Computed once and inlined so
# the test doesn't depend on the same hashing libraries it's pinning.
HELLO_DPP = b"hello, datapusher-plus"
SHA256_HEX = hashlib.sha256(HELLO_DPP).hexdigest()
MD5_HEX = hashlib.md5(HELLO_DPP).hexdigest()  # noqa: S324 — test-only reference

# blake3 reference is computed at import time via the same package
# the SUT uses — fine because the test is about *factory wiring*, not
# blake3's own correctness (which the upstream package owns).
try:
    from blake3 import blake3 as _blake3

    BLAKE3_HEX = _blake3(HELLO_DPP).hexdigest()
except ImportError:
    BLAKE3_HEX = None  # blake3 not installed → skip blake3 tests


@pytest.fixture
def factory():
    pytest.importorskip("ckan")
    from ckanext.datapusher_plus.jobs.stages.download import _get_file_hasher

    return _get_file_hasher


# ---------------------------------------------------------------------------
# Default behaviour: blake3 (the v3 default)
# ---------------------------------------------------------------------------


def test_default_algorithm_is_blake3(factory, monkeypatch):
    # The most important contract from #221: operators get blake3 by
    # default, not md5 by silent legacy fallback. A regression where
    # someone changes ``FILE_HASH_ALGORITHM = "blake3"`` to ``"md5"``
    # would slip through every other test in this file.
    if BLAKE3_HEX is None:
        pytest.skip("blake3 package not installed")

    pytest.importorskip("ckan")
    import ckan.plugins.toolkit as tk

    # Unset the knob so the default kicks in.
    monkeypatch.delitem(
        tk.config,
        "ckanext.datapusher_plus.file_hash_algorithm",
        raising=False,
    )
    # Reimport conf so FILE_HASH_ALGORITHM picks up the cleared config.
    import importlib

    from ckanext.datapusher_plus import config as conf_mod

    importlib.reload(conf_mod)
    assert conf_mod.FILE_HASH_ALGORITHM == "blake3"

    # And the factory wires it to a real blake3 hasher.
    import ckanext.datapusher_plus.jobs.stages.download as dl_mod

    importlib.reload(dl_mod)
    h = dl_mod._get_file_hasher()
    h.update(HELLO_DPP)
    assert h.hexdigest() == BLAKE3_HEX


# ---------------------------------------------------------------------------
# Per-algorithm wiring
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("algo", "expected_hex"),
    [
        pytest.param(
            "blake3",
            BLAKE3_HEX,
            marks=pytest.mark.skipif(
                BLAKE3_HEX is None, reason="blake3 package not installed"
            ),
        ),
        ("sha256", SHA256_HEX),
        ("md5", MD5_HEX),
    ],
)
def test_factory_returns_hasher_matching_algorithm(
    algo, expected_hex, monkeypatch
):
    # For each supported algorithm: setting the config to that algorithm
    # makes the factory return a hasher that produces the canonical
    # digest. Catches a regression where (say) the "blake3" branch
    # accidentally returns ``hashlib.sha256()``.
    pytest.importorskip("ckan")
    import ckan.plugins.toolkit as tk

    monkeypatch.setitem(
        tk.config, "ckanext.datapusher_plus.file_hash_algorithm", algo
    )
    # Reimport conf so the module-level constant re-reads the config.
    import importlib

    from ckanext.datapusher_plus import config as conf_mod

    importlib.reload(conf_mod)

    import ckanext.datapusher_plus.jobs.stages.download as dl_mod

    importlib.reload(dl_mod)
    h = dl_mod._get_file_hasher()
    h.update(HELLO_DPP)
    assert h.hexdigest() == expected_hex


# ---------------------------------------------------------------------------
# Case insensitivity (operators write `SHA256` not `sha256`)
# ---------------------------------------------------------------------------


def test_algorithm_name_is_case_insensitive(monkeypatch):
    # Operators typing ``SHA256`` (or ``Blake3``) in ckan.ini should
    # work — config.py applies ``.lower()`` at module-load time. Catches
    # a regression where someone "cleans up" the .lower() call.
    pytest.importorskip("ckan")
    import ckan.plugins.toolkit as tk

    monkeypatch.setitem(
        tk.config, "ckanext.datapusher_plus.file_hash_algorithm", "SHA256"
    )
    import importlib

    from ckanext.datapusher_plus import config as conf_mod

    importlib.reload(conf_mod)
    assert conf_mod.FILE_HASH_ALGORITHM == "sha256"

    import ckanext.datapusher_plus.jobs.stages.download as dl_mod

    importlib.reload(dl_mod)
    h = dl_mod._get_file_hasher()
    h.update(HELLO_DPP)
    assert h.hexdigest() == SHA256_HEX


# ---------------------------------------------------------------------------
# Unknown algorithm: explicit failure, not silent fallback
# ---------------------------------------------------------------------------


def test_unknown_algorithm_raises_job_error(monkeypatch):
    # A typo in ckan.ini (``shA256``? ``blakethree``?) MUST surface at
    # the first download with a clear error message, not silently fall
    # back to md5 (or worse, an empty hasher that produces a constant
    # digest for every file).
    pytest.importorskip("ckan")
    import ckan.plugins.toolkit as tk

    monkeypatch.setitem(
        tk.config,
        "ckanext.datapusher_plus.file_hash_algorithm",
        "not-a-real-algo",
    )
    import importlib

    from ckanext.datapusher_plus import config as conf_mod

    importlib.reload(conf_mod)

    import ckanext.datapusher_plus.jobs.stages.download as dl_mod
    from ckanext.datapusher_plus import utils as utils_mod

    importlib.reload(dl_mod)

    with pytest.raises(utils_mod.JobError) as exc_info:
        dl_mod._get_file_hasher()

    # Error message identifies the offending value AND lists what IS
    # valid — so the operator can fix the config without grepping.
    assert "not-a-real-algo" in str(exc_info.value)
    assert "blake3" in str(exc_info.value)
    assert "sha256" in str(exc_info.value)
    assert "md5" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Streaming contract: chunked update produces the same digest
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("algo", "expected_hex"),
    [
        pytest.param(
            "blake3",
            BLAKE3_HEX,
            marks=pytest.mark.skipif(
                BLAKE3_HEX is None, reason="blake3 package not installed"
            ),
        ),
        ("sha256", SHA256_HEX),
        ("md5", MD5_HEX),
    ],
)
def test_chunked_update_matches_single_update(
    algo, expected_hex, monkeypatch
):
    # The download loop calls ``m.update(chunk)`` once per CHUNK_SIZE
    # bytes — the resulting digest MUST match a single ``update(full)``
    # call. This is the hashlib protocol contract; pinning it here
    # catches a regression where someone "optimizes" the factory by
    # returning a hasher with a different streaming protocol.
    pytest.importorskip("ckan")
    import ckan.plugins.toolkit as tk

    monkeypatch.setitem(
        tk.config, "ckanext.datapusher_plus.file_hash_algorithm", algo
    )
    import importlib

    from ckanext.datapusher_plus import config as conf_mod

    importlib.reload(conf_mod)

    import ckanext.datapusher_plus.jobs.stages.download as dl_mod

    importlib.reload(dl_mod)
    h = dl_mod._get_file_hasher()
    # Tiny chunks — exercises the chunked-update path the download loop
    # actually uses (different from a single big update call).
    for i in range(0, len(HELLO_DPP), 3):
        h.update(HELLO_DPP[i : i + 3])
    assert h.hexdigest() == expected_hex
