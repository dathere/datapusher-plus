# -*- coding: utf-8 -*-
"""
Regression coverage for the ``pii_screening`` config-key typo fix.

A documentation audit (2026-05-19) found that ``config.py`` read
``PII_SCREENING`` from ``ckanext.datastore_plus.pii_screening`` — a
typo: ``datastore_plus`` instead of ``datapusher_plus``. Every other
DP+ setting, the ``config_declaration.yaml`` declaration, and the
README all use the ``datapusher_plus`` namespace. The practical
effect of the typo: operators enabling PII screening via the
documented key ``ckanext.datapusher_plus.pii_screening`` had no
effect — ``PII_SCREENING`` silently stayed at its ``False`` fallback,
so the PII-screening feature could not be turned on as documented.

These tests pin three properties:

1. The documented key ``ckanext.datapusher_plus.pii_screening``
   actually drives ``PII_SCREENING`` — the functional fix.
2. The old typo'd key ``ckanext.datastore_plus.pii_screening`` does
   NOT drive it — proves the read moved off the wrong namespace and
   would catch a regression that reintroduced the typo.
3. The key string read in ``config.py`` matches the one declared in
   ``config_declaration.yaml`` — a #179-style declaration-vs-code
   drift guard (AST-parsed to avoid the CKAN bootstrap).
"""

from __future__ import annotations

from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parent.parent


def _reload_config(monkeypatch, key, value):
    """Set ``key`` on ``tk.config`` and reload ``config.py`` so its
    import-time ``PII_SCREENING`` constant is recomputed. Mirrors the
    reload pattern used by ``test_file_hash_algorithm`` / the #61
    config tests."""
    import importlib

    pytest.importorskip("ckan")
    import ckan.plugins.toolkit as tk

    monkeypatch.setitem(tk.config, key, value)
    import ckanext.datapusher_plus.config as conf

    return importlib.reload(conf)


def test_documented_key_enables_pii_screening(monkeypatch):
    # The core fix: the key the README + config_declaration.yaml
    # document must actually flip PII_SCREENING on.
    conf = _reload_config(
        monkeypatch, "ckanext.datapusher_plus.pii_screening", True
    )
    assert conf.PII_SCREENING is True


def test_old_typo_key_has_no_effect(monkeypatch):
    # The pre-fix typo'd key must NOT drive PII_SCREENING anymore.
    # Set ONLY the typo'd key to True, with the correct key absent,
    # and PII_SCREENING must stay at its False default. A regression
    # that reinstated ``datastore_plus`` would flip this to True.
    pytest.importorskip("ckan")
    import ckan.plugins.toolkit as tk

    # Ensure the correct key is absent so the assertion is unambiguous
    # (config_declaration.yaml declares it with default false, so the
    # test env may have it pre-populated).
    monkeypatch.delitem(
        tk.config, "ckanext.datapusher_plus.pii_screening", raising=False
    )
    conf = _reload_config(
        monkeypatch, "ckanext.datastore_plus.pii_screening", True
    )
    assert conf.PII_SCREENING is False


def test_config_py_key_matches_declaration():
    """The key string read in ``config.py`` must match the one
    declared in ``config_declaration.yaml``. A namespace typo in
    either source is exactly the bug this test exists to catch."""
    import ast

    # PyYAML guarded the same way the #112 / #142 / #61 drift-guards
    # are — degrade to a skip in dep-light CI rather than ImportError.
    try:
        import yaml
    except ImportError:
        pytest.skip("PyYAML not available")

    # config_declaration.yaml side — iterate all groups/options so the
    # guard survives a future declaration restructure.
    yaml_path = (
        REPO_ROOT
        / "ckanext"
        / "datapusher_plus"
        / "config_declaration.yaml"
    )
    decl = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
    declared_keys = {
        opt.get("key")
        for group in decl.get("groups", [])
        for opt in group.get("options", [])
    }
    assert "ckanext.datapusher_plus.pii_screening" in declared_keys, (
        "config_declaration.yaml must declare "
        "ckanext.datapusher_plus.pii_screening."
    )

    # config.py side — AST-find the PII_SCREENING assignment and the
    # config key string it reads.
    config_py = (
        REPO_ROOT / "ckanext" / "datapusher_plus" / "config.py"
    ).read_text()
    tree = ast.parse(config_py)
    found_key = None
    for node in ast.walk(tree):
        if not (
            isinstance(node, ast.Assign)
            and any(
                getattr(t, "id", None) == "PII_SCREENING"
                for t in node.targets
            )
        ):
            continue
        for call in ast.walk(node):
            if (
                isinstance(call, ast.Call)
                and getattr(call.func, "attr", None) == "get"
                and call.args
                and isinstance(call.args[0], ast.Constant)
            ):
                found_key = call.args[0].value
                break
        break
    assert found_key == "ckanext.datapusher_plus.pii_screening", (
        f"config.py reads PII_SCREENING from {found_key!r}; must be "
        "'ckanext.datapusher_plus.pii_screening' to match the "
        "config_declaration.yaml declaration and the README "
        "(the 'datastore_plus' typo was the documented-but-dead "
        "key bug)."
    )
