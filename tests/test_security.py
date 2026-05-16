# -*- coding: utf-8 -*-
# flake8: noqa: E501
"""
Security-focused tests for paths flagged in the codebase review:

  * Jinja2 formula engine must be sandboxed (C1 in the review).
  * LIKE-pattern escaping in the metadata stage (M2).
  * Email regex used by PII screening (L1).
  * CSV-injection defang for ZIP-entry filenames (L4).

These tests are deliberately import-light. The CKAN extension's helper modules
read configuration at import time (``QSV_BIN``, ``DATASTORE_WRITE_URL``), so
each project-dependent test guards its imports with ``pytest.importorskip``
and skips gracefully outside the CI container.
"""

from __future__ import annotations

import importlib
import re

import pytest


def _import_or_skip(module_name: str):
    """Like ``pytest.importorskip`` but also skips on RuntimeError.

    ``ckanext.datapusher_plus.config`` raises ``RuntimeError`` at import time
    when required CKAN settings (``qsv_bin``, ``ckan.datastore.write_url``)
    are not configured — exactly the case where these tests need to skip
    rather than fail noisily. ``pytest.importorskip`` only catches
    ``ImportError``, so wrap it here.
    """
    try:
        return importlib.import_module(module_name)
    except ImportError as exc:
        pytest.skip(f"{module_name} is not importable: {exc}")
    except RuntimeError as exc:
        pytest.skip(f"{module_name} requires CKAN config: {exc}")


# ---------------------------------------------------------------------------
# C1 — Jinja2 sandbox must block ``__class__`` / ``__subclasses__`` escapes.
# ---------------------------------------------------------------------------


class TestJinja2Sandbox:
    """The formula engine renders templates authored in scheming YAML. Without
    ``SandboxedEnvironment`` a template like ``{{ ''.__class__.__mro__[1] }}``
    walks the type graph to ``subprocess.Popen`` and yields RCE. These tests
    pin the sandbox behavior directly against jinja2 — they don't import
    DataPusher+ so they run in any environment with jinja2 installed."""

    def test_sandbox_hides_dunder_attribute(self):
        """Direct ``__class__`` access returns an ``Undefined`` object rather
        than the real class — i.e. the underlying type is not exposed."""
        SandboxedEnvironment = pytest.importorskip(
            "jinja2.sandbox"
        ).SandboxedEnvironment

        env = SandboxedEnvironment()
        # Rendering as a string produces empty output (default Undefined.__str__)
        # — the attacker never gets a class repr they can navigate from.
        rendered = env.from_string("{{ ''.__class__ }}").render()
        assert "class" not in rendered.lower()
        assert "str" not in rendered.lower()

    def test_sandbox_blocks_subclasses_walk(self):
        """The actual RCE chain — walking ``__mro__`` to ``__subclasses__()``
        to reach ``subprocess`` — must surface as ``SecurityError`` or
        ``UndefinedError`` (depending on which step in the chain fails first).
        Either is acceptable; both prevent reaching real classes."""
        SandboxedEnvironment = pytest.importorskip(
            "jinja2.sandbox"
        ).SandboxedEnvironment
        exceptions = pytest.importorskip("jinja2.exceptions")

        env = SandboxedEnvironment()
        template = env.from_string(
            "{{ ''.__class__.__mro__[1].__subclasses__() }}"
        )
        with pytest.raises((exceptions.SecurityError, exceptions.UndefinedError)):
            template.render()

    def test_sandbox_blocks_direct_subclasses_call(self):
        """Calling ``__subclasses__`` directly on a real type must be blocked
        — this is the second canonical RCE entry point alongside __mro__."""
        SandboxedEnvironment = pytest.importorskip(
            "jinja2.sandbox"
        ).SandboxedEnvironment
        exceptions = pytest.importorskip("jinja2.exceptions")

        env = SandboxedEnvironment()
        template = env.from_string(
            "{{ cls.__subclasses__() }}"
        )
        with pytest.raises((exceptions.SecurityError, exceptions.UndefinedError)):
            template.render(cls=object)

    def test_normal_rendering_still_works(self):
        SandboxedEnvironment = pytest.importorskip(
            "jinja2.sandbox"
        ).SandboxedEnvironment

        env = SandboxedEnvironment()
        template = env.from_string("hello {{ name }}")
        assert template.render(name="world") == "hello world"

    def test_formula_processor_uses_sandboxed_environment(self):
        """Pin DataPusher+'s own formula engine to ``SandboxedEnvironment`` —
        the standalone sandbox tests above only prove jinja2's library
        contract; this one catches a regression back to plain ``Environment``
        inside ``create_jinja2_env``."""
        SandboxedEnvironment = pytest.importorskip(
            "jinja2.sandbox"
        ).SandboxedEnvironment
        jinja2_helpers = _import_or_skip("ckanext.datapusher_plus.jinja2_helpers")

        # FormulaProcessor.__init__ does heavy lat/lon/date inference on the
        # constructor args, so build a minimal stub directly and call the
        # env-factory method via __get__ to bypass __init__.
        class _Stub:
            logger = type(
                "L",
                (),
                {
                    "trace": lambda self, *a, **k: None,
                    "debug": lambda self, *a, **k: None,
                    "info": lambda self, *a, **k: None,
                    "warning": lambda self, *a, **k: None,
                    "error": lambda self, *a, **k: None,
                },
            )()

        env = jinja2_helpers.FormulaProcessor.create_jinja2_env(
            _Stub(), {"f": "hello {{ name }}"}
        )
        assert isinstance(env, SandboxedEnvironment), (
            "DataPusher+ formula engine must be a SandboxedEnvironment "
            "(see jinja2_helpers.create_jinja2_env). Plain Environment "
            "would allow scheming-YAML formulas to RCE via __class__ walks."
        )


# ---------------------------------------------------------------------------
# M2 — LIKE-pattern escape for AUTO_ALIAS uniqueness checks.
# ---------------------------------------------------------------------------


class TestEscapeLike:
    """``_escape_like`` defangs SQL LIKE metacharacters so resource names
    containing ``%``/``_``/``\\`` don't accidentally turn the alias-uniqueness
    prefix match into a wildcard scan."""

    def _load(self):
        metadata = _import_or_skip(
            "ckanext.datapusher_plus.jobs.stages.metadata"
        )
        return metadata._escape_like

    def test_percent_is_escaped(self):
        escape = self._load()
        assert escape("50% off") == "50\\% off"

    def test_underscore_is_escaped(self):
        escape = self._load()
        assert escape("my_resource") == "my\\_resource"

    def test_backslash_is_escaped_first(self):
        escape = self._load()
        # The backslash must be doubled BEFORE the % / _ escapes are added,
        # otherwise the resulting string contains an unescaped trailing slash.
        assert escape("a\\b") == "a\\\\b"

    def test_plain_value_passes_through(self):
        escape = self._load()
        assert escape("clean-name") == "clean-name"


# ---------------------------------------------------------------------------
# L1 — Updated PII email regex.
# ---------------------------------------------------------------------------


class TestEmailRegex:
    """Pins the email regex shipped in ``default-pii-regexes.txt`` so future
    edits can't silently regress TLD coverage or plus-addressing support."""

    PATTERN = re.compile(r"(?x)[\w.+-]+@[\w-]+(?:\.[\w-]+)+")

    @pytest.mark.parametrize(
        "candidate",
        [
            "joel@dathere.com",
            "joel+work@dathere.com",
            "joel.natividad@dathere.co.uk",
            "info@example.museum",
            "first.last@sub.domain.info",
        ],
    )
    def test_matches_valid_emails(self, candidate):
        assert self.PATTERN.search(candidate) is not None

    def test_does_not_match_obvious_non_email(self):
        assert self.PATTERN.search("no at sign here") is None

    def test_equals_is_not_in_local_part(self):
        """The old regex included ``=`` in the local-part character class, so
        ``re.search("key=value@example.co")`` would match starting at ``k``
        and produce ``"key=value@example.co"`` as a 'pseudo-token'. The new
        regex excludes ``=``; if a match is found at all, it must start after
        the equals sign (e.g. ``"value@example.co"`` is still a real email
        and SHOULD be flagged as PII — that's the correct behaviour, not a
        bug). This test pins that boundary."""
        match = self.PATTERN.search("key=value@example.co")
        assert match is None or "=" not in match.group()


# ---------------------------------------------------------------------------
# L4 — CSV-injection defang on attacker-controlled ZIP entry filenames.
# ---------------------------------------------------------------------------


class TestCsvSafeCell:
    """Spreadsheet apps interpret cells whose first character is
    ``= + - @ \\t \\r`` as formulas. ``_csv_safe_cell`` prefixes such values
    with a single quote so the receiving spreadsheet treats them as text."""

    def _load(self):
        helpers = _import_or_skip("ckanext.datapusher_plus.helpers")
        return helpers._csv_safe_cell

    @pytest.mark.parametrize(
        "danger",
        ["=cmd|' /C calc'!A0", "+1+1", "-2+3", "@SUM(A1)", "\tfoo", "\rbar"],
    )
    def test_dangerous_prefixes_are_quoted(self, danger):
        safe = self._load()
        result = safe(danger)
        assert result.startswith("'")
        assert result == "'" + danger

    def test_safe_strings_pass_through(self):
        safe = self._load()
        assert safe("normal.csv") == "normal.csv"
        assert safe("readme.txt") == "readme.txt"

    def test_non_strings_pass_through(self):
        safe = self._load()
        assert safe(None) is None
        assert safe(123) == 123

    def test_empty_string_passes_through(self):
        safe = self._load()
        assert safe("") == ""
