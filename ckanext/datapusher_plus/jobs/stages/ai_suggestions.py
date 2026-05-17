# -*- coding: utf-8 -*-
"""
AI suggestions stage for the DataPusher Plus pipeline.

Optional stage that calls ``qsv describegpt`` against the post-analysis
CSV and persists the LLM-generated description / per-field dictionary /
tags under ``package["dpp_suggestions"]["ai_suggestions"]`` for the
scheming form layer to surface (via ``scheming_get_ai_suggestion``).

Gated by ``ckanext.datapusher_plus.enable_ai_suggestions`` (off by
default). All failure modes are non-blocking by design — an
unreachable LLM endpoint, a qsv timeout, or malformed JSON output
should never gate datastore ingestion. The stage catches every
exception internally, logs at warning level, and always returns the
context unchanged.
"""

from __future__ import annotations

import json
import time
from typing import Any, Dict, Optional

import ckanext.datapusher_plus.config as conf
import ckanext.datapusher_plus.datastore_utils as dsu
from ckanext.datapusher_plus.jobs.context import ProcessingContext
from ckanext.datapusher_plus.jobs.stages.base import BaseStage
from ckanext.datapusher_plus.qsv_utils import QSVCommand


class AISuggestionsStage(BaseStage):
    """
    Generate AI-assisted metadata suggestions via ``qsv describegpt``.

    Placement in the flow: between ``AnalysisStage`` (which produces
    the cached CSV) and the database-mutating group. Sitting OUTSIDE
    the ``transaction()`` block in ``datapusher_plus_flow`` keeps a
    failure here from poisoning the per-task rollback hooks.

    Output shape (when the stage succeeds and qsv returns valid JSON)::

        package["dpp_suggestions"]["ai_suggestions"] = {
            "description": "...",            # if qsv emits it
            "tags": [...],                   # if qsv emits them
            "dictionary": [...],             # per-field metadata
            "generated_at": "<ISO 8601>",    # when we ran the stage
        }

    The exact keys inside ``ai_suggestions`` are whatever ``qsv
    describegpt --description --dictionary --tags --json`` emits, plus
    a ``generated_at`` timestamp. We deliberately do not validate or
    reshape qsv's output — the scheming helper consumes it as-is.

    The stage sits alongside (NOT inside) ``dpp_suggestions["package"]``
    (formula-derived per-field suggestions) — keeping the AI namespace
    separate so a formula-driven suggestion and an AI-derived one can
    co-exist without collision.
    """

    def __init__(self) -> None:
        super().__init__(name="AISuggestionsGeneration")

    def should_skip(self, context: ProcessingContext) -> bool:
        """Skip when the operator hasn't opted in."""
        if not conf.ENABLE_AI_SUGGESTIONS:
            context.logger.info(
                "AI suggestions disabled "
                "(ckanext.datapusher_plus.enable_ai_suggestions=False); "
                "skipping AISuggestionsStage"
            )
            return True
        return False

    def process(self, context: ProcessingContext) -> ProcessingContext:
        """
        Generate and persist AI suggestions.

        Never raises. Every failure mode falls through to a warning log
        and returns the context unchanged so the rest of the pipeline
        continues normally.

        Args:
            context: Processing context with ``tmp`` (post-analysis CSV
                path) and ``resource`` (with ``package_id``).

        Returns:
            The context, unchanged. Side effects land on the CKAN
            package via ``dsu.patch_package``.
        """
        stage_start = time.perf_counter()

        try:
            suggestions = self._run_describegpt(context)
        except Exception as exc:
            context.logger.warning(
                f"qsv describegpt failed; skipping AI suggestions: {exc}"
            )
            return context

        if suggestions is None:
            # Already logged inside _run_describegpt for the
            # parseable-but-empty case. Nothing more to do.
            return context

        try:
            self._persist_suggestions(context, suggestions)
        except Exception as exc:
            context.logger.warning(
                f"Failed to persist AI suggestions to package; "
                f"continuing pipeline: {exc}"
            )
            return context

        elapsed = time.perf_counter() - stage_start
        context.logger.info(
            f"AI suggestions persisted in {elapsed:,.2f} seconds."
        )
        return context

    def _run_describegpt(
        self, context: ProcessingContext
    ) -> Optional[Dict[str, Any]]:
        """
        Shell out to ``qsv describegpt`` and parse the JSON result.

        Returns ``None`` (with a warning) when the subprocess succeeds
        but stdout isn't valid JSON or is empty — that's a "soft" miss
        the caller treats as no-suggestions, not as an error.
        Re-raises anything else (subprocess failure, missing binary,
        timeout) so the caller's outer try/except can log + skip.
        """
        prompt_file = conf.DESCRIBEGPT_CONFIG_PATH or None
        # ``_build_runtime_context`` constructs a ``QSVCommand`` once per
        # flow run and stashes it on the context — reuse that so we
        # don't pay the binary-existence + ``check_version`` cost twice.
        # Fall back to a fresh instance if the context doesn't have one
        # (e.g. unit tests that hand-build a context stand-in).
        qsv = getattr(context, "qsv", None) or QSVCommand(logger=context.logger)
        context.logger.info(
            f"Running qsv describegpt against {context.tmp} "
            f"(prompt_file={prompt_file or '<qsv default>'}, "
            f"timeout={conf.DESCRIBEGPT_TIMEOUT_SECONDS}s)"
        )
        result = qsv.describegpt(
            input_file=str(context.tmp),
            prompt_file=prompt_file,
            description=True,
            dictionary=True,
            tags=True,
            json_output=True,
        )
        stdout = (result.stdout or "").strip()
        if not stdout:
            context.logger.warning(
                "qsv describegpt returned empty stdout; no suggestions to persist"
            )
            return None
        try:
            parsed = json.loads(stdout)
        except json.JSONDecodeError as exc:
            context.logger.warning(
                f"qsv describegpt stdout was not valid JSON "
                f"(first 200 chars: {stdout[:200]!r}): {exc}"
            )
            return None
        if not isinstance(parsed, dict):
            context.logger.warning(
                f"qsv describegpt JSON wasn't an object "
                f"(got {type(parsed).__name__}); no suggestions to persist"
            )
            return None
        return parsed

    def _persist_suggestions(
        self,
        context: ProcessingContext,
        suggestions: Dict[str, Any],
    ) -> None:
        """
        Merge ``suggestions`` into
        ``package["dpp_suggestions"]["ai_suggestions"]`` and call
        ``dsu.patch_package`` to save.

        Raises on package-fetch / patch failure — caller logs and
        continues.
        """
        package_id = context.resource["package_id"]
        # FormulaStage uses ``get_scheming_yaml`` as the package fetch
        # because it also needs the schema; we only need the package
        # body, but reusing the same accessor keeps the call paths
        # consistent (and tolerant of the same scheming-unavailable
        # corner case).
        _scheming_yaml, package = dsu.get_scheming_yaml(
            package_id, scheming_yaml_type="dataset"
        )
        if package is None:
            raise RuntimeError(
                f"package {package_id} not returned by get_scheming_yaml"
            )

        # Stamp the generation time so consumers (and audit logs) can
        # tell stale suggestions from fresh ones. UTC, ISO-8601, no
        # microseconds — readable in log scans.
        payload = dict(suggestions)
        payload["generated_at"] = (
            time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        )

        dpp = package.get("dpp_suggestions")
        if dpp is None:
            package["dpp_suggestions"] = dpp = {}
        elif not isinstance(dpp, dict):
            # Bail rather than overwrite. The stage is non-blocking by
            # contract, so a corrupted ``dpp_suggestions`` (legacy JSON
            # string, accidental scalar from a custom plugin, …) gets
            # skipped here rather than destroyed — preserving whatever
            # the operator put there. FormulaStage's
            # ``_setup_dpp_suggestions`` has the inverse policy
            # (errors out and aborts the flow) because formula
            # processing is the package's primary purpose; AI
            # suggestions are bonus content and not worth losing data
            # over.
            context.logger.warning(
                f"package['dpp_suggestions'] is {type(dpp).__name__}, "
                "not a dict; skipping AI suggestions write to avoid "
                "overwriting existing data"
            )
            return
        dpp["ai_suggestions"] = payload

        dsu.patch_package(package)
        context.logger.info(
            f"Wrote {len(payload)} ai_suggestions keys to package {package_id}"
        )
