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
import logging
import time
from typing import Any, Dict, Optional

import ckanext.datapusher_plus.config as conf
import ckanext.datapusher_plus.datastore_utils as dsu
from ckanext.datapusher_plus.jobs.context import ProcessingContext
from ckanext.datapusher_plus.jobs.stages.base import BaseStage
from ckanext.datapusher_plus.qsv_utils import QSVCommand


_LOG = logging.getLogger(__name__)


# Keys that the dataset-level / bookkeeping entries occupy in the
# on-disk ``ai_suggestions`` map. Per-column entries from
# ``Dictionary.response.fields`` are skipped when their ``name``
# collides with one of these — otherwise a CSV with a column literally
# named ``description`` / ``tags`` would silently overwrite the
# dataset-level Description / Tags envelopes, and a column named
# ``STATUS`` would break the polling-termination signal the JS reads.
# Skipping (rather than nesting per-column entries under a sub-key)
# preserves the flat-map contract the polling JS depends on
# (``Object.keys(aiSuggestions).forEach`` → ``[data-field-name=X]``).
#
# NOTE: This set is intentionally case-sensitive. The polling JS uses
# case-sensitive attribute selectors (``[data-field-name="X"]``) and
# qsv preserves CSV header casing verbatim in
# ``Dictionary.response.fields[i].name``. So a CSV with header
# ``Description`` produces a dictionary entry ``{"name": "Description",
# ...}`` that creates ``ai_suggestions["Description"]`` — which the
# dataset description field (whose ``data-field-name`` is the
# lowercase ``description``) would NOT pick up. The case-sensitive
# guard is therefore exactly the right granularity: it blocks the
# real collision class (exact-name match), and case-mismatched names
# are already isolated from each other by the JS's selector. Do NOT
# lowercase both sides "for safety" — that would re-introduce the
# very collision this guard prevents.
_RESERVED_AI_KEYS = frozenset({"description", "tags", "STATUS", "generated_at"})


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
        Reshape qsv's verbatim output into the per-field
        ``{value, source}`` schema the UI expects, then merge into
        ``package["dpp_suggestions"]["ai_suggestions"]`` and call
        ``dsu.patch_package`` to save.

        Output shape on the package (UI contract — the JS in
        ``assets/js/scheming-ai-suggestions.js`` reads this directly
        via ``package_show``)::

            package["dpp_suggestions"]["ai_suggestions"] = {
                "description": {"value": "...", "source": "..."},
                "tags":        {"value": "...", "source": "..."},
                "<col>":       {"value": "...", "source": "..."},  # per-column
                "STATUS":      "DONE",
                "generated_at": "<ISO 8601 UTC>",
            }

        ``STATUS=DONE`` is the polling-termination signal — the JS
        loops every 2.5s on ``package_show`` until it sees a STATUS in
        ``[DONE, ERROR, FAILED]``. Without it the loop runs out the
        clock at ``maxPollAttempts``, which works but burns API calls.

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

        payload = self._reshape_for_ui(suggestions)
        # Stamp the generation time so consumers (and audit logs) can
        # tell stale suggestions from fresh ones. UTC, ISO-8601, no
        # microseconds — readable in log scans.
        payload["generated_at"] = (
            time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        )
        payload["STATUS"] = "DONE"

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
        # ``- 2`` excludes the bookkeeping keys (STATUS / generated_at)
        # so the log reflects the count of actual per-field suggestions.
        suggestion_count = max(0, len(payload) - 2)
        context.logger.info(
            f"Wrote {suggestion_count} ai_suggestions to package {package_id} "
            f"(STATUS=DONE)"
        )

    def _reshape_for_ui(self, suggestions: Dict[str, Any]) -> Dict[str, Any]:
        """Transform qsv describegpt's verbatim JSON envelope into the
        per-field ``{value, source}`` schema the UI reads.

        qsv 20.0.0 emits a wrapped envelope (validated against a real
        LM Studio round-trip; see ``tests/fixtures/qsv_describegpt_sample.json``)::

            {
              "Dictionary": {
                "response": {
                  "fields": [
                    {"name": "...", "type": "...", "label": "...",
                     "description": "...", "min": "...", "max": "...",
                     "cardinality": N, ...},
                    ...
                  ],
                  "enum_threshold": N, "num_examples": N,
                  "truncate_str": N, "attribution": "..."
                },
                "reasoning": "...", "token_usage": {...}
              },
              "Description": {
                "response": "<markdown string>",
                "reasoning": "...", "token_usage": {...}
              },
              "Tags": {
                "response": {"tags": [...], "attribution": "..."},
                "reasoning": "...", "token_usage": {...},
                "num_tags": N, "tag_vocab": ...
              }
            }

        The UI expects a per-field map::

            {"description": {"value": "<markdown>", "source": "qsv describegpt"},
             "tags":        {"value": "<comma-joined>", "source": "qsv describegpt"},
             "<col>":       {"value": "<dict entry description>", "source": "qsv describegpt"}}

        Reshape rules:

        * ``Description.response`` → top-level ``description`` (string
          as-is; qsv emits Markdown inside the JSON envelope, which the
          scheming markdown form snippet handles natively).
        * ``Tags.response.tags`` → top-level ``tags`` (comma-joined).
          scheming's tag field accepts a comma-joined string natively.
          Tags containing literal commas are filtered out with a
          warning — they'd be split into multiple tags by scheming's
          parser, silently corrupting the dataset.
        * ``Dictionary.response.fields[i]`` → per-column entry keyed
          by ``name``. The value is the LLM-generated ``description``;
          ``label`` is folded into ``source`` so reviewers see both
          when they hover the popover. Per-column entries whose
          ``name`` collides with a reserved key (``description``,
          ``tags``, ``STATUS``, ``generated_at``) are skipped with a
          warning — otherwise they'd silently overwrite the
          dataset-level Description / Tags entries or the
          polling-termination ``STATUS`` signal.

        Done in the stage (rather than letting the helpers cope with
        both shapes) so the on-disk schema is uniform — the existing
        ``scheming_get_ai_suggestion_value`` helper just reaches into
        ``ai_suggestions[field_name]["value"]`` and the JS polls for
        ``ai_suggestions[field_name].value`` directly.

        Defensive at every level: a malformed / partial envelope
        produces an empty dict rather than raising, so the caller's
        try/except is rarely the line of defense.
        """
        base_source = "qsv describegpt"
        out: Dict[str, Any] = {}

        # ---- Description ----
        desc_envelope = suggestions.get("Description")
        if isinstance(desc_envelope, dict):
            desc_value = desc_envelope.get("response")
            if isinstance(desc_value, str) and desc_value.strip():
                out["description"] = {"value": desc_value, "source": base_source}

        # ---- Tags ----
        tags_envelope = suggestions.get("Tags")
        if isinstance(tags_envelope, dict):
            tags_response = tags_envelope.get("response")
            tags_list = None
            if isinstance(tags_response, dict):
                tags_list = tags_response.get("tags")
            elif isinstance(tags_response, list):
                # Some prompt-file variants might emit the list directly.
                tags_list = tags_response
            if isinstance(tags_list, list) and tags_list:
                # Filter out tags containing commas — scheming's tag
                # field uses comma as the separator, so a tag like
                # ``"retail, b2b"`` would be silently split into two
                # tags by downstream parsers.
                safe_tags: list = []
                for raw in tags_list:
                    s = str(raw)
                    if "," in s:
                        # Best-effort: warn via the module logger so
                        # operators can find it in worker logs without
                        # us needing a context handle here.
                        _LOG.warning(
                            "Skipping qsv describegpt tag %r — contains a comma "
                            "which scheming's tag field would split into multiple tags",
                            s,
                        )
                        continue
                    safe_tags.append(s)
                if safe_tags:
                    out["tags"] = {
                        "value": ", ".join(safe_tags),
                        "source": base_source,
                    }

        # ---- Dictionary (per-column entries) ----
        dict_envelope = suggestions.get("Dictionary")
        if isinstance(dict_envelope, dict):
            dict_response = dict_envelope.get("response")
            if isinstance(dict_response, dict):
                fields = dict_response.get("fields")
                if isinstance(fields, list):
                    for entry in fields:
                        if not isinstance(entry, dict):
                            continue
                        name = entry.get("name")
                        if not name:
                            continue
                        if name in _RESERVED_AI_KEYS:
                            # Don't overwrite dataset-level / bookkeeping
                            # entries with a per-column entry sharing
                            # the same name.
                            _LOG.warning(
                                "Skipping per-column AI suggestion for %r — "
                                "name collides with reserved dataset-level / "
                                "bookkeeping key",
                                name,
                            )
                            continue
                        # ``description`` is the canonical LLM-generated
                        # narrative; ``label`` is a shorter human-friendly
                        # heading. Prefer description; fall back to
                        # label with a marker in source so operators
                        # know the popover is showing a label rather
                        # than a description.
                        description = entry.get("description")
                        label = entry.get("label")
                        if description:
                            value = description
                            if label and label != value:
                                source = f"{base_source} · {label}"
                            else:
                                source = base_source
                        elif label:
                            value = label
                            source = f"{base_source} (label only)"
                        else:
                            continue
                        out[name] = {"value": value, "source": source}

        return out
