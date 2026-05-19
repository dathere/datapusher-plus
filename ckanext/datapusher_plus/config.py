# -*- coding: utf-8 -*-
# flake8: noqa: E501

import json
import requests
from pathlib import Path
import ckan.plugins.toolkit as tk

# SSL verification settings.
# Default to True (verify TLS) — disabling verification is a MITM footgun on
# downloads. The old non-namespaced "SSL_VERIFY" key is still honoured for
# backward compatibility with existing deployments.
SSL_VERIFY = tk.asbool(
    tk.config.get(
        "ckanext.datapusher_plus.ssl_verify",
        tk.config.get("SSL_VERIFY", True),
    )
)
if not SSL_VERIFY:
    requests.packages.urllib3.disable_warnings()

# Proxy settings
USE_PROXY = "ckanext.datapusher_plus.download_proxy" in tk.config
if USE_PROXY:
    DOWNLOAD_PROXY = tk.config.get("ckanext.datapusher_plus.download_proxy")

# PostgreSQL integer limits
POSTGRES_INT_MAX = 2147483647
POSTGRES_INT_MIN = -2147483648
POSTGRES_BIGINT_MAX = 9223372036854775807
POSTGRES_BIGINT_MIN = -9223372036854775808

# QSV version requirements
MINIMUM_QSV_VERSION = "20.1.0"

# Logging level
# TRACE, DEBUG, INFO, WARNING, ERROR, CRITICAL
UPLOAD_LOG_LEVEL = tk.config.get("ckanext.datapusher_plus.upload_log_level", "INFO")

# Supported formats
FORMATS = tk.config.get(
    "ckanext.datapusher_plus.formats",
    ["csv", "tsv", "tab", "ssv", "xls", "xlsx", "ods", "geojson", "shp", "qgis", "zip"],
)
if isinstance(FORMATS, str):
    FORMATS = FORMATS.split()

# PII screening settings
PII_SCREENING = tk.asbool(tk.config.get("ckanext.datastore_plus.pii_screening", False))
PII_FOUND_ABORT = tk.asbool(
    tk.config.get("ckanext.datapusher_plus.pii_found_abort", False)
)
PII_REGEX_RESOURCE_ID = tk.config.get(
    "ckanext.datapusher_plus.pii_regex_resource_id_or_alias"
)
PII_SHOW_CANDIDATES = tk.asbool(
    tk.config.get("ckanext.datapusher_plus.pii_show_candidates", False)
)
PII_QUICK_SCREEN = tk.asbool(
    tk.config.get("ckanext.datapusher_plus.pii_quick_screen", False)
)

# Binary paths.
# Falls back to ``/usr/local/bin/qsvdp`` (the ckan-dev container's install
# location) when the config key is unset, so CI doesn't have to manage this
# in CKAN's ``ckan.ini``. The flow-start path-existence check in
# ``prefect_flow._build_runtime_context`` raises a clear ``JobError`` if the
# binary isn't actually at the resolved path, so a misconfiguration still
# surfaces — just later, when it matters.
QSV_BIN = Path(
    tk.config.get("ckanext.datapusher_plus.qsv_bin") or "/usr/local/bin/qsvdp"
)

# Data processing settings
PREVIEW_ROWS = tk.asint(tk.config.get("ckanext.datapusher_plus.preview_rows", "1000"))
TIMEOUT = tk.asint(tk.config.get("ckanext.datapusher_plus.download_timeout", "300"))
QSV_COMMAND_TIMEOUT = tk.asint(
    tk.config.get("ckanext.datapusher_plus.qsv_command_timeout", "1800")
)
MAX_CONTENT_LENGTH = tk.asint(
    tk.config.get("ckanext.datapusher_plus.max_content_length", "5000000")
)
CHUNK_SIZE = tk.asint(tk.config.get("ckanext.datapusher_plus.chunk_size", "1048576"))
DEFAULT_EXCEL_SHEET = tk.asint(tk.config.get("ckanext.datapusher_plus.default_excel_sheet", 0))
SORT_AND_DUPE_CHECK = tk.asbool(
    tk.config.get("ckanext.datapusher_plus.sort_and_dupe_check", True)
)
DEDUP = tk.asbool(tk.config.get("ckanext.datapusher_plus.dedup", True))
UNSAFE_PREFIX = tk.config.get("ckanext.datapusher_plus.unsafe_prefix", "unsafe_")
RESERVED_COLNAMES = tk.config.get("ckanext.datapusher_plus.reserved_colnames", "_id")
PREFER_DMY = tk.asbool(tk.config.get("ckanext.datapusher_plus.prefer_dmy", False))

# Issue #112: locale-aware number normalization. Resolution order
# (per resource, per ingestion) is:
#
#   1. ``context.resource.get('dpp_locale')``  → babel CLDR parse
#   2. ``DEFAULT_LOCALE``                      → babel CLDR parse
#   3. ``DECIMAL_SEPARATOR`` (single char)     → anchored regex pass
#   4. (none of the above)                     → no-op
#
# The locale paths route through ``babel.numbers.parse_decimal``, so
# values like ``57,957``, ``1.234,56`` (de_DE) and ``57 957,12``
# (fr_FR) all normalize to dot-decimal Floats that qsv stats then
# infers correctly. ``DECIMAL_SEPARATOR`` is the escape hatch for
# operators who know the separator but don't have or want CLDR locale
# info — single character, anchored regex, no thousands handling.
DEFAULT_LOCALE = tk.config.get("ckanext.datapusher_plus.default_locale", "")
DECIMAL_SEPARATOR = tk.config.get(
    "ckanext.datapusher_plus.decimal_separator", ""
)
IGNORE_FILE_HASH = tk.asbool(
    tk.config.get("ckanext.datapusher_plus.ignore_file_hash", False)
)

# Issue #61: ``ckanext.datapusher_plus.download_always_whitelist`` is
# declared in ``config_declaration.yaml`` (``editable: true``) but is
# intentionally NOT mirrored here as a module-level constant. The
# download stage's ``_host_in_always_whitelist`` reads it from
# ``tk.config`` live at each call so a runtime change via the admin UI
# (adding/removing a host mid-incident, e.g.) takes effect without a
# worker restart. Mirrors the ``file_hash_algorithm`` pattern below.

# Issue #221: ``ckanext.datapusher_plus.file_hash_algorithm`` is declared in
# ``config_declaration.yaml`` but intentionally NOT mirrored here as a
# module-level constant. The setting is ``editable: true`` and the download
# stage's ``_get_file_hasher`` reads it from ``tk.config`` live at each
# call so a runtime change via the admin UI takes effect without a worker
# restart. A module-import-time snapshot here would silently break that
# contract for any code that imported the snapshot instead of the live
# config — kept as a docstring rather than a constant on purpose.

# Indexing settings
# Issue #142: a column gets an auto-index when ``MIN_THRESHOLD <= cardinality
# <= AUTO_INDEX_THRESHOLD``. The Postgres planner ignores very-low-cardinality
# indexes (single-value text columns produce useless 10–40MB indexes the
# planner never chooses), so the floor exists to skip those. The
# DataTables-SearchBuilder-style filtering use case the upper threshold was
# originally built for hits the [3, 10] sweet spot for typical enum-shaped
# columns (Borough = 5 values, status = 3–10, etc.). Defaults bumped 3 → 10
# at issue #142's resolution per @EricSoroos's analysis.
#
# Edge cases:
# - ``AUTO_INDEX_THRESHOLD = -1`` keeps its legacy "no upper bound" meaning
#   (mapped to record_count in indexing.py); the MIN floor still applies, so
#   operators who want literally every column indexed (incl. cardinality < 3)
#   must also set ``AUTO_INDEX_MIN_THRESHOLD = 0``.
# - ``AUTO_INDEX_THRESHOLD = 0`` disables cardinality-based auto-indexing
#   entirely (existing contract — ``bool(AUTO_INDEX_THRESHOLD)`` checks in
#   ``analysis.py`` still gate the cardinality stats computation).
# - ``AUTO_INDEX_MIN_THRESHOLD > AUTO_INDEX_THRESHOLD`` makes the range empty;
#   the indexing stage logs a clear warning so operators see what happened.
AUTO_INDEX_THRESHOLD = tk.asint(
    tk.config.get("ckanext.datapusher_plus.auto_index_threshold", "10")
)
AUTO_INDEX_MIN_THRESHOLD = tk.asint(
    tk.config.get("ckanext.datapusher_plus.auto_index_min_threshold", "3")
)
AUTO_INDEX_DATES = tk.asbool(
    tk.config.get("ckanext.datapusher_plus.auto_index_dates", True)
)
AUTO_UNIQUE_INDEX = tk.asbool(
    tk.config.get("ckanext.datapusher_plus.auto_unique_index", True)
)

# Summary statistics settings
SUMMARY_STATS_OPTIONS = tk.config.get("ckanext.datapusher_plus.summary_stats_options")
ADD_SUMMARY_STATS_RESOURCE = tk.asbool(
    tk.config.get("ckanext.datapusher_plus.add_summary_stats_resource", False)
)
SUMMARY_STATS_WITH_PREVIEW = tk.asbool(
    tk.config.get("ckanext.datapusher_plus.summary_stats_with_preview", False)
)
QSV_STATS_STRING_MAX_LENGTH = tk.asint(
    tk.config.get("ckanext.datapusher_plus.qsv_stats_string_max_length", "32767")
)
# whitelist of case-insensitive dates patterns of column names to use for date inferencing
# date inferencing will only be attempted on columns that match the patterns
# "all" means to scan all columns as date candidates
# date inferencing is an expensive operation, as we try to match on 19 different
# date formats, so we only want to do it on columns that are likely to contain dates
# the default is "date,time,due,open,close,created"
# e.g. "created_date", "open_dt", "issue_closed", "DATE_DUE", "OPEN_DT", "CLOSED_DT", "OPEN_ISSUES"
# will all be scanned as potential date columns. Note that OPEN_ISSUES is likely not a date
# column, but it will still be scanned as a date candidate because it matches the pattern
QSV_DATES_WHITELIST = tk.config.get(
    "ckanext.datapusher_plus.qsv_dates_whitelist", "date,time,due,open,close,created"
)
QSV_FREQ_LIMIT = tk.asint(tk.config.get("ckanext.datapusher_plus.qsv_freq_limit", "10"))

# Type mapping
TYPE_MAPPING = json.loads(
    tk.config.get(
        "ckanext.datapusher_plus.type_mapping",
        '{"String": "text", "Integer": "numeric","Float": "numeric","DateTime": "timestamp","Date": "date","NULL": "text"}',
    )
)

# Alias settings
AUTO_ALIAS = tk.asbool(tk.config.get("ckanext.datapusher_plus.auto_alias", True))
AUTO_ALIAS_UNIQUE = tk.asbool(
    tk.config.get("ckanext.datapusher_plus.auto_alias_unique", True)
)

# Copy buffer size
COPY_READBUFFER_SIZE = tk.asint(
    tk.config.get("ckanext.datapusher_plus.copy_readbuffer_size", "1048576")
)

# TRUNCATE + COPY ... WITH FREEZE strategy.
#
# When ``True`` (default), DP+ runs ``TRUNCATE TABLE`` and ``COPY ... WITH
# FREEZE`` in the same transaction. ``WITH FREEZE`` marks the new rows as
# already-vacuumed, so a subsequent VACUUM doesn't have to rewrite them —
# substantial speedup on multi-million-row loads. The cost is that the
# ``AccessExclusive`` lock acquired by ``TRUNCATE`` is held for the
# full duration of the COPY, blocking *any* concurrent read of the
# datastore table. Fine for write-heavy workloads; painful for read-heavy
# workloads where the datastore is queried while an ingestion is running.
#
# When ``False``, DP+ commits the TRUNCATE immediately (brief
# ``AccessExclusive``), then runs the COPY *without* ``FREEZE`` in a
# separate transaction (only ``RowExclusive`` — allows concurrent
# ``SELECT``s). The FREEZE speedup is lost; the subsequent
# ``VACUUM ANALYZE`` still happens at the end, so eventual page state
# is identical, just paid later. See #258 for the motivating case.
USE_TRUNCATE_FREEZE = tk.asbool(
    tk.config.get("ckanext.datapusher_plus.use_truncate_freeze", True)
)

# Datastore URLs
DATASTORE_URLS = {
    "datastore_delete": "{ckan_url}/api/action/datastore_delete",
    "resource_update": "{ckan_url}/api/action/resource_update",
}

# Datastore write URL.
# Read at module import (the rest of the module's constants depend on this
# being a string-ish). A missing / empty value here surfaces later at the
# point of use — the database task's psycopg2 connect raises a clear
# OperationalError, captured by the flow's exception path and recorded on
# the Jobs row. Hard-failing at import would also break test environments
# and tooling imports (ckan db init, plugin metadata extraction) that have
# no business needing the write URL.
DATASTORE_WRITE_URL = tk.config.get("ckan.datastore.write_url")

# spatial simplification settings
AUTO_SPATIAL_SIMPLIFICATION = tk.asbool(
    tk.config.get("ckanext.datapusher_plus.auto_spatial_simplification", True)
)
SPATIAL_SIMPLIFICATION_RELATIVE_TOLERANCE = tk.config.get(
    "ckanext.datapusher_plus.SPATIAL_SIMPLIFICATION_RELATIVE_TOLERANCE", "0.1"
)

# Latitude and longitude column names
# multiple fields can be specified, separated by commas
# matching columns will be from left to right and the jinja2
# variable dpp.LAT_FIELD and dpp.LON_FIELD will be set to the
# value of the first matching column, case-insensitive
LATITUDE_FIELDS = tk.config.get(
    "ckanext.datapusher_plus.latitude_fields",
    "latitude,lat",
)
LONGITUDE_FIELDS = tk.config.get(
    "ckanext.datapusher_plus.longitude_fields",
    "longitude,lon",
)

# Auto-persist a ``dpp_spatial_extent`` BoundingBox on the resource when
# a CSV has lat/lon columns (detected via the same heuristic the
# formula engine uses, see ``jinja2_helpers.detect_lat_lon_fields``).
# Shapefile / GeoJSON resources already get this written by
# ``FormatConverterStage``; this flag controls the CSV path only.
AUTO_CSV_SPATIAL_EXTENT = tk.asbool(
    tk.config.get("ckanext.datapusher_plus.auto_csv_spatial_extent", True)
)

# AI suggestions via ``qsv describegpt``.
#
# Opt-in by design: ``ENABLE_AI_SUGGESTIONS`` defaults to False because
# the LLM call requires an OpenAI-compatible endpoint that the operator
# has to bring (Ollama, OpenRouter, OpenAI, vLLM, etc.) AND because
# every push of every resource would otherwise burn API budget /
# inference time / wall-clock seconds without consent.
#
# When enabled, the ``AISuggestionsStage`` shells out to
# ``qsv describegpt --description --dictionary --tags --json`` with the
# cached stats + frequency files from ``AnalysisStage``. The LLM
# endpoint, model, prompt, and API key all live in qsv's own config
# file (pointed at by ``DESCRIBEGPT_CONFIG_PATH``) or qsv's environment
# (``OPENAI_API_KEY``) — we deliberately do NOT proxy any of that
# through ckan.ini, both because it's qsv's surface and to keep
# secrets out of CKAN's config dump.
#
# Stage failure is non-blocking by design (try/except in the stage,
# never raises) — an LLM-endpoint outage must not gate datastore
# ingestion.
ENABLE_AI_SUGGESTIONS = tk.asbool(
    tk.config.get("ckanext.datapusher_plus.enable_ai_suggestions", False)
)
DESCRIBEGPT_CONFIG_PATH = tk.config.get(
    "ckanext.datapusher_plus.describegpt_config_path", ""
)
DESCRIBEGPT_TIMEOUT_SECONDS = tk.asint(
    tk.config.get("ckanext.datapusher_plus.describegpt_timeout_seconds", "120")
)

# Jinja2 bytecode cache settings
JINJA2_BYTECODE_CACHE_DIR = tk.config.get(
    "ckanext.datapusher_plus.jinja2_bytecode_cache_dir", "/tmp/jinja2_bytecode_cache"
)

# if a zip archive is uploaded, and it only contains one file and the file
# is one of the supported formats, automatically unzip the file and pump the
# contents into the datastore. Leave the zip file as the "main" resource.
AUTO_UNZIP_ONE_FILE = tk.asbool(
    tk.config.get("ckanext.datapusher_plus.auto_unzip_one_file", True)
)
