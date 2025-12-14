# -*- coding: utf-8 -*-
# flake8: noqa: E501

import json
import requests
from pathlib import Path

import ckan.plugins.toolkit as tk

_DEFAULT_FORMATS = [
    "csv",
    "tsv",
    "tab",
    "ssv",
    "xls",
    "xlsx",
    "ods",
    "geojson",
    "shp",
    "qgis",
    "zip",
]
_DEFAULT_TYPE_MAPPING = '{"String": "text", "Integer": "numeric","Float": "numeric","DateTime": "timestamp","Date": "date","NULL": "text"}'

# PostgreSQL integer limits
POSTGRES_INT_MAX = 2147483647
POSTGRES_INT_MIN = -2147483648
POSTGRES_BIGINT_MAX = 9223372036854775807
POSTGRES_BIGINT_MIN = -9223372036854775808

# QSV version requirements
MINIMUM_QSV_VERSION = "4.0.0"

# Datastore URLs
DATASTORE_URLS = {
    "datastore_delete": "{ckan_url}/api/action/datastore_delete",
    "resource_update": "{ckan_url}/api/action/resource_update",
}


def _as_list(value, fallback):
    if value is None:
        return list(fallback)
    if isinstance(value, str):
        return value.split()
    return value


def reload(config_obj=None):
    """
    Reload values from tk.config. Needed when config is injected at runtime
    (eg via ckanext-envars) so module-level settings pick up the latest values.
    """
    cfg = config_obj or tk.config

    global SSL_VERIFY
    SSL_VERIFY = tk.asbool(cfg.get("SSL_VERIFY"))
    if not SSL_VERIFY:
        requests.packages.urllib3.disable_warnings()

    global USE_PROXY
    USE_PROXY = "ckanext.datapusher_plus.download_proxy" in cfg
    global DOWNLOAD_PROXY
    DOWNLOAD_PROXY = (
        cfg.get("ckanext.datapusher_plus.download_proxy") if USE_PROXY else None
    )

    global UPLOAD_LOG_LEVEL
    UPLOAD_LOG_LEVEL = cfg.get("ckanext.datapusher_plus.upload_log_level", "INFO")

    global FORMATS
    FORMATS = _as_list(
        cfg.get("ckanext.datapusher_plus.formats", _DEFAULT_FORMATS), _DEFAULT_FORMATS
    )

    global PII_SCREENING
    PII_SCREENING = tk.asbool(
        cfg.get("ckanext.datastore_plus.pii_screening", False)
    )
    global PII_FOUND_ABORT
    PII_FOUND_ABORT = tk.asbool(
        cfg.get("ckanext.datapusher_plus.pii_found_abort", False)
    )
    global PII_REGEX_RESOURCE_ID
    PII_REGEX_RESOURCE_ID = cfg.get(
        "ckanext.datapusher_plus.pii_regex_resource_id_or_alias"
    )
    global PII_SHOW_CANDIDATES
    PII_SHOW_CANDIDATES = tk.asbool(
        cfg.get("ckanext.datapusher_plus.pii_show_candidates", False)
    )
    global PII_QUICK_SCREEN
    PII_QUICK_SCREEN = tk.asbool(
        cfg.get("ckanext.datapusher_plus.pii_quick_screen", False)
    )

    global QSV_BIN
    QSV_BIN = Path(cfg.get("ckanext.datapusher_plus.qsv_bin"))

    global PREVIEW_ROWS
    PREVIEW_ROWS = tk.asint(cfg.get("ckanext.datapusher_plus.preview_rows", "1000"))
    global TIMEOUT
    TIMEOUT = tk.asint(cfg.get("ckanext.datapusher_plus.download_timeout", "300"))
    global MAX_CONTENT_LENGTH
    MAX_CONTENT_LENGTH = tk.asint(
        cfg.get("ckanext.datapusher_plus.max_content_length", "5000000")
    )
    global CHUNK_SIZE
    CHUNK_SIZE = tk.asint(cfg.get("ckanext.datapusher_plus.chunk_size", "1048576"))
    global DEFAULT_EXCEL_SHEET
    DEFAULT_EXCEL_SHEET = tk.asint(
        cfg.get("ckanext.datapusher_plus.default_excel_sheet", 0)
    )
    global SORT_AND_DUPE_CHECK
    SORT_AND_DUPE_CHECK = tk.asbool(
        cfg.get("ckanext.datapusher_plus.sort_and_dupe_check", True)
    )
    global DEDUP
    DEDUP = tk.asbool(cfg.get("ckanext.datapusher_plus.dedup", True))
    global UNSAFE_PREFIX
    UNSAFE_PREFIX = cfg.get("ckanext.datapusher_plus.unsafe_prefix", "unsafe_")
    global RESERVED_COLNAMES
    RESERVED_COLNAMES = cfg.get("ckanext.datapusher_plus.reserved_colnames", "_id")
    global PREFER_DMY
    PREFER_DMY = tk.asbool(cfg.get("ckanext.datapusher_plus.prefer_dmy", False))
    global IGNORE_FILE_HASH
    IGNORE_FILE_HASH = tk.asbool(
        cfg.get("ckanext.datapusher_plus.ignore_file_hash", False)
    )

    global AUTO_INDEX_THRESHOLD
    AUTO_INDEX_THRESHOLD = tk.asint(
        cfg.get("ckanext.datapusher_plus.auto_index_threshold", "3")
    )
    global AUTO_INDEX_DATES
    AUTO_INDEX_DATES = tk.asbool(
        cfg.get("ckanext.datapusher_plus.auto_index_dates", True)
    )
    global AUTO_UNIQUE_INDEX
    AUTO_UNIQUE_INDEX = tk.asbool(
        cfg.get("ckanext.datapusher_plus.auto_unique_index", True)
    )

    global SUMMARY_STATS_OPTIONS
    SUMMARY_STATS_OPTIONS = cfg.get("ckanext.datapusher_plus.summary_stats_options")
    global ADD_SUMMARY_STATS_RESOURCE
    ADD_SUMMARY_STATS_RESOURCE = tk.asbool(
        cfg.get("ckanext.datapusher_plus.add_summary_stats_resource", False)
    )
    global SUMMARY_STATS_WITH_PREVIEW
    SUMMARY_STATS_WITH_PREVIEW = tk.asbool(
        cfg.get("ckanext.datapusher_plus.summary_stats_with_preview", False)
    )
    global QSV_STATS_STRING_MAX_LENGTH
    QSV_STATS_STRING_MAX_LENGTH = tk.asint(
        cfg.get("ckanext.datapusher_plus.qsv_stats_string_max_length", "32767")
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
    global QSV_DATES_WHITELIST
    QSV_DATES_WHITELIST = cfg.get(
        "ckanext.datapusher_plus.qsv_dates_whitelist", "date,time,due,open,close,created"
    )
    global QSV_FREQ_LIMIT
    QSV_FREQ_LIMIT = tk.asint(cfg.get("ckanext.datapusher_plus.qsv_freq_limit", "10"))

    global TYPE_MAPPING
    TYPE_MAPPING = json.loads(
        cfg.get("ckanext.datapusher_plus.type_mapping", _DEFAULT_TYPE_MAPPING)
    )

    global AUTO_ALIAS
    AUTO_ALIAS = tk.asbool(cfg.get("ckanext.datapusher_plus.auto_alias", True))
    global AUTO_ALIAS_UNIQUE
    AUTO_ALIAS_UNIQUE = tk.asbool(
        cfg.get("ckanext.datapusher_plus.auto_alias_unique", True)
    )

    global COPY_READBUFFER_SIZE
    COPY_READBUFFER_SIZE = tk.asint(
        cfg.get("ckanext.datapusher_plus.copy_readbuffer_size", "1048576")
    )

    global DATASTORE_WRITE_URL
    DATASTORE_WRITE_URL = cfg.get("ckan.datastore.write_url")

    global AUTO_SPATIAL_SIMPLIFICATION
    AUTO_SPATIAL_SIMPLIFICATION = tk.asbool(
        cfg.get("ckanext.datapusher_plus.auto_spatial_simplification", True)
    )
    global SPATIAL_SIMPLIFICATION_RELATIVE_TOLERANCE
    SPATIAL_SIMPLIFICATION_RELATIVE_TOLERANCE = float(
        cfg.get("ckanext.datapusher_plus.SPATIAL_SIMPLIFICATION_RELATIVE_TOLERANCE", "0.1")
    )

    # Latitude and longitude column names
    # multiple fields can be specified, separated by commas
    # matching columns will be from left to right and the jinja2
    # variable dpp.LAT_FIELD and dpp.LON_FIELD will be set to the
    # value of the first matching column, case-insensitive
    global LATITUDE_FIELDS
    LATITUDE_FIELDS = cfg.get(
        "ckanext.datapusher_plus.latitude_fields",
        "latitude,lat",
    )
    global LONGITUDE_FIELDS
    LONGITUDE_FIELDS = cfg.get(
        "ckanext.datapusher_plus.longitude_fields",
        "longitude,lon",
    )

    # Jinja2 bytecode cache settings
    global JINJA2_BYTECODE_CACHE_DIR
    JINJA2_BYTECODE_CACHE_DIR = cfg.get(
        "ckanext.datapusher_plus.jinja2_bytecode_cache_dir",
        "/tmp/jinja2_bytecode_cache",
    )

    # if a zip archive is uploaded, and it only contains one file and the file
    # is one of the supported formats, automatically unzip the file and pump the
    # contents into the datastore. Leave the zip file as the "main" resource.
    global AUTO_UNZIP_ONE_FILE
    AUTO_UNZIP_ONE_FILE = tk.asbool(
        cfg.get("ckanext.datapusher_plus.auto_unzip_one_file", True)
    )


reload()
