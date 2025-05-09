# -*- coding: utf-8 -*-
# flake8: noqa: E501

import json
import requests
from pathlib import Path
import ckan.plugins.toolkit as tk

# SSL verification settings
SSL_VERIFY = tk.asbool(tk.config.get("SSL_VERIFY"))
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
MINIMUM_QSV_VERSION = "4.0.0"

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

# Binary paths
QSV_BIN = Path(tk.config.get("ckanext.datapusher_plus.qsv_bin"))

# Data processing settings
PREVIEW_ROWS = tk.asint(tk.config.get("ckanext.datapusher_plus.preview_rows", "1000"))
TIMEOUT = tk.asint(tk.config.get("ckanext.datapusher_plus.download_timeout", "300"))
MAX_CONTENT_LENGTH = tk.asint(
    tk.config.get("ckanext.datapusher_plus.max_content_length", "5000000")
)
CHUNK_SIZE = tk.asint(tk.config.get("ckanext.datapusher_plus.chunk_size", "1048576"))
DEFAULT_EXCEL_SHEET = tk.asint(tk.config.get("DEFAULT_EXCEL_SHEET", 0))
SORT_AND_DUPE_CHECK = tk.asbool(
    tk.config.get("ckanext.datapusher_plus.sort_and_dupe_check", True)
)
DEDUP = tk.asbool(tk.config.get("ckanext.datapusher_plus.dedup", True))
UNSAFE_PREFIX = tk.config.get("ckanext.datapusher_plus.unsafe_prefix", "unsafe_")
RESERVED_COLNAMES = tk.config.get("ckanext.datapusher_plus.reserved_colnames", "_id")
PREFER_DMY = tk.asbool(tk.config.get("ckanext.datapusher_plus.prefer_dmy", False))
IGNORE_FILE_HASH = tk.asbool(
    tk.config.get("ckanext.datapusher_plus.ignore_file_hash", False)
)

# Indexing settings
AUTO_INDEX_THRESHOLD = tk.asint(
    tk.config.get("ckanext.datapusher_plus.auto_index_threshold", "3")
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

# Datastore URLs
DATASTORE_URLS = {
    "datastore_delete": "{ckan_url}/api/action/datastore_delete",
    "resource_update": "{ckan_url}/api/action/resource_update",
}

# Datastore write URL
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
