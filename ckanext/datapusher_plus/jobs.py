# -*- coding: utf-8 -*-

# Standard library imports
import csv
import datetime
import hashlib
import locale
import mimetypes
import os
import shutil
import subprocess
import tempfile
import time
import decimal
from urllib.parse import urlsplit
from urllib.parse import urlparse
import logging

# Third-party imports
import psycopg2
from ckanext.datapusher_plus.job_exceptions import HTTPError
from datasize import DataSize
from dateutil.parser import parse as parsedate
import json
import requests

from urllib.parse import urlsplit
import datetime
import locale
import logging
import decimal
import hashlib
import time
import tempfile
import subprocess
import csv
import os
import psycopg2
import semver
import sys
import traceback

import sqlalchemy as sa
from pathlib import Path
from datasize import DataSize
from psycopg2 import sql
from dateutil.parser import parse as parsedate

from rq import get_current_job
import ckan.plugins.toolkit as tk

from jinja2 import DictLoader, Environment

import ckanext.datapusher_plus.utils as utils
import ckanext.datapusher_plus.helpers as dph

# from ckanext.datapusher_plus.config import config


if locale.getdefaultlocale()[0]:
    lang, encoding = locale.getdefaultlocale()
    locale.setlocale(locale.LC_ALL, locale=(lang, encoding))
else:
    locale.setlocale(locale.LC_ALL, "")


SSL_VERIFY = tk.asbool(tk.config.get("SSL_VERIFY"))
if not SSL_VERIFY:
    requests.packages.urllib3.disable_warnings()

USE_PROXY = "ckanext.datapusher_plus.download_proxy" in tk.config
if USE_PROXY:
    DOWNLOAD_PROXY = tk.config.get("ckanext.datapusher_plus.download_proxy")

POSTGRES_INT_MAX = 2147483647
POSTGRES_INT_MIN = -2147483648
POSTGRES_BIGINT_MAX = 9223372036854775807
POSTGRES_BIGINT_MIN = -9223372036854775808

MINIMUM_QSV_VERSION = "4.0.0"

PII_SCREENING = tk.asbool(tk.config.get("ckanext.datastore_plus.pii_screening", False))
QSV_BIN = Path(tk.config.get("ckanext.datapusher_plus.qsv_bin"))
FILE_BIN = Path(tk.config.get("ckanext.datapusher_plus.file_bin"))
PREVIEW_ROWS = tk.asint(tk.config.get("ckanext.datapusher_plus.preview_rows", "1000"))
TIMEOUT = tk.asint(tk.config.get("ckanext.datapusher_plus.download_timeout", "300"))
MAX_CONTENT_LENGTH = tk.asint(
    tk.config.get("ckanext.datapusher_plus.max_content_length", "5000000")
)
CHUNK_SIZE = tk.asint(tk.config.get("ckanext.datapusher_plus.chunk_size", "1048576"))
DEFAULT_EXCEL_SHEET = tk.asint(tk.config.get("DEFAULT_EXCEL_SHEET", 1))
SORT_AND_DUPE_CHECK = tk.asbool(
    tk.config.get("ckanext.datapusher_plus.sort_and_dupe_check", True)
)
DEDUP = tk.asbool(tk.config.get("ckanext.datapusher_plus.dedup", True))
UNSAFE_PREFIX = tk.config.get("ckanext.datapusher_plus.unsafe_prefix", "unsafe_")
RESERVED_COLNAMES = tk.config.get("ckanext.datapusher_plus.reserved_colnames", "_id")
PREFER_DMY = tk.asbool(tk.config.get("ckanext.datapusher_plus.prefer_dmy", False))
AUTO_INDEX_THRESHOLD = tk.asint(
    tk.config.get("ckanext.datapusher_plus.auto_index_threshold", "3")
)
SUMMARY_STATS_OPTIONS = tk.config.get("ckanext.datapusher_plus.summary_stats_options")
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
COPY_READBUFFER_SIZE = tk.asint(
    tk.config.get("ckanext.datapusher_plus.copy_readbuffer_size", "1048576")
)
AUTO_INDEX_DATES = tk.asbool(
    tk.config.get("ckanext.datapusher_plus.auto_index_dates", True)
)
AUTO_UNIQUE_INDEX = tk.asbool(
    tk.config.get("ckanext.datapusher_plus.auto_unique_index", True)
)
TYPE_MAPPING = json.loads(
    tk.config.get(
        "ckanext.datapusher_plus.type_mapping",
        '{"String": "text", "Integer": "numeric","Float": "numeric","DateTime": "timestamp","Date": "timestamp","NULL": "text"}',
    )
)
AUTO_ALIAS = tk.asbool(tk.config.get("ckanext.datapusher_plus.auto_alias", True))
AUTO_ALIAS_UNIQUE = tk.asbool(
    tk.config.get("ckanext.datapusher_plus.auto_alias_unique", True)
)

ADD_SUMMARY_STATS_RESOURCE = tk.asbool(
    tk.config.get("ckanext.datapusher_plus.add_summary_stats_resource", False)
)
SUMMARY_STATS_WITH_PREVIEW = tk.asbool(
    tk.config.get("ckanext.datapusher_plus.summary_stats_with_preview", False)
)


DATASTORE_URLS = {
    "datastore_delete": "{ckan_url}/api/action/datastore_delete",
    "resource_update": "{ckan_url}/api/action/resource_update",
}


def get_url(action, ckan_url):
    """
    Get url for ckan action
    """
    if not urlsplit(ckan_url).scheme:
        ckan_url = "http://" + ckan_url.lstrip("/")  # DevSkim: ignore DS137138
    ckan_url = ckan_url.rstrip("/")
    return "{ckan_url}/api/3/action/{action}".format(ckan_url=ckan_url, action=action)


class DatastoreEncoder(json.JSONEncoder):
    # Custom JSON encoder
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        if isinstance(obj, decimal.Decimal):
            return str(obj)

        return json.JSONEncoder.default(self, obj)


def delete_datastore_resource(resource_id):
    try:
        tk.get_action("datastore_delete")(
            {"ignore_auth": True}, {"resource_id": resource_id, "force": True}
        )
    except tk.ObjectNotFound:
        raise utils.JobError("Deleting existing datastore failed.")


def delete_resource(resource_id):
    if not tk.user:
        raise utils.JobError("No user found.")
    try:
        tk.get_action("resource_delete")(
            {"user": tk.user}, {"id": resource_id, "force": True}
        )
    except tk.ObjectNotFound:
        raise utils.JobError("Deleting existing resource failed.")


def datastore_resource_exists(resource_id):

    data_dict = {
        "resource_id": resource_id,
        "limit": 0,
        "include_total": False,
    }

    context = {"ignore_auth": True}

    try:
        result = tk.get_action("datastore_search")(context, data_dict)
        return result
    except tk.ObjectNotFound:
        return False


def send_resource_to_datastore(
    resource,
    resource_id,
    headers,
    records,
    aliases,
    calculate_record_count,
):
    """
    Stores records in CKAN datastore
    """

    if resource_id:
        # used to create the "main" resource
        request = {
            "resource_id": resource_id,
            "fields": headers,
            "force": True,
            "records": records,
            "aliases": aliases,
            "calculate_record_count": calculate_record_count,
        }
    else:
        # used to create the "stats" resource
        request = {
            "resource": resource,
            "fields": headers,
            "force": True,
            "aliases": aliases,
            "calculate_record_count": calculate_record_count,
        }
    try:
        resource_dict = tk.get_action("datastore_create")(
            {"ignore_auth": True}, request
        )
        return resource_dict
    except Exception as e:
        raise utils.JobError("Error sending data to datastore ({!s}).".format(e))


def update_resource(resource):
    """
    Updates resource metadata
    """
    site_user = tk.get_action("get_site_user")({"ignore_auth": True}, {})
    context = {"ignore_auth": True, "user": site_user["name"], "auth_user_obj": None}
    try:
        tk.get_action("resource_update")(context, resource)
    except tk.ObjectNotFound:
        raise utils.JobError("Updating existing resource failed.")


def get_resource(resource_id):
    """
    Gets available information about the resource from CKAN
    """
    resource_dict = tk.get_action("resource_show")(
        {"ignore_auth": True}, {"id": resource_id}
    )

    return resource_dict


def get_package(package_id):
    """
    Gets available information about a package from CKAN
    """
    dataset_dict = tk.get_action("package_show")(
        {"ignore_auth": True}, {"id": package_id}
    )

    return dataset_dict

def patch_package(package):
    """
    Patches package metadata
    """
    site_user = tk.get_action("get_site_user")({"ignore_auth": True}, {})
    context = {"ignore_auth": True, "user": site_user["name"], "auth_user_obj": None}
    patched_package = tk.get_action("package_patch")(context, package)
    return patched_package

# def revise_package(package, match=None, update=None, append=None, remove=None, force=False):
#     """
#     Revises package metadata using the package_revise action API.
    
#     Args:
#         package (dict): The package dict containing the package ID to revise
#         match (dict, optional): Fields that must match the current version of the package
#         update (dict, optional): Fields to update to new values
#         append (dict, optional): Lists to append to
#         remove (dict, optional): Lists to remove from
#         force (bool, optional): Set to True to force update even if match fails. Default False.

#     Returns:
#         dict: The revised package metadata

#     Example:
#         revise_package(
#             package={'id': '123'}, 
#             update={'title': 'New Title'},
#             append={'tags': [{'name': 'new_tag'}]},
#             remove={'tags': [{'name': 'old_tag'}]}
#         )
#     """
#     site_user = tk.get_action("get_site_user")({"ignore_auth": True}, {})
#     context = {"ignore_auth": True, "user": site_user["name"], "auth_user_obj": None}
    
#     data_dict = {
#         'match': match or {},
#         'update': update or {},
#         'append': append or {},
#         'remove': remove or {},
#         'force': force
#     }
    
#     # Add package ID if provided
#     if isinstance(package, dict) and 'id' in package:
#         data_dict['id'] = package['id']
#     elif isinstance(package, str):
#         data_dict['id'] = package
#     else:
#         raise ValueError("Package must be either a package dict with 'id' or a package ID string")

#     revised_package = tk.get_action("package_revise")(context, data_dict)
#     return revised_package

def revise_package(package_id, match={}, filter=None, update=None, include=None):
    """
    Revises package metadata using the package_revise action API.
    
    Args:
        package_id (str): The ID of the package to revise
        match (dict, optional): Fields that must match the current version of the package
        filter (list, optional): List of fields to remove from the package
        update (dict, optional): Fields to update to new values 
        include (list, optional): List of fields to include in the response

    Returns:
        dict: The revised package metadata
    """
    site_user = tk.get_action("get_site_user")({"ignore_auth": True}, {})
    context = {"ignore_auth": True, "user": site_user["name"], "auth_user_obj": None}

    # package_id is required
    if not package_id:
        raise ValueError("Package ID is required")

    # add package_id to match
    match['id'] = package_id

    data_dict = {
        'match': match,
        'filter': filter or [], # Must be a list
        'update': update or {},
        'include': include or [], # Must be a list
        'id': package_id
    }

    dph.datastore_log(f"Revising package with data_dict: {data_dict}", logging.INFO)

    revised_package = tk.get_action("package_revise")(context, data_dict)
    return revised_package

def get_scheming_yaml(package_id):
    """
    Gets the scheming yaml for a package
    """
    package = get_package(package_id)
    if not package:
        raise utils.JobError("Package not found")
    type = package.get("type")

    scheming_yaml = tk.get_action("scheming_dataset_schema_show")(
        {"ignore_auth": True}, {"type": "dataset"}
    )

    return scheming_yaml, package


def validate_input(input):
    # Especially validate metadata which is provided by the user
    if "metadata" not in input:
        raise utils.JobError("Metadata missing")

    data = input["metadata"]

    if "resource_id" not in data:
        raise utils.JobError("No id provided.")
    # if "ckan_url" not in data:
    #     raise utils.JobError("No ckan_url provided.")
    # if not input.get("api_key"):
    #     raise utils.JobError("No CKAN API key provided")


def callback_datapusher_hook(result_url, job_dict):
    # api_key_from_job = job_dict.pop("api_key", None)
    # if not api_key:
    #     api_key = api_key_from_job
    api_token = utils.get_dp_plus_user_apitoken()
    headers = {"Content-Type": "application/json", "Authorization": api_token}

    try:
        result = requests.post(
            result_url,
            data=json.dumps(job_dict, cls=utils.DatetimeJsonEncoder),
            verify=SSL_VERIFY,
            headers=headers,
        )
    except requests.ConnectionError:
        return False

    return result.status_code == requests.codes.ok


def datapusher_plus_to_datastore(input):
    """
    This is the main function that is called by the datapusher_plus worker

    Errors are caught and logged in the database

    """
    job_dict = dict(metadata=input["metadata"], status="running")
    callback_datapusher_hook(result_url=input["result_url"], job_dict=job_dict)

    job_id = get_current_job().id
    errored = False
    try:
        push_to_datastore(input, job_id)
        job_dict["status"] = "complete"
        dph.mark_job_as_completed(job_id, job_dict)
    except utils.JobError as e:
        dph.mark_job_as_errored(job_id, str(e))
        job_dict["status"] = "error"
        job_dict["error"] = str(e)
        log = logging.getLogger(__name__)
        log.error("Datapusher Plus error: {0}, {1}".format(e, traceback.format_exc()))
        errored = True
    except Exception as e:
        dph.mark_job_as_errored(
            job_id, traceback.format_tb(sys.exc_info()[2])[-1] + repr(e)
        )
        job_dict["status"] = "error"
        job_dict["error"] = str(e)
        log = logging.getLogger(__name__)
        log.error("Datapusher Plus error: {0}, {1}".format(e, traceback.format_exc()))
        errored = True
    finally:
        # job_dict is defined in datapusher_hook's docstring
        is_saved_ok = callback_datapusher_hook(
            result_url=input["result_url"], job_dict=job_dict
        )
        errored = errored or not is_saved_ok
    return "error" if errored else None


def push_to_datastore(input, task_id, dry_run=False):
    """Download and parse a resource push its data into CKAN's DataStore.

    An asynchronous job that gets a resource from CKAN, downloads the
    resource's data file and, if the data file has changed since last time,
    parses the data and posts it into CKAN's DataStore.

    :param dry_run: Fetch and parse the data file but don't actually post the
        data to the DataStore, instead return the data headers and rows that
        would have been posted.
    :type dry_run: boolean

    """

    # Ensure temporary files are removed after run
    with tempfile.TemporaryDirectory() as temp_dir:
        return _push_to_datastore(task_id, input, dry_run=dry_run, temp_dir=temp_dir)


def _push_to_datastore(task_id, input, dry_run=False, temp_dir=None):
    # add job to dn  (datapusher_plus_jobs table)
    try:
        dph.add_pending_job(task_id, **input)
    except sa.exc.IntegrityError:
        raise utils.JobError("Job already exists.")
    handler = utils.StoringHandler(task_id, input)
    logger = logging.getLogger(task_id)
    logger.addHandler(handler)
    # also show logs on stderr
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.DEBUG)

    # check if QSV_BIN and FILE_BIN exists
    # qsv_path = Path(QSV_BIN)
    if not QSV_BIN.is_file():
        raise utils.JobError("{} not found.".format(QSV_BIN))

    if not FILE_BIN.is_file():
        raise utils.JobError("{} not found.".format(FILE_BIN))

    # make sure qsv binary variant is up-to-date
    try:
        qsv_version = subprocess.run(
            [QSV_BIN, "--version"],
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as e:
        raise utils.JobError("qsv version check error: {}".format(e))
    qsv_version_info = str(qsv_version.stdout)
    if not qsv_version_info:
        """
        Sample response
        qsvdp 0.134.0-mimalloc-Luau 0.640;polars-0.42.0-fe04390;...
        """
        raise utils.JobError(
            f"We expect qsv version info to be returned. Command: {QSV_BIN} --version. Response: {qsv_version_info}"
        )
    qsv_semver = qsv_version_info[
        qsv_version_info.find(" ") : qsv_version_info.find("-")
    ].lstrip()
    logger.info("qsv version found: {}".format(qsv_semver))
    try:
        if semver.compare(qsv_semver, MINIMUM_QSV_VERSION) < 0:
            raise utils.JobError(
                "At least qsv version {} required. Found {}. You can get the latest release at https://github.com/jqnatividad/qsv/releases/latest".format(
                    MINIMUM_QSV_VERSION, qsv_version_info
                )
            )
    except ValueError as e:
        raise utils.JobError("Cannot parse qsv version info: {}".format(e))

    validate_input(input)

    data = input["metadata"]

    ckan_url = data["ckan_url"]
    resource_id = data["resource_id"]
    try:
        resource = get_resource(resource_id)
    except utils.JobError:
        # try again in 5 seconds just incase CKAN is slow at adding resource
        time.sleep(5)
        resource = get_resource(resource_id)

    # check if the resource url_type is a datastore
    if resource.get("url_type") == "datastore":
        logger.info("Dump files are managed with the Datastore API")
        return

    # check scheme
    resource_url = resource.get("url")
    scheme = urlsplit(resource_url).scheme
    if scheme not in ("http", "https", "ftp"):
        raise utils.JobError("Only http, https, and ftp resources may be fetched.")

    # ==========================================================================
    # DOWNLOAD
    # ==========================================================================
    timer_start = time.perf_counter()

    # fetch the resource data
    logger.info("Fetching from: {0}...".format(resource_url))
    headers = {}
    if resource.get("url_type") == "upload":
        # If this is an uploaded file to CKAN, authenticate the request,
        # otherwise we won't get file from private resources
        api_token = utils.get_dp_plus_user_apitoken()
        headers["Authorization"] = api_token

        # If the ckan_url differs from this url, rewrite this url to the ckan
        # url. This can be useful if ckan is behind a firewall.
        if not resource_url.startswith(ckan_url):
            new_url = urlparse(resource_url)
            rewrite_url = urlparse(ckan_url)
            new_url = new_url._replace(
                scheme=rewrite_url.scheme, netloc=rewrite_url.netloc
            )
            resource_url = new_url.geturl()
            logger.info("Rewrote resource url to: {0}".format(resource_url))

    try:
        kwargs = {
            "headers": headers,
            "timeout": tk.asint(
                tk.config.get("ckanext.datapusher_plus.download_timeout")
            ),
            "verify": SSL_VERIFY,
            "stream": True,
        }
        if USE_PROXY:
            kwargs["proxies"] = {"http": DOWNLOAD_PROXY, "https": DOWNLOAD_PROXY}
        with requests.get(resource_url, **kwargs) as response:
            response.raise_for_status()

            cl = response.headers.get("content-length")
            max_content_length = int(
                tk.config.get("ckanext.datapusher_plus.max_content_length")
            )
            ct = response.headers.get("content-type")

            try:
                if cl and int(cl) > max_content_length and PREVIEW_ROWS > 0:
                    raise utils.JobError(
                        "Resource too large to download: {cl:.2MB} > max ({max_cl:.2MB}).".format(
                            cl=DataSize(int(cl)),
                            max_cl=DataSize(int(max_content_length)),
                        )
                    )
            except ValueError:
                pass

            resource_format = resource.get("format").upper()

            # if format was not specified, try to get it from mime type
            if not resource_format:
                logger.info("File format: NOT SPECIFIED")
                # if we have a mime type, get the file extension from the response header
                if ct:
                    resource_format = mimetypes.guess_extension(ct.split(";")[0])

                    if resource_format is None:
                        raise utils.JobError(
                            "Cannot determine format from mime type. Please specify format."
                        )
                    logger.info("Inferred file format: {}".format(resource_format))
                else:
                    raise utils.JobError(
                        "Server did not return content-type. Please specify format."
                    )
            else:
                logger.info("File format: {}".format(resource_format))

            tmp = os.path.join(temp_dir, "tmp." + resource_format)
            length = 0
            # using MD5 for file deduplication only
            # no need for it to be cryptographically secure
            m = hashlib.md5()  # DevSkim: ignore DS126858

            # download the file
            if cl:
                logger.info("Downloading {:.2MB} file...".format(DataSize(int(cl))))
            else:
                logger.info("Downloading file of unknown size...")

            with open(tmp, "wb") as tmp_file:
                for chunk in response.iter_content(
                    int(tk.config.get("ckanext.datapusher_plus.chunk_size"))
                ):
                    length += len(chunk)
                    if length > max_content_length and not PREVIEW_ROWS:
                        raise utils.JobError(
                            "Resource too large to process: {cl} > max ({max_cl}).".format(
                                cl=length, max_cl=max_content_length
                            )
                        )
                    tmp_file.write(chunk)
                    m.update(chunk)

    except requests.HTTPError as e:
        raise HTTPError(
            "DataPusher+ received a bad HTTP response when trying to download "
            "the data file",
            status_code=e.response.status_code,
            request_url=resource_url,
            response=e.response.content,
        )
    except requests.RequestException as e:
        raise HTTPError(
            message=str(e), status_code=None, request_url=resource_url, response=None
        )

    file_hash = m.hexdigest()

    # check if the resource metadata (like data dictionary data types)
    # has been updated since the last fetch
    resource_updated = False
    resource_last_modified = resource.get("last_modified")
    if resource_last_modified:
        resource_last_modified = parsedate(resource_last_modified)
        file_last_modified = response.headers.get("last-modified")
        if file_last_modified:
            file_last_modified = parsedate(file_last_modified).replace(tzinfo=None)
            if file_last_modified < resource_last_modified:
                resource_updated = True

    if (
        resource.get("hash") == file_hash
        and not data.get("ignore_hash")
        and not tk.config.get("ckanext.datapusher_plus.ignore_file_hash")
        and not resource_updated
    ):
        logger.warning(
            "Upload skipped as the file hash hasn't changed: {hash}.".format(
                hash=file_hash
            )
        )
        return

    resource["hash"] = file_hash

    fetch_elapsed = time.perf_counter() - timer_start
    logger.info(
        "Fetched {:.2MB} file in {:,.2f} seconds.".format(
            DataSize(length), fetch_elapsed
        )
    )

    # ===================================================================================
    # ANALYZE WITH QSV
    # ===================================================================================
    # Start Analysis using qsv instead of messytables, as
    # 1) its type inferences are bullet-proof not guesses as it scans the entire file,
    # 2) its super-fast, and
    # 3) it has addl data-wrangling capabilities we use in DP+ (e.g. stats, dedup, etc.)
    analysis_start = time.perf_counter()
    logger.info("ANALYZING WITH QSV..")

    # ----------------- is it a spreadsheet? ---------------
    # check content type or file extension if its a spreadsheet
    spreadsheet_extensions = ["XLS", "XLSX", "ODS", "XLSM", "XLSB"]
    format = resource.get("format").upper()
    if format in spreadsheet_extensions:
        # if so, export spreadsheet as a CSV file
        default_excel_sheet = tk.config.get("DEFAULT_EXCEL_SHEET")
        logger.info(
            "Converting {} sheet {} to CSV...".format(format, default_excel_sheet)
        )
        # first, we need a temporary spreadsheet filename with the right file extension
        # we only need the filename though, that's why we remove it
        # and create a hardlink to the file we got from CKAN
        qsv_spreadsheet = os.path.join(temp_dir, "qsv_spreadsheet." + resource_format)
        os.link(tmp, qsv_spreadsheet)

        # run `qsv excel` and export it to a CSV
        # use --trim option to trim column names and the data
        qsv_excel_csv = os.path.join(temp_dir, "qsv_excel.csv")
        try:
            qsv_excel = subprocess.run(
                [
                    QSV_BIN,
                    "excel",
                    qsv_spreadsheet,
                    "--sheet",
                    str(default_excel_sheet),
                    "--trim",
                    "--output",
                    qsv_excel_csv,
                ],
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as e:
            logger.error(
                "Upload aborted. Cannot export spreadsheet(?) to CSV: {}".format(e)
            )

            # it had a spreadsheet extension but `qsv excel` failed,
            # get some file info and log it by running `file`
            # just in case the file is not actually a spreadsheet or is encrypted
            # so the user has some actionable info
            file_format = subprocess.run(
                [FILE_BIN, qsv_spreadsheet],
                check=True,
                capture_output=True,
                text=True,
            )

            logger.warning(
                "Is the file encrypted or is not a spreadsheet?\nFILE ATTRIBUTES: {}".format(
                    file_format.stdout
                )
            )

            return
        excel_export_msg = qsv_excel.stderr
        logger.info("{}...".format(excel_export_msg))
        tmp = qsv_excel_csv
    else:
        # -------------- its not a spreadsheet, its a CSV/TSV/TAB file ---------------
        # Normalize & transcode to UTF-8 using `qsv input`. We need to normalize as
        # it could be a CSV/TSV/TAB dialect with differing delimiters, quoting, etc.
        # Using qsv input's --output option also auto-transcodes to UTF-8.
        # Note that we only change the workfile, the resource file itself is unchanged.

        # ------------------- Normalize to CSV ---------------------
        qsv_input_csv = os.path.join(temp_dir, "qsv_input.csv")
        # if resource_format is CSV we don't need to normalize
        if resource_format.upper() == "CSV":
            logger.info("Normalizing/UTF-8 transcoding {}...".format(resource_format))
        else:
            # if not CSV (e.g. TSV, TAB, etc.) we need to normalize to CSV
            logger.info(
                "Normalizing/UTF-8 transcoding {} to CSV...".format(resource_format)
            )

        qsv_input_utf_8_encoded_csv = os.path.join(
            temp_dir, "qsv_input_utf_8_encoded.csv"
        )

        # using uchardet to determine encoding
        file_encoding = subprocess.run(
            ["uchardet", tmp],
            check=True,
            capture_output=True,
            text=True,
        )
        logger.info("Identified encoding of the file: {}".format(file_encoding.stdout))

        # trim the encoding string
        file_encoding.stdout = file_encoding.stdout.strip()

        # using iconv to re-encode in UTF-8 OR ASCII (as ASCII is a subset of UTF-8)
        if file_encoding.stdout != "UTF-8" and file_encoding.stdout != "ASCII":
            logger.info(
                "File is not UTF-8 encoded. Re-encoding from {} to UTF-8".format(
                    file_encoding.stdout
                )
            )
            try:
                cmd = subprocess.run(
                    [
                        "iconv",
                        "-f",
                        file_encoding.stdout,
                        "-t",
                        "UTF-8",
                        tmp,
                    ],
                    capture_output=True,
                    check=True,
                )
            except subprocess.CalledProcessError as e:
                logger.error(
                    f"Job aborted as the file cannot be re-encoded to UTF-8. {e.stderr}"
                )
                return
            f = open(qsv_input_utf_8_encoded_csv, "wb")
            f.write(cmd.stdout)
            f.close()
            logger.info("Successfully re-encoded to UTF-8")

        else:
            qsv_input_utf_8_encoded_csv = tmp
        try:
            qsv_input = subprocess.run(
                [
                    QSV_BIN,
                    "input",
                    tmp,
                    "--trim-headers",
                    "--output",
                    qsv_input_csv,
                ],
                check=True,
            )
        except subprocess.CalledProcessError as e:
            # return as we can't push an invalid CSV file
            logger.error(
                "Job aborted as the file cannot be normalized/transcoded: {}.".format(e)
            )
            return
        tmp = qsv_input_csv
        logger.info("Normalized & transcoded...")

    # ------------------------------------- Validate CSV --------------------------------------
    # Run an RFC4180 check with `qsv validate` against the normalized, UTF-8 encoded CSV file.
    # Even excel exported CSVs can be potentially invalid, as it allows the export of "flexible"
    # CSVs - i.e. rows may have different column counts.
    # If it passes validation, we can handle it with confidence downstream as a "normal" CSV.
    logger.info("Validating CSV...")
    try:
        subprocess.run(
            [QSV_BIN, "validate", tmp], check=True, capture_output=True, text=True
        )
    except subprocess.CalledProcessError as e:
        # return as we can't push an invalid CSV file
        validate_error_msg = e.stderr
        logger.error("Invalid CSV! Job aborted: {}.".format(validate_error_msg))
        return
    logger.info("Well-formed, valid CSV file confirmed...")

    # --------------------- Sortcheck --------------------------
    # if SORT_AND_DUPE_CHECK is True or DEDUP is True
    # check if the file is sorted and if it has duplicates
    # get the record count, unsorted breaks and duplicate count as well
    if SORT_AND_DUPE_CHECK or DEDUP:
        logger.info("Checking for duplicates and if the CSV is sorted...")
        try:
            qsv_sortcheck = subprocess.run(
                [QSV_BIN, "sortcheck", tmp, "--json"],
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as e:
            raise utils.JobError("Sortcheck error: {}".format(e))
        sortcheck_json = json.loads(str(qsv_sortcheck.stdout))
        is_sorted = sortcheck_json["sorted"]
        record_count = int(sortcheck_json["record_count"])
        unsorted_breaks = int(sortcheck_json["unsorted_breaks"])
        dupe_count = int(sortcheck_json["dupe_count"])
        sortcheck_msg = "Sorted: {}; Unsorted breaks: {:,}".format(
            is_sorted, unsorted_breaks
        )
        # dupe count is only relevant if the file is sorted
        if is_sorted and dupe_count > 0:
            sortcheck_msg = sortcheck_msg + " Duplicates: {:,}".format(dupe_count)
        logger.info(sortcheck_msg)

    # --------------- Do we need to dedup? ------------------
    if DEDUP and dupe_count > 0:
        qsv_dedup_csv = os.path.join(temp_dir, "qsv_dedup.csv")
        logger.info("{:.} duplicate rows found. Deduping...".format(dupe_count))
        qsv_extdedup_cmd = [QSV_BIN, "extdedup", tmp, qsv_dedup_csv]

        try:
            qsv_extdedup = subprocess.run(
                qsv_extdedup_cmd,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as e:
            raise utils.JobError("Check for duplicates error: {}".format(e))
        dupe_count = int(str(qsv_extdedup.stderr).strip())
        if dupe_count > 0:
            tmp = qsv_dedup_csv
            logger.warning("{:,} duplicates found and removed.".format(dupe_count))
    elif dupe_count > 0:
        logger.warning("{:,} duplicates found but not deduping...".format(dupe_count))

    # ----------------------- Headers & Safenames ---------------------------
    # get existing header names, so we can use them for data dictionary labels
    # should we need to change the column name to make it "db-safe"
    try:
        qsv_headers = subprocess.run(
            [QSV_BIN, "headers", "--just-names", tmp],
            capture_output=True,
            check=True,
            text=True,
        )
    except subprocess.CalledProcessError as e:
        raise utils.JobError("Cannot scan CSV headers: {}".format(e))
    original_headers = str(qsv_headers.stdout).strip()
    original_header_dict = {
        idx: ele for idx, ele in enumerate(original_headers.splitlines())
    }

    # now, ensure our column/header names identifiers are "safe names"
    # i.e. valid postgres/CKAN Datastore identifiers
    # unsafe_prefix = tk.config.get("ckanext.datapusher_plus.unsafe_prefix", "unsafe_")
    # reserved_colnames = tk.config.get(
    #     "ckanext.datapuhser_plus.reserved_colnames", "_id"
    # )
    qsv_safenames_csv = os.path.join(temp_dir, "qsv_safenames.csv")
    logger.info('Checking for "database-safe" header names...')
    try:
        qsv_safenames = subprocess.run(
            [
                QSV_BIN,
                "safenames",
                tmp,
                "--mode",
                "json",
                "--reserved",
                RESERVED_COLNAMES,
                "--prefix",
                UNSAFE_PREFIX,
            ],
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as e:
        raise utils.JobError("Safenames error: {}".format(e))

    unsafe_json = json.loads(str(qsv_safenames.stdout))
    unsafe_headers = unsafe_json["unsafe_headers"]

    if unsafe_headers:
        logger.info(
            '"{} unsafe" header names found ({}). Sanitizing..."'.format(
                len(unsafe_headers), unsafe_headers
            )
        )
        qsv_safenames = subprocess.run(
            [
                QSV_BIN,
                "safenames",
                tmp,
                "--mode",
                "conditional",
                "--output",
                qsv_safenames_csv,
            ],
            capture_output=True,
            text=True,
        )
        tmp = qsv_safenames_csv
    else:
        logger.info("No unsafe header names found...")

    # ---------------------- Type Inferencing -----------------------
    # at this stage, we have a "clean" CSV ready for Type Inferencing

    # first, index csv for speed - count, stats and slice
    # are all accelerated/multithreaded when an index is present
    try:
        qsv_index_file = tmp + ".idx"
        subprocess.run([QSV_BIN, "index", tmp], check=True)
    except subprocess.CalledProcessError as e:
        raise utils.JobError("Cannot index CSV: {}".format(e))

    # if SORT_AND_DUPE_CHECK = True, we already know the record count
    # so we can skip qsv count.
    if not SORT_AND_DUPE_CHECK:
        # get record count, this is instantaneous with an index
        try:
            qsv_count = subprocess.run(
                [QSV_BIN, "count", tmp], capture_output=True, check=True, text=True
            )
        except subprocess.CalledProcessError as e:
            raise utils.JobError("Cannot count records in CSV: {}".format(e))
        record_count = int(str(qsv_count.stdout).strip())

    # its empty, nothing to do
    if record_count == 0:
        logger.warning("Upload skipped as there are zero records.")
        return

    # log how many records we detected
    unique_qualifier = ""
    if DEDUP:
        unique_qualifier = "unique"
    logger.info("{:,} {} records detected...".format(record_count, unique_qualifier))

    # run qsv stats to get data types and summary statistics
    logger.info("Inferring data types and compiling statistics...")
    headers = []
    types = []
    headers_min = []
    headers_max = []
    headers_cardinality = []
    qsv_stats_csv = os.path.join(temp_dir, "qsv_stats.csv")
    qsv_stats_cmd = [
        QSV_BIN,
        "stats",
        tmp,
        "--infer-dates",
        "--dates-whitelist",
        "all",
        "--stats-jsonl",
        "--output",
        qsv_stats_csv,
    ]

    if PREFER_DMY:
        qsv_stats_cmd.append("--prefer-dmy")

    global AUTO_INDEX_THRESHOLD
    if AUTO_INDEX_THRESHOLD:
        qsv_stats_cmd.append("--cardinality")
    if SUMMARY_STATS_OPTIONS:
        qsv_stats_cmd.append(SUMMARY_STATS_OPTIONS)

    try:
        qsv_stats = subprocess.run(qsv_stats_cmd, check=True)
    except subprocess.CalledProcessError as e:
        raise utils.JobError(
            "Cannot infer data types and compile statistics: {}".format(e)
        )

    # Dictionary to look up stats by field name
    field_stats_lookup = {}

    with open(qsv_stats_csv, mode="r") as inp:
        reader = csv.DictReader(inp)
        for row in reader:
            # Add to lookup dictionary with field name as key
            field_stats_lookup[row["field"]] = {"stats": row}

            fr = {k: v for k, v in row.items()}
            schema_field = fr.get("field", "Unnamed Column")
            if schema_field.startswith("qsv_"):
                break
            headers.append(schema_field)
            types.append(fr.get("type", "String"))
            headers_min.append(fr["min"])
            headers_max.append(fr["max"])
            if AUTO_INDEX_THRESHOLD:
                headers_cardinality.append(int(fr.get("cardinality") or 0))

    # Get the field stats for each field in the headers list
    existing = datastore_resource_exists(resource_id)
    existing_info = None
    if existing:
        existing_info = dict(
            (f["id"], f["info"]) for f in existing.get("fields", []) if "info" in f
        )

    # if this is an existing resource
    # override with types user requested in Data Dictionary
    if existing_info:
        types = [
            {
                "text": "String",
                "numeric": "Float",
                "timestamp": "DateTime",
            }.get(existing_info.get(h, {}).get("type_override"), t)
            for t, h in zip(types, headers)
        ]

    # Delete existing datastore resource before proceeding.
    if existing:
        logger.info(
            'Deleting existing resource "{res_id}" from datastore.'.format(
                res_id=resource_id
            )
        )
        delete_datastore_resource(resource_id)

    # 1st pass of building headers_dict
    # here we map inferred types to postgresql data types
    default_type = "String"
    temp_headers_dicts = [
        dict(
            id=field[0],
            type=TYPE_MAPPING.get(str(field[1]) if field[1] else default_type, "text"),
        )
        for field in zip(headers, types)
    ]

    # 2nd pass header_dicts, checking for smartint types.
    # "smartint" will automatically select the best integer data type based on the
    # min/max values of the column we got from qsv stats.
    # We also set the Data Dictionary Label to original column names in case we made
    # the names "db-safe" as the labels are used by DataTables_view to label columns
    # we also take note of datetime/timestamp fields, so we can normalize them
    # to RFC3339 format, which is Postgres COPY ready
    datetimecols_list = []
    headers_dicts = []
    for idx, header in enumerate(temp_headers_dicts):
        if header["type"] == "smartint":
            if (
                int(headers_max[idx]) <= POSTGRES_INT_MAX
                and int(headers_min[idx]) >= POSTGRES_INT_MIN
            ):
                header_type = "integer"
            elif (
                int(headers_max[idx]) <= POSTGRES_BIGINT_MAX
                and int(headers_min[idx]) >= POSTGRES_BIGINT_MIN
            ):
                header_type = "bigint"
            else:
                header_type = "numeric"
        else:
            header_type = header["type"]
        if header_type == "timestamp":
            datetimecols_list.append(header["id"])
        info_dict = dict(label=original_header_dict.get(idx, "Unnamed Column"))
        headers_dicts.append(dict(id=header["id"], type=header_type, info=info_dict))

    # Maintain data dictionaries from matching column names
    # if data dictionary already exists for this resource as
    # we want to preserve the user's data dictionary curations
    if existing_info:
        for h in headers_dicts:
            if h["id"] in existing_info:
                h["info"] = existing_info[h["id"]]
                # create columns with types user requested
                type_override = existing_info[h["id"]].get("type_override")
                if type_override in list(TYPE_MAPPING.values()):
                    h["type"] = type_override

    logger.info(
        "Determined headers and types: {headers}...".format(headers=headers_dicts)
    )

    # save stats to the datastore by loading qsv_stats_csv directly using COPY
    stats_table = sql.Identifier(resource_id + "-druf-stats")

    try:
        raw_connection_statsfreq = psycopg2.connect(
            tk.config.get("ckan.datastore.write_url")
        )
    except psycopg2.Error as e:
        raise utils.JobError("Could not connect to the Datastore: {}".format(e))
    else:
        cur_statsfreq = raw_connection_statsfreq.cursor()

    # Create stats table based on qsv stats CSV structure
    cur_statsfreq.execute(
        sql.SQL(
            """
            DROP TABLE IF EXISTS {};
            CREATE TABLE {} (
                field TEXT,
                type TEXT,
                is_ascii BOOLEAN,
                sum TEXT,
                min TEXT,
                max TEXT,
                range TEXT,
                sort_order TEXT,
                sortiness FLOAT,
                min_length INTEGER,
                max_length INTEGER,
                sum_length INTEGER,
                avg_length FLOAT,
                stddev_length FLOAT,
                variance_length FLOAT,
                cv_length FLOAT,
                mean FLOAT,
                sem FLOAT,
                geometric_mean FLOAT,
                harmonic_mean FLOAT,
                stddev FLOAT,
                variance FLOAT,
                cv FLOAT,
                nullcount INTEGER,
                max_precision INTEGER,
                sparsity FLOAT,
                cardinality INTEGER,
                uniqueness_ratio FLOAT
            )
        """
        ).format(stats_table, stats_table)
    )

    # Load stats CSV directly using COPY
    copy_sql = sql.SQL("COPY {} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)").format(
        stats_table
    )

    # Copy stats CSV to /tmp directory for debugging purposes
    try:
        debug_stats_path = os.path.join("/tmp", os.path.basename(qsv_stats_csv))
        shutil.copy2(qsv_stats_csv, debug_stats_path)
        logger.info(f"Copied stats CSV to {debug_stats_path} for debugging")
    except Exception as e:
        logger.warning(f"Failed to copy stats CSV to /tmp for debugging: {e}")

    try:
        with open(qsv_stats_csv, "r") as f:
            cur_statsfreq.copy_expert(copy_sql, f)
    except IOError as e:
        raise utils.JobError("Could not open stats CSV file: {}".format(e))
    except psycopg2.Error as e:
        raise utils.JobError("Could not copy stats data to database: {}".format(e))

    raw_connection_statsfreq.commit()

    # ----------------------- Frequency Table ---------------------------
    # compile a frequency table for each column
    qsv_freq_csv = os.path.join(temp_dir, "qsv_freq.csv")
    qsv_freq_cmd = [
        QSV_BIN,
        "frequency",
        "--limit",
        "0",
        tmp,
        "--output",
        qsv_freq_csv,
    ]
    try:
        qsv_freq = subprocess.run(qsv_freq_cmd, check=True)
    except subprocess.CalledProcessError as e:
        raise utils.JobError("Cannot create a frequency table: {}".format(e))

    # save frequency table to the datastore by loading qsv_freq_csv directly using COPY
    # into the datastore using the resource_id + "-freq" table
    # first, create the table
    freq_table = sql.Identifier(resource_id + "-druf-freq")
    cur_statsfreq.execute(
        sql.SQL(
            """
            DROP TABLE IF EXISTS {};
            CREATE TABLE {} (
                field TEXT,
                value TEXT,
                count INTEGER,
                percentage FLOAT,
                PRIMARY KEY (field, value, count)
            )
        """
        ).format(freq_table, freq_table)
    )

    # Copy frequency CSV to /tmp directory for debugging purposes
    try:
        debug_freq_path = os.path.join("/tmp", os.path.basename(qsv_freq_csv))
        shutil.copy2(qsv_freq_csv, debug_freq_path)
        logger.info(f"Copied frequency CSV to {debug_freq_path} for debugging")
    except Exception as e:
        logger.warning(f"Failed to copy frequency CSV to /tmp for debugging: {e}")

    # load the frequency table using COPY
    copy_sql = sql.SQL("COPY {} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)").format(
        freq_table
    )
    try:
        with open(qsv_freq_csv, "r") as f:
            cur_statsfreq.copy_expert(copy_sql, f)
    except IOError as e:
        raise utils.JobError("Could not open frequency CSV file: {}".format(e))
    except psycopg2.Error as e:
        raise utils.JobError("Could not copy frequency data to database: {}".format(e))

    raw_connection_statsfreq.commit()

    cur_statsfreq.close()
    raw_connection_statsfreq.close()

    # ------------------- Do we need to create a Preview?  -----------------------
    # if PREVIEW_ROWS is not zero, create a preview using qsv slice
    # we do the rows_to_copy > preview_rows to check if we don't need to slice
    # the CSV anymore if we only did a partial download of N preview_rows already
    rows_to_copy = record_count
    if PREVIEW_ROWS and record_count > PREVIEW_ROWS:
        if PREVIEW_ROWS > 0:
            # PREVIEW_ROWS is positive, slice from the beginning
            logger.info("Preparing {:,}-row preview...".format(PREVIEW_ROWS))
            qsv_slice_csv = os.path.join(temp_dir, "qsv_slice.csv")
            try:
                qsv_slice = subprocess.run(
                    [
                        QSV_BIN,
                        "slice",
                        "--len",
                        str(PREVIEW_ROWS),
                        tmp,
                        "--output",
                        qsv_slice_csv,
                    ],
                    check=True,
                )
            except subprocess.CalledProcessError as e:
                raise utils.JobError("Cannot create a preview slice: {}".format(e))
            rows_to_copy = PREVIEW_ROWS
            tmp = qsv_slice_csv
        else:
            # PREVIEW_ROWS is negative, slice from the end
            # TODO: do http range request so we don't have to download the whole file
            # to slice from the end
            slice_len = abs(PREVIEW_ROWS)
            logger.info("Preparing {:,}-row preview from the end...".format(slice_len))
            qsv_slice_csv = os.path.join(temp_dir, "qsv_slice.csv")
            try:
                qsv_slice = subprocess.run(
                    [
                        QSV_BIN,
                        "slice",
                        "--start",
                        "-1",
                        "--len",
                        str(slice_len),
                        tmp,
                        "--output",
                        qsv_slice_csv,
                    ],
                    check=True,
                )
            except subprocess.CalledProcessError as e:
                raise utils.JobError(
                    "Cannot create a preview slice from the end: {}".format(e)
                )
            rows_to_copy = slice_len
            tmp = qsv_slice_csv

    # ---------------- Normalize dates to RFC3339 format --------------------
    # if there are any datetime fields, normalize them to RFC3339 format
    # so we can readily insert them as timestamps into postgresql with COPY
    if datetimecols_list:
        qsv_applydp_csv = os.path.join(temp_dir, "qsv_applydp.csv")
        datecols = ",".join(datetimecols_list)

        qsv_applydp_cmd = [
            QSV_BIN,
            "datefmt",
            datecols,
            tmp,
            "--output",
            qsv_applydp_csv,
        ]
        if PREFER_DMY:
            qsv_applydp_cmd.append("--prefer-dmy")
        logger.info(
            'Formatting dates "{}" to ISO 8601/RFC 3339 format with PREFER_DMY: {}...'.format(
                datecols, PREFER_DMY
            )
        )
        try:
            qsv_applydp = subprocess.run(qsv_applydp_cmd, check=True)
        except subprocess.CalledProcessError as e:
            raise utils.JobError("Applydp error: {}".format(e))
        tmp = qsv_applydp_csv

    # -------------------- QSV ANALYSIS DONE --------------------
    analysis_elapsed = time.perf_counter() - analysis_start
    logger.info(
        "ANALYSIS DONE! Analyzed and prepped in {:,.2f} seconds.".format(
            analysis_elapsed
        )
    )

    # ----------------------------- PII Screening ------------------------------
    # we scan for Personally Identifiable Information (PII) using qsv's powerful
    # searchset command which can SIMULTANEOUSLY compare several regexes per
    # field in one pass
    piiscreening_start = 0
    piiscreening_elapsed = 0
    if PII_SCREENING:
        piiscreening_start = time.perf_counter()
        pii_found_abort = tk.config.get("ckanext.datastore_plus.pii_found_abort")

        # DP+ comes with default regex patterns for PII (SSN, credit cards,
        # email, bank account numbers, & phone number). The DP+ admin can
        # use a custom set of regex patterns by pointing to a resource with
        # a text file, with each line having a regex pattern, and an optional
        # label comment prefixed with "#" (e.g. #SSN, #Email, #Visa, etc.)
        if PII_REGEX_RESOURCE_ID:
            pii_regex_resource_exist = datastore_resource_exists(PII_REGEX_RESOURCE_ID)
            if pii_regex_resource_exist:
                pii_resource = get_resource(PII_REGEX_RESOURCE_ID)
                pii_regex_url = pii_resource["url"]

                r = requests.get(pii_regex_url)
                pii_regex_file = pii_regex_url.split("/")[-1]

                p = Path(__file__).with_name("user-pii-regexes.txt")
                with p.open("wb") as f:
                    f.write(r.content)
        else:
            pii_regex_file = "default-pii-regexes.txt"
            p = Path(__file__).with_name(pii_regex_file)

        pii_found = False
        pii_regex_fname = p.absolute()

        if PII_QUICK_SCREEN:
            logger.info("Quickly scanning for PII using {}...".format(pii_regex_file))
            try:
                qsv_searchset = subprocess.run(
                    [
                        QSV_BIN,
                        "searchset",
                        "--ignore-case",
                        "--quick",
                        pii_regex_fname,
                        tmp,
                    ],
                    capture_output=True,
                    text=True,
                )
            except subprocess.CalledProcessError as e:
                raise utils.JobError("Cannot quickly search CSV for PII: {}".format(e))
            pii_candidate_row = str(qsv_searchset.stderr)
            if pii_candidate_row:
                pii_found = True

        else:
            logger.info("Scanning for PII using {}...".format(pii_regex_file))
            qsv_searchset_csv = os.path.join(temp_dir, "qsv_searchset.csv")
            try:
                qsv_searchset = subprocess.run(
                    [
                        QSV_BIN,
                        "searchset",
                        "--ignore-case",
                        "--flag",
                        "PII_info",
                        "--flag-matches-only",
                        "--json",
                        pii_regex_file,
                        tmp,
                        "--output",
                        qsv_searchset_csv,
                    ],
                    capture_output=True,
                    text=True,
                )
            except subprocess.CalledProcessError as e:
                raise utils.JobError("Cannot search CSV for PII: {}".format(e))
            pii_json = json.loads(str(qsv_searchset.stderr))
            pii_total_matches = int(pii_json["total_matches"])
            pii_rows_with_matches = int(pii_json["rows_with_matches"])
            if pii_total_matches > 0:
                pii_found = True

        if pii_found and pii_found_abort and not PII_SHOW_CANDIDATES:
            logger.error("PII Candidate/s Found!")
            if PII_QUICK_SCREEN:
                raise utils.JobError(
                    "PII CANDIDATE FOUND on row {}! Job aborted.".format(
                        pii_candidate_row.rstrip()
                    )
                )
            else:
                raise utils.JobError(
                    "PII CANDIDATE/S FOUND! Job aborted. Found {} PII candidate/s in {} row/s.".format(
                        pii_total_matches, pii_rows_with_matches
                    )
                )
        elif pii_found and PII_SHOW_CANDIDATES and not PII_QUICK_SCREEN:
            # TODO: Create PII Candidates resource and set package to private if its not private
            # ------------ Create PII Preview Resource ------------------
            logger.warning(
                "PII CANDIDATE/S FOUND! Found {} PII candidate/s in {} row/s. Creating PII preview...".format(
                    pii_total_matches, pii_rows_with_matches
                )
            )
            pii_resource_id = resource_id + "-pii"

            try:
                raw_connection_pii = psycopg2.connect(
                    tk.config.get("ckan.datastore.write_url")
                )
            except psycopg2.Error as e:
                raise utils.JobError("Could not connect to the Datastore: {}".format(e))
            else:
                cur_pii = raw_connection_pii.cursor()

            # check if the pii already exist
            existing_pii = datastore_resource_exists(pii_resource_id)

            # Delete existing pii preview before proceeding.
            if existing_pii:
                logger.info(
                    'Deleting existing PII preview "{}".'.format(pii_resource_id)
                )

                cur_pii.execute(
                    "SELECT alias_of FROM _table_metadata where name like %s group by alias_of;",
                    (pii_resource_id + "%",),
                )
                pii_alias_result = cur_pii.fetchone()
                if pii_alias_result:
                    existing_pii_alias_of = pii_alias_result[0]

                    delete_datastore_resource(existing_pii_alias_of)
                    delete_resource(existing_pii_alias_of)

            pii_alias = [pii_resource_id]

            # run stats on pii preview CSV to get header names and infer data types
            # we don't need summary statistics, so use the --typesonly option
            try:
                qsv_pii_stats = subprocess.run(
                    [
                        QSV_BIN,
                        "stats",
                        "--typesonly",
                        qsv_searchset_csv,
                    ],
                    capture_output=True,
                    check=True,
                    text=True,
                )
            except subprocess.CalledProcessError as e:
                raise utils.JobError(
                    "Cannot run stats on PII preview CSV: {}".format(e)
                )

            pii_stats = str(qsv_pii_stats.stdout).strip()
            pii_stats_dict = [
                dict(id=ele.split(",")[0], type=type_mapping[ele.split(",")[1]])
                for idx, ele in enumerate(pii_stats.splitlines()[1:], 1)
            ]

            pii_resource = {
                "package_id": resource["package_id"],
                "name": resource["name"] + " - PII",
                "format": "CSV",
                "mimetype": "text/csv",
            }
            pii_response = send_resource_to_datastore(
                pii_resource,
                resource_id=None,
                headers=pii_stats_dict,
                records=None,
                aliases=pii_alias,
                calculate_record_count=False,
            )

            new_pii_resource_id = pii_response["result"]["resource_id"]

            # now COPY the PII preview to the datastore
            logger.info(
                'ADDING PII PREVIEW in "{}" with alias "{}"...'.format(
                    new_pii_resource_id,
                    pii_alias,
                )
            )
            col_names_list = [h["id"] for h in pii_stats_dict]
            column_names = sql.SQL(",").join(sql.Identifier(c) for c in col_names_list)

            copy_sql = sql.SQL(
                "COPY {} ({}) FROM STDIN "
                "WITH (FORMAT CSV, "
                "HEADER 1, ENCODING 'UTF8');"
            ).format(
                sql.Identifier(new_pii_resource_id),
                column_names,
            )

            with open(qsv_searchset_csv, "rb") as f:
                try:
                    cur_pii.copy_expert(copy_sql, f)
                except psycopg2.Error as e:
                    raise utils.JobError("Postgres COPY failed: {}".format(e))
                else:
                    pii_copied_count = cur_pii.rowcount

            raw_connection_pii.commit()
            cur_pii.close()

            pii_resource["id"] = new_pii_resource_id
            pii_resource["pii_preview"] = True
            pii_resource["pii_of_resource"] = resource_id
            pii_resource["total_record_count"] = pii_rows_with_matches
            update_resource(pii_resource)

            pii_msg = (
                "{} PII candidate/s in {} row/s are available at {} for review".format(
                    pii_total_matches,
                    pii_copied_count,
                    resource["url"][: resource["url"].find("/resource/")]
                    + "/resource/"
                    + new_pii_resource_id,
                )
            )
            if pii_found_abort:
                raise utils.JobError(pii_msg)
            else:
                logger.warning(pii_msg)
                logger.warning(
                    "PII CANDIDATE/S FOUND but proceeding with job per Datapusher+ configuration."
                )
        elif not pii_found:
            logger.info("PII Scan complete. No PII candidate/s found.")

        piiscreening_elapsed = time.perf_counter() - piiscreening_start

    # delete the qsv index file manually
    # as it was created by qsv index, and not by tempfile
    os.remove(qsv_index_file)

    # at this stage, the resource is ready for COPYing to the Datastore

    if dry_run:
        logger.warning("Dry run only. Returning without copying to the Datastore...")
        return headers_dicts

    # ============================================================
    # COPY to Datastore
    # ============================================================
    copy_start = time.perf_counter()

    if PREVIEW_ROWS:
        logger.info("COPYING {:,}-row preview to Datastore...".format(rows_to_copy))
    else:
        logger.info("COPYING {:,} rows to Datastore...".format(rows_to_copy))

    # first, let's create an empty datastore table w/ guessed types
    send_resource_to_datastore(
        resource=None,
        resource_id=resource["id"],
        headers=headers_dicts,
        records=None,
        aliases=None,
        calculate_record_count=False,
    )

    copied_count = 0
    try:
        raw_connection = psycopg2.connect(tk.config.get("ckan.datastore.write_url"))
    except psycopg2.Error as e:
        raise utils.JobError("Could not connect to the Datastore: {}".format(e))
    else:
        cur = raw_connection.cursor()
        """
        truncate table to use copy freeze option and further increase
        performance as there is no need for WAL logs to be maintained
        https://www.postgresql.org/docs/current/populate.html#POPULATE-COPY-FROM
        """
        try:
            cur.execute(
                sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(resource_id))
            )

        except psycopg2.Error as e:
            logger.warning("Could not TRUNCATE: {}".format(e))

        col_names_list = [h["id"] for h in headers_dicts]
        column_names = sql.SQL(",").join(sql.Identifier(c) for c in col_names_list)
        copy_sql = sql.SQL(
            "COPY {} ({}) FROM STDIN "
            "WITH (FORMAT CSV, FREEZE 1, "
            "HEADER 1, ENCODING 'UTF8');"
        ).format(
            sql.Identifier(resource_id),
            column_names,
        )
        # specify a 1MB buffer size for COPY read from disk
        with open(tmp, "rb", COPY_READBUFFER_SIZE) as f:
            try:
                cur.copy_expert(copy_sql, f, size=COPY_READBUFFER_SIZE)
            except psycopg2.Error as e:
                raise utils.JobError("Postgres COPY failed: {}".format(e))
            else:
                copied_count = cur.rowcount

        raw_connection.commit()
        # this is needed to issue a VACUUM ANALYZE
        raw_connection.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
        )
        analyze_cur = raw_connection.cursor()
        analyze_cur.execute(
            sql.SQL("VACUUM ANALYZE {}").format(sql.Identifier(resource_id))
        )
        analyze_cur.close()

    copy_elapsed = time.perf_counter() - copy_start
    logger.info(
        '...copying done. Copied {n} rows to "{res_id}" in {copy_elapsed} seconds.'.format(
            n="{:,}".format(copied_count),
            res_id=resource_id,
            copy_elapsed="{:,.2f}".format(copy_elapsed),
        )
    )

    # ============================================================
    # FETCH SCHEMING YAML
    # ============================================================
    package_id = resource["package_id"]
    scheming_yaml, package = get_scheming_yaml(package_id)
    logger.info(f"package: {package}")
    # Check if there are any fields with suggest_jinja2 in the scheming_yaml
    # first, get the dataset package fields
    suggest_package_fields = [
        field
        for field in scheming_yaml["dataset_fields"]
        if field.get("suggest_jinja2")
    ]
    # then, get the resource fields
    suggest_resource_fields = [
        field
        for field in scheming_yaml["resource_fields"]
        if field.get("suggest_jinja2")
    ]

    jinja2_dict = {}
    if suggest_package_fields:
        logger.info(
            "Found {} package field/s with suggest_jinja2 in the scheming_yaml".format(
                len(suggest_package_fields)
            )
        )
        need_stats_in_package = False
        # For each field with suggest_jinja2, get the associated jinja2 template
        for schema_field in suggest_package_fields:
            jinja2_template = schema_field["suggest_jinja2"]
            if jinja2_template:
                # Check if the jinja2 template contains .stats. or ['stats'] or ["stats"]
                if not need_stats_in_package and (
                    jinja2_template.find(".stats.") != -1
                    or jinja2_template.find("['stats']") != -1
                    or jinja2_template.find('["stats"]') != -1
                ):
                    need_stats_in_package = True
            package_field_name = schema_field["field_name"]
            jinja2_dict[package_field_name] = jinja2_template
            logger.debug(
                'Jinja2 template for packagefield "{}": {}'.format(package_field_name, jinja2_template)
            )

    logger.debug("Jinja2 dict: {}".format(jinja2_dict))
    # Create a dictionary with both package and field_stats_lookup
    # context = {"package": package, "field_stats_lookup": field_stats_lookup}
    context = {"package": package, "resource": field_stats_lookup}

    # Add the jinja2 templates to the context
    context.update(jinja2_dict)
    logger.info(f"Context: {context}")
    jinja2_env = Environment(loader=DictLoader(context))
    # jinja2_env = Environment(loader=DictLoader({"package": package, **jinja2_dict}))
    jinja2_env.filters['truncate_with_ellipsis'] = dph.truncate_with_ellipsis
    jinja2_env.globals['spatial_extent_wkt'] = dph.spatial_extent_wkt

    revise_update_content = {"package": {}}
    if need_stats_in_package:
        logger.debug("Need stats in jinja2")

        for schema_field in suggest_package_fields:
            package_field_name = schema_field["field_name"]

            # evaluate the jinja2 snippet
            try:
                formula = jinja2_env.get_template(package_field_name)
                # No need to pass field_stats_lookup here as it's already in the context
                rendered_formula = formula.render(**context)
                logger.debug(
                    'Evaluated jinja2 formula for field "{}": {}'.format(
                        package_field_name, rendered_formula
                    )
                )

                # package["dpp_suggestion.package.{}".format(package_field_name)] = rendered_formula
                # revise_update["dpp_suggestion.package.{}".format(package_field_name)] = rendered_formula
                revise_update_content["package"][package_field_name] = rendered_formula
                # resource[package_field_name] = rendered_formula
                # package[package_field_name] = rendered_formula
            except Exception as e:
                logger.error(
                    'Error evaluating jinja2 formula for field "{}": {}'.format(
                        package_field_name, str(e)
                    )
                )

        # patch the package
        # logger.info("Patching package...")
        # logger.info(f"Package before patching: {package}")
        # try:
        #     patched_package = patch_package(package)
        #     logger.info(f"Package after patching: {patched_package}")
        # except Exception as e:
        #     logger.error("Error patching package: {}".format(str(e)))

        # revise the package
        logger.info("Revising package: {}".format(package_id))
        logger.info(f"Package before revising: {package}")
        logger.info(f"Revise update content: {revise_update_content}")
        # revise_update = {"dpp_suggestion": json.dumps(revise_update_content)}
        # logger.info(f"Revise update: {revise_update}")
        try:
            # revised_package = revise_package(package_id, match={"dpp_suggestion": ""}, update=revise_update)
            revised_package = revise_package(
                package_id,
                update={
                    "dpp_suggestion": revise_update_content  # Store the entire object directly
                }
            )
            logger.info(f"Package after revising: {revised_package}")
        except Exception as e:
            logger.error("Error revising package: {}".format(str(e)))
    else:
        logger.info("No need for stats in jinja2")

    # ============================================================
    # UPDATE METADATA
    # ============================================================
    metadata_start = time.perf_counter()
    logger.info("UPDATING RESOURCE METADATA...")

    # --------------------- AUTO-ALIASING ------------------------
    # aliases are human-readable, and make it easier to use than resource id hash
    # when using the Datastore API and in SQL queries
    alias = None
    if AUTO_ALIAS:
        logger.info(
            "AUTO-ALIASING. Auto-alias-unique: {} ...".format(AUTO_ALIAS_UNIQUE)
        )
        # get package info, so we can construct the alias
        package = get_package(resource["package_id"])

        resource_name = resource.get("name")
        package_name = package.get("name")
        owner_org = package.get("organization")
        owner_org_name = ""
        if owner_org:
            owner_org_name = owner_org.get("name")
        if resource_name and package_name and owner_org_name:
            # we limit it to 55, so we still have space for sequence & stats suffix
            # postgres max identifier length is 63
            alias = f"{resource_name}-{package_name}-{owner_org_name}"[:55]
            # if AUTO_ALIAS_UNIQUE is true, check if the alias already exist, if it does
            # add a sequence suffix so the new alias can be created
            cur.execute(
                "SELECT COUNT(*), alias_of FROM _table_metadata where name like %s group by alias_of",
                (alias + "%",),
            )
            alias_query_result = cur.fetchone()
            if alias_query_result:
                alias_count = alias_query_result[0]
                existing_alias_of = alias_query_result[1]
            else:
                alias_count = 0
                existing_alias_of = ""
            if AUTO_ALIAS_UNIQUE and alias_count > 1:
                alias_sequence = alias_count + 1
                while True:
                    # we do this, so we're certain the new alias does not exist
                    # just in case they deleted an older alias with a lower sequence #
                    alias = f"{alias}-{alias_sequence:03}"
                    cur.execute(
                        "SELECT COUNT(*), alias_of FROM _table_metadata where name like %s group by alias_of;",
                        (alias + "%",),
                    )
                    alias_exists = cur.fetchone()[0]
                    if not alias_exists:
                        break
                    alias_sequence += 1
            elif alias_count == 1:
                logger.warning(
                    'Dropping existing alias "{}" for resource "{}"...'.format(
                        alias, existing_alias_of
                    )
                )
                try:
                    cur.execute(
                        sql.SQL("DROP VIEW IF EXISTS {}").format(sql.Identifier(alias))
                    )
                except psycopg2.Error as e:
                    logger.warning("Could not drop alias/view: {}".format(e))

        else:
            logger.warning(
                "Cannot create alias: {}-{}-{}".format(
                    resource_name, package_name, owner_org
                )
            )
            alias = None

    # -------- should we ADD_SUMMARY_STATS_RESOURCE? -------------
    # by default, we only add summary stats if we're not doing a partial download
    # (otherwise, you're summarizing the preview, not the whole file)
    # That is, unless SUMMARY_STATS_WITH_PREVIEW is set to true
    if ADD_SUMMARY_STATS_RESOURCE or SUMMARY_STATS_WITH_PREVIEW:
        stats_resource_id = resource_id + "-stats"

        # check if the stats already exist
        existing_stats = datastore_resource_exists(stats_resource_id)
        # Delete existing summary-stats before proceeding.
        if existing_stats:
            logger.info(
                'Deleting existing summary stats "{}".'.format(stats_resource_id)
            )

            cur.execute(
                "SELECT alias_of FROM _table_metadata where name like %s group by alias_of;",
                (stats_resource_id + "%",),
            )
            stats_alias_result = cur.fetchone()
            if stats_alias_result:
                existing_stats_alias_of = stats_alias_result[0]

                delete_datastore_resource(existing_stats_alias_of)
                delete_resource(existing_stats_alias_of)

        stats_aliases = [stats_resource_id]
        if AUTO_ALIAS:
            auto_alias_stats_id = alias + "-stats"
            stats_aliases.append(auto_alias_stats_id)

            # check if the summary-stats alias already exist. We need to do this as summary-stats resources
            # may end up having the same alias if AUTO_ALIAS_UNIQUE is False, so we need to drop the
            # existing summary stats-alias.
            existing_alias_stats = datastore_resource_exists(auto_alias_stats_id)
            # Delete existing auto-aliased summary-stats before proceeding.
            if existing_alias_stats:
                logger.info(
                    'Deleting existing alias summary stats "{}".'.format(
                        auto_alias_stats_id
                    )
                )

                cur.execute(
                    "SELECT alias_of FROM _table_metadata where name like %s group by alias_of;",
                    (auto_alias_stats_id + "%",),
                )
                result = cur.fetchone()
                if result:
                    existing_stats_alias_of = result[0]

                    delete_datastore_resource(existing_stats_alias_of)
                    delete_resource(existing_stats_alias_of)

        # run stats on stats CSV to get header names and infer data types
        # we don't need summary statistics, so use the --typesonly option
        try:
            qsv_stats_stats = subprocess.run(
                [
                    QSV_BIN,
                    "stats",
                    "--typesonly",
                    qsv_stats_csv,
                ],
                capture_output=True,
                check=True,
                text=True,
            )
        except subprocess.CalledProcessError as e:
            raise utils.JobError("Cannot run stats on CSV stats: {}".format(e))

        stats_stats = str(qsv_stats_stats.stdout).strip()
        stats_stats_dict = [
            dict(id=ele.split(",")[0], type=TYPE_MAPPING[ele.split(",")[1]])
            for idx, ele in enumerate(stats_stats.splitlines()[1:], 1)
        ]

        logger.info(f"stats_stats_dict: {stats_stats_dict}")

        resource_name = resource.get("name")
        stats_resource = {
            "package_id": resource["package_id"],
            "name": resource_name + " - Summary Statistics",
            "format": "CSV",
            "mimetype": "text/csv",
        }
        stats_response = send_resource_to_datastore(
            stats_resource,
            resource_id=None,
            headers=stats_stats_dict,
            records=None,
            aliases=stats_aliases,
            calculate_record_count=False,
        )

        logger.info(f"stats_response: {stats_response}")

        new_stats_resource_id = stats_response["result"]["resource_id"]

        # now COPY the stats to the datastore
        col_names_list = [h["id"] for h in stats_stats_dict]
        logger.info(
            'ADDING SUMMARY STATISTICS {} in "{}" with alias/es "{}"...'.format(
                col_names_list,
                new_stats_resource_id,
                stats_aliases,
            )
        )

        column_names = sql.SQL(",").join(sql.Identifier(c) for c in col_names_list)

        copy_sql = sql.SQL(
            "COPY {} ({}) FROM STDIN "
            "WITH (FORMAT CSV, "
            "HEADER 1, ENCODING 'UTF8');"
        ).format(
            sql.Identifier(new_stats_resource_id),
            column_names,
        )

        with open(qsv_stats_csv, "rb") as f:
            try:
                cur.copy_expert(copy_sql, f)
            except psycopg2.Error as e:
                raise utils.JobError("Postgres COPY failed: {}".format(e))

        stats_resource["id"] = new_stats_resource_id
        stats_resource["summary_statistics"] = True
        stats_resource["summary_of_resource"] = resource_id
        update_resource(stats_resource)

    cur.close()
    raw_connection.commit()

    resource["datastore_active"] = True
    resource["total_record_count"] = record_count
    if PREVIEW_ROWS < record_count or (PREVIEW_ROWS > 0):
        resource["preview"] = True
        resource["preview_rows"] = copied_count
    else:
        resource["preview"] = False
        resource["preview_rows"] = None
        resource["partial_download"] = False
    update_resource(resource)

    # tell CKAN to calculate_record_count and set alias if set
    send_resource_to_datastore(
        resource=None,
        resource_id=resource["id"],
        headers=headers_dicts,
        records=None,
        aliases=alias,
        calculate_record_count=True,
    )

    if alias:
        logger.info('Created alias "{}" for "{}"...'.format(alias, resource_id))

    metadata_elapsed = time.perf_counter() - metadata_start
    logger.info(
        "METADATA UPDATES DONE! Resource metadata updated in {:.2f} seconds.".format(
            metadata_elapsed
        )
    )

    # =================================================================================================
    # INDEXING
    # =================================================================================================
    # if AUTO_INDEX_THRESHOLD > 0 or AUTO_INDEX_DATES is true
    # create indices automatically based on summary statistics
    # For columns w/ cardinality = record_count, it's all unique values, create a unique index
    # If AUTO_INDEX_DATES is true, index all date columns
    # if a column's cardinality <= AUTO_INDEX_THRESHOLD, create an index for that column
    # auto_index_dates = tk.config.get("ckanext.datastore_plus.auto_index_dates")
    # auto_unique_index = tk.config.get("ckanext.datastore_plus.auto_unique_index")
    if (
        AUTO_INDEX_THRESHOLD
        or (AUTO_INDEX_DATES and datetimecols_list)
        or AUTO_UNIQUE_INDEX
    ):
        index_start = time.perf_counter()
        logger.info(
            "AUTO-INDEXING. Auto-index threshold: {:,} unique value/s. Auto-unique index: {} Auto-index dates: {} ...".format(
                AUTO_INDEX_THRESHOLD, AUTO_UNIQUE_INDEX, AUTO_INDEX_DATES
            )
        )
        index_cur = raw_connection.cursor()

        # if auto_index_threshold == -1
        # we index all the columns
        if AUTO_INDEX_THRESHOLD == -1:
            AUTO_INDEX_THRESHOLD = record_count

        index_count = 0
        for idx, cardinality in enumerate(headers_cardinality):
            curr_col = headers[idx]
            if AUTO_INDEX_THRESHOLD > 0 or AUTO_INDEX_DATES or AUTO_UNIQUE_INDEX:
                if cardinality == record_count and AUTO_UNIQUE_INDEX:
                    # all the values are unique for this column, create a unique index
                    if PREVIEW_ROWS > 0:
                        unique_value_count = min(PREVIEW_ROWS, cardinality)
                    else:
                        unique_value_count = cardinality
                    logger.info(
                        'Creating UNIQUE index on "{}" for {:,} unique values...'.format(
                            curr_col, unique_value_count
                        )
                    )
                    try:
                        index_cur.execute(
                            sql.SQL("CREATE UNIQUE INDEX ON {} ({})").format(
                                sql.Identifier(resource_id),
                                sql.Identifier(curr_col),
                            )
                        )
                    except psycopg2.Error as e:
                        logger.warning(
                            'Could not CREATE UNIQUE INDEX on "{}": {}'.format(
                                curr_col, e
                            )
                        )
                    index_count += 1
                elif cardinality <= AUTO_INDEX_THRESHOLD or (
                    AUTO_INDEX_DATES and (curr_col in datetimecols_list)
                ):
                    # cardinality <= auto_index_threshold or its a date and auto_index_date is true
                    # create an index
                    if curr_col in datetimecols_list:
                        logger.info(
                            'Creating index on "{}" date column for {:,} unique value/s...'.format(
                                curr_col, cardinality
                            )
                        )
                    else:
                        logger.info(
                            'Creating index on "{}" for {:,} unique value/s...'.format(
                                curr_col, cardinality
                            )
                        )
                    try:
                        index_cur.execute(
                            sql.SQL("CREATE INDEX ON {} ({})").format(
                                sql.Identifier(resource_id),
                                sql.Identifier(curr_col),
                            )
                        )
                    except psycopg2.Error as e:
                        logger.warning(
                            'Could not CREATE INDEX on "{}": {}'.format(curr_col, e)
                        )
                    index_count += 1

        index_cur.close()
        raw_connection.commit()

        logger.info("Vacuum Analyzing table to optimize indices...")

        # this is needed to issue a VACUUM ANALYZE
        raw_connection.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
        )
        analyze_cur = raw_connection.cursor()
        analyze_cur.execute(
            sql.SQL("VACUUM ANALYZE {}").format(sql.Identifier(resource_id))
        )
        analyze_cur.close()

        index_elapsed = time.perf_counter() - index_start
        logger.info(
            '...indexing/vacuum analysis done. Indexed {n} column/s in "{res_id}" in {index_elapsed} seconds.'.format(
                n="{:,}".format(index_count),
                res_id=resource_id,
                index_elapsed="{:,.2f}".format(index_elapsed),
            )
        )

    raw_connection.close()
    total_elapsed = time.perf_counter() - timer_start
    newline_var = "\n"
    end_msg = f"""
    DATAPUSHER+ JOB DONE!
      Download: {fetch_elapsed:,.2f}
      Analysis: {analysis_elapsed:,.2f}{(newline_var + f"  PII Screening: {piiscreening_elapsed:,.2f}") if piiscreening_elapsed > 0 else ""}
      COPYing: {copy_elapsed:,.2f}
      Metadata updates: {metadata_elapsed:,.2f}
      Indexing: {index_elapsed:,.2f}
    TOTAL ELAPSED TIME: {total_elapsed:,.2f}
    """
    logger.info(end_msg)
