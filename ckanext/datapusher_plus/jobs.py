# -*- coding: utf-8 -*-
# flake8: noqa: E501

# Standard library imports
import csv
import hashlib
import locale
import mimetypes
import os
import shutil
import subprocess
import tempfile
import time
from urllib.parse import urlsplit
from urllib.parse import urlparse
import logging
import uuid

# Third-party imports
import psycopg2
from datasize import DataSize
from dateutil.parser import parse as parsedate
import json
import requests
import locale
import logging
import hashlib
import time
import tempfile
import subprocess
import csv
import os
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

import ckanext.datapusher_plus.utils as utils
import ckanext.datapusher_plus.helpers as dph
import ckanext.datapusher_plus.jinja2_helpers as j2h
from ckanext.datapusher_plus.job_exceptions import HTTPError
import ckanext.datapusher_plus.config as conf
import ckanext.datapusher_plus.spatial_helpers as sh
import ckanext.datapusher_plus.datastore_utils as dsu
from ckanext.datapusher_plus.logging_utils import trace, TRACE

if locale.getdefaultlocale()[0]:
    lang, encoding = locale.getdefaultlocale()
    locale.setlocale(locale.LC_ALL, locale=(lang, encoding))
else:
    locale.setlocale(locale.LC_ALL, "")


def validate_input(input):
    # Especially validate metadata which is provided by the user
    if "metadata" not in input:
        raise utils.JobError("Metadata missing")

    data = input["metadata"]

    if "resource_id" not in data:
        raise utils.JobError("No id provided.")


def callback_datapusher_hook(result_url, job_dict):
    api_token = utils.get_dp_plus_user_apitoken()
    headers = {"Content-Type": "application/json", "Authorization": api_token}

    try:
        result = requests.post(
            result_url,
            data=json.dumps(job_dict, cls=utils.DatetimeJsonEncoder),
            verify=conf.SSL_VERIFY,
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
    log_level = getattr(logging, conf.UPLOAD_LOG_LEVEL.upper())

    # set the log level to the config upload_log_level
    logger.setLevel(logging.INFO)
    logger.info(f"Setting log level to {logging.getLevelName(int(log_level))}")
    logger.setLevel(log_level)

    # check if conf.QSV_BIN and conf.FILE_BIN exists
    if not Path(conf.QSV_BIN).is_file():
        raise utils.JobError("{} not found.".format(conf.QSV_BIN))

    if not conf.FILE_BIN.is_file():
        raise utils.JobError("{} not found.".format(conf.FILE_BIN))

    # make sure qsv binary variant is up-to-date
    try:
        qsv_version = subprocess.run(
            [conf.QSV_BIN, "--version"],
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as e:
        raise utils.JobError("qsv version check error: {}".format(e))
    qsv_version_info = str(qsv_version.stdout)
    if not qsv_version_info:
        # Sample response
        # qsvdp 4.0.0-mimalloc-geocode;Luau 0.663;self_update...
        raise utils.JobError(
            f"We expect qsv version info to be returned. Command: {conf.QSV_BIN} --version. Response: {qsv_version_info}"
        )
    qsv_semver = qsv_version_info[
        qsv_version_info.find(" ") : qsv_version_info.find("-")
    ].lstrip()

    logger.info("qsv version found: {}".format(qsv_semver))
    try:
        if semver.compare(qsv_semver, conf.MINIMUM_QSV_VERSION) < 0:
            raise utils.JobError(
                "At least qsv version {} required. Found {}. You can get the latest release at https://github.com/jqnatividad/qsv/releases/latest".format(
                    conf.MINIMUM_QSV_VERSION, qsv_version_info
                )
            )
    except ValueError as e:
        raise utils.JobError("Cannot parse qsv version info: {}".format(e))

    validate_input(input)

    data = input["metadata"]

    ckan_url = data["ckan_url"]
    resource_id = data["resource_id"]
    try:
        resource = dsu.get_resource(resource_id)
    except utils.JobError:
        # try again in 5 seconds just incase CKAN is slow at adding resource
        time.sleep(5)
        resource = dsu.get_resource(resource_id)

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
            logger.info("Rewritten resource url to: {0}".format(resource_url))

    try:
        kwargs = {
            "headers": headers,
            "timeout": conf.TIMEOUT,
            "verify": conf.SSL_VERIFY,
            "stream": True,
        }
        if conf.USE_PROXY:
            kwargs["proxies"] = {
                "http": conf.DOWNLOAD_PROXY,
                "https": conf.DOWNLOAD_PROXY,
            }
        with requests.get(resource_url, **kwargs) as response:
            response.raise_for_status()

            cl = response.headers.get("content-length")
            max_content_length = conf.MAX_CONTENT_LENGTH
            ct = response.headers.get("content-type")

            try:
                if cl and int(cl) > max_content_length and conf.PREVIEW_ROWS > 0:
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
                for chunk in response.iter_content(conf.CHUNK_SIZE):
                    length += len(chunk)
                    if length > max_content_length and not conf.PREVIEW_ROWS:
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
        and not conf.IGNORE_FILE_HASH
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

    # Check if the file is a zip file
    unzipped_format = ""
    if resource_format.upper() == "ZIP":
        logger.info("Processing ZIP file...")

        file_count, extracted_path, unzipped_format = dph.extract_zip_or_metadata(
            tmp, temp_dir, logger
        )
        if not file_count:
            logger.error("ZIP file invalid or no files found in ZIP file.")
            return
        logger.info(
            "More than one file in the ZIP file ({} files), saving metadata...".format(
                file_count
            )
            if file_count > 1
            else "Extracted {} file: {}".format(unzipped_format, extracted_path)
        )
        tmp = extracted_path

    # ===================================================================================
    # ANALYZE WITH QSV
    # ===================================================================================
    # Start Analysis using qsv instead of messytables, as
    # 1) its type inferences are bullet-proof not guesses as it scans the entire file,
    # 2) its super-fast, and
    # 3) it has addl data-wrangling capabilities we use in DP+ (e.g. stats, dedup, etc.)
    analysis_start = time.perf_counter()
    logger.info("ANALYZING WITH QSV..")

    # flag to check if the file is a spatial format
    spatial_format_flag = False
    simplification_failed_flag = False
    # ----------------- is it a spreadsheet? ---------------
    # check content type or file extension if its a spreadsheet
    spreadsheet_extensions = ["XLS", "XLSX", "ODS", "XLSM", "XLSB"]
    file_format = resource.get("format").upper()
    if (
        file_format in spreadsheet_extensions
        or unzipped_format in spreadsheet_extensions
    ):
        # if so, export spreadsheet as a CSV file
        default_excel_sheet = conf.DEFAULT_EXCEL_SHEET
        file_format = unzipped_format if unzipped_format != "" else file_format
        logger.info(
            "Converting {} sheet {} to CSV...".format(file_format, default_excel_sheet)
        )
        # first, we need a temporary spreadsheet filename with the right file extension
        # we only need the filename though, that's why we remove it
        # and create a hardlink to the file we got from CKAN
        qsv_spreadsheet = os.path.join(temp_dir, "qsv_spreadsheet." + file_format)
        os.link(tmp, qsv_spreadsheet)

        # run `qsv excel` and export it to a CSV
        # use --trim option to trim column names and the data
        qsv_excel_csv = os.path.join(temp_dir, "qsv_excel.csv")
        try:
            qsv_excel = subprocess.run(
                [
                    conf.QSV_BIN,
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
            file_metadata = subprocess.run(
                [conf.FILE_BIN, qsv_spreadsheet],
                check=True,
                capture_output=True,
                text=True,
            )

            logger.warning(
                "Is the file encrypted or is not a spreadsheet?\nFILE ATTRIBUTES: {}".format(
                    file_metadata.stdout
                )
            )

            return
        excel_export_msg = qsv_excel.stderr
        logger.info("{}...".format(excel_export_msg))
        tmp = qsv_excel_csv
    elif resource_format.upper() in ["SHP", "QGIS", "GEOJSON"]:
        logger.info("SHAPEFILE or GEOJSON file detected...")

        qsv_spatial_file = os.path.join(
            temp_dir, "qsv_spatial_" + str(uuid.uuid4()) + "." + resource_format
        )
        os.link(tmp, qsv_spatial_file)
        qsv_spatial_csv = os.path.join(temp_dir, "qsv_spatial.csv")

        if conf.AUTO_SPATIAL_SIMPLIFICATION:
            # Try to convert spatial file to CSV using spatial_helpers
            logger.info(
                "Converting spatial file to CSV with a simplification relative tolerance of {}...".format(
                    conf.SPATIAL_SIMPLIFICATION_RELATIVE_TOLERANCE
                )
            )

            try:
                # Use the convert_to_csv function from spatial_helpers
                success, error_message = sh.process_spatial_file(
                    qsv_spatial_file,
                    resource_format,
                    output_csv_path=qsv_spatial_csv,
                    tolerance=conf.SPATIAL_SIMPLIFICATION_RELATIVE_TOLERANCE,
                    task_logger=logger,
                )

                if success:
                    logger.info(
                        "Spatial file successfully simplified and converted to CSV"
                    )
                    tmp = qsv_spatial_csv

                    # Check if the resource already exists
                    simplified_resource_name = (
                        os.path.splitext(resource["name"])[0]
                        + "_simplified"
                        + os.path.splitext(resource["name"])[1]
                    )
                    existing_resource, existing_resource_id = dsu.resource_exists(
                        resource["package_id"], simplified_resource_name
                    )

                    if existing_resource:
                        logger.info(
                            "Simplified resource already exists. Replacing it..."
                        )
                        dsu.delete_resource(existing_resource_id)
                    else:
                        logger.info(
                            "Simplified resource does not exist. Uploading it..."
                        )
                        new_simplified_resource = {
                            "package_id": resource["package_id"],
                            "name": os.path.splitext(resource["name"])[0]
                            + "_simplified"
                            + os.path.splitext(resource["name"])[1],
                            "url": "",
                            "format": resource["format"],
                            "hash": "",
                            "mimetype": resource["mimetype"],
                            "mimetype_inner": resource["mimetype_inner"],
                        }
                        dsu.upload_resource(new_simplified_resource, qsv_spatial_file)

                    simplification_failed_flag = False
                else:
                    logger.warning(
                        f"Upload of simplified spatial file failed: {error_message}"
                    )
                    simplification_failed_flag = True
            except Exception as e:
                logger.warning(f"Simplification and conversion failed: {str(e)}")
                logger.warning(
                    "Simplification and conversion failed. Using qsv geoconvert to convert to CSV, truncating large columns to {} characters...".format(
                        conf.QSV_STATS_STRING_MAX_LENGTH
                    )
                )
                simplification_failed_flag = True
                pass

        # If we are not auto-simplifying or simplification failed, use qsv geoconvert
        if not conf.AUTO_SPATIAL_SIMPLIFICATION or simplification_failed_flag:
            logger.info("Converting spatial file to CSV using qsv geoconvert...")

            # Run qsv geoconvert
            qsv_geoconvert_csv = os.path.join(temp_dir, "qsv_geoconvert.csv")
            try:
                subprocess.run(
                    [
                        conf.QSV_BIN,
                        "geoconvert",
                        tmp,
                        "geojson",
                        "csv",
                        "--max-length",
                        str(conf.QSV_STATS_STRING_MAX_LENGTH),
                        "--output",
                        qsv_geoconvert_csv,
                    ],
                    check=True,
                    capture_output=True,
                    text=True,
                )
            except subprocess.CalledProcessError as e:
                logger.error(f"qsv geoconvert failed: {e.stderr}")
                raise

            tmp = qsv_geoconvert_csv
            logger.info("Geoconverted successfully")

    else:
        # --- its not a spreadsheet nor a spatial format, its a CSV/TSV/TAB file ------
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
                    conf.QSV_BIN,
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
            [conf.QSV_BIN, "validate", tmp], check=True, capture_output=True, text=True
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
    if conf.SORT_AND_DUPE_CHECK or conf.DEDUP:
        logger.info("Checking for duplicates and if the CSV is sorted...")
        try:
            qsv_sortcheck = subprocess.run(
                [conf.QSV_BIN, "sortcheck", tmp, "--json"],
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
    if conf.DEDUP and dupe_count > 0:
        qsv_dedup_csv = os.path.join(temp_dir, "qsv_dedup.csv")
        logger.info("{:.} duplicate rows found. Deduping...".format(dupe_count))
        qsv_extdedup_cmd = [conf.QSV_BIN, "extdedup", tmp, qsv_dedup_csv]

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
            [conf.QSV_BIN, "headers", "--just-names", tmp],
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
    qsv_safenames_csv = os.path.join(temp_dir, "qsv_safenames.csv")
    logger.info('Checking for "database-safe" header names...')
    try:
        qsv_safenames = subprocess.run(
            [
                conf.QSV_BIN,
                "safenames",
                tmp,
                "--mode",
                "json",
                "--reserved",
                conf.RESERVED_COLNAMES,
                "--prefix",
                conf.UNSAFE_PREFIX,
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
                conf.QSV_BIN,
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
        subprocess.run([conf.QSV_BIN, "index", tmp], check=True)
    except subprocess.CalledProcessError as e:
        raise utils.JobError("Cannot index CSV: {}".format(e))

    # if SORT_AND_DUPE_CHECK = True, we already know the record count
    # so we can skip qsv count.
    if not conf.SORT_AND_DUPE_CHECK:
        # get record count, this is instantaneous with an index
        try:
            qsv_count = subprocess.run(
                [conf.QSV_BIN, "count", tmp], capture_output=True, check=True, text=True
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
    if conf.DEDUP:
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
        conf.QSV_BIN,
        "stats",
        tmp,
        "--infer-dates",
        "--dates-whitelist",
        "all",
        "--stats-jsonl",
        "--output",
        qsv_stats_csv,
    ]

    if conf.PREFER_DMY:
        qsv_stats_cmd.append("--prefer-dmy")

    # global conf.AUTO_INDEX_THRESHOLD
    if conf.AUTO_INDEX_THRESHOLD:
        qsv_stats_cmd.append("--cardinality")
    if conf.SUMMARY_STATS_OPTIONS:
        qsv_stats_cmd.append(conf.SUMMARY_STATS_OPTIONS)

    try:
        # If the file is a spatial format, we need to use --max-length
        # to truncate overly long strings from causing issues with
        # Python's CSV reader and Postgres's limits with the COPY command
        if spatial_format_flag:
            env = os.environ.copy()
            env["QSV_STATS_STRING_MAX_LENGTH"] = str(conf.QSV_STATS_STRING_MAX_LENGTH)
            qsv_stats = subprocess.run(qsv_stats_cmd, check=True, env=env)
        else:
            qsv_stats = subprocess.run(qsv_stats_cmd, check=True)
    except subprocess.CalledProcessError as e:
        raise utils.JobError(
            "Cannot infer data types and compile statistics: {}".format(e)
        )

    # Dictionary to look up stats by resource field name
    resource_fields_stats = {}

    with open(qsv_stats_csv, mode="r") as inp:
        reader = csv.DictReader(inp)
        for row in reader:
            # Add to stats dictionary with resource field name as key
            resource_fields_stats[row["field"]] = {"stats": row}

            fr = {k: v for k, v in row.items()}
            schema_field = fr.get("field", "Unnamed Column")
            if schema_field.startswith("qsv_"):
                break
            headers.append(schema_field)
            types.append(fr.get("type", "String"))
            headers_min.append(fr["min"])
            headers_max.append(fr["max"])
            if conf.AUTO_INDEX_THRESHOLD:
                headers_cardinality.append(int(fr.get("cardinality") or 0))

    # Get the field stats for each field in the headers list
    existing = dsu.datastore_resource_exists(resource_id)
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
        dsu.delete_datastore_resource(resource_id)

    # 1st pass of building headers_dict
    # here we map inferred types to postgresql data types
    default_type = "String"
    temp_headers_dicts = [
        dict(
            id=field[0],
            type=conf.TYPE_MAPPING.get(
                str(field[1]) if field[1] else default_type, "text"
            ),
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
                int(headers_max[idx]) <= conf.POSTGRES_INT_MAX
                and int(headers_min[idx]) >= conf.POSTGRES_INT_MIN
            ):
                header_type = "integer"
            elif (
                int(headers_max[idx]) <= conf.POSTGRES_BIGINT_MAX
                and int(headers_min[idx]) >= conf.POSTGRES_BIGINT_MIN
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
                if type_override in list(conf.TYPE_MAPPING.values()):
                    h["type"] = type_override

    logger.info(
        "Determined headers and types: {headers}...".format(headers=headers_dicts)
    )

    # save stats to the datastore by loading qsv_stats_csv directly using COPY
    stats_table = sql.Identifier(resource_id + "-druf-stats")

    try:
        raw_connection_statsfreq = psycopg2.connect(conf.DATASTORE_WRITE_URL)
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
                mean TEXT,
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
    more_debug_info = logger.getEffectiveLevel() >= logging.DEBUG
    if more_debug_info:
        try:
            debug_stats_path = os.path.join("/tmp", os.path.basename(qsv_stats_csv))
            shutil.copy2(qsv_stats_csv, debug_stats_path)
            logger.debug(f"Copied stats CSV to {debug_stats_path} for debugging")
        except Exception as e:
            logger.debug(f"Failed to copy stats CSV to /tmp for debugging: {e}")

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
        conf.QSV_BIN,
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
    if more_debug_info:
        try:
            debug_freq_path = os.path.join("/tmp", os.path.basename(qsv_freq_csv))
            shutil.copy2(qsv_freq_csv, debug_freq_path)
            logger.debug(f"Copied frequency CSV to {debug_freq_path} for debugging")
        except Exception as e:
            logger.debug(f"Failed to copy frequency CSV to /tmp for debugging: {e}")

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
    # if conf.PREVIEW_ROWS is not zero, create a preview using qsv slice
    # we do the rows_to_copy > conf.PREVIEW_ROWS to check if we don't need to slice
    # the CSV anymore if we only did a partial download of N conf.PREVIEW_ROWS already
    rows_to_copy = record_count
    if conf.PREVIEW_ROWS and record_count > conf.PREVIEW_ROWS:
        if conf.PREVIEW_ROWS > 0:
            # conf.PREVIEW_ROWS is positive, slice from the beginning
            logger.info("Preparing {:,}-row preview...".format(conf.PREVIEW_ROWS))
            qsv_slice_csv = os.path.join(temp_dir, "qsv_slice.csv")
            try:
                qsv_slice = subprocess.run(
                    [
                        conf.QSV_BIN,
                        "slice",
                        "--len",
                        str(conf.PREVIEW_ROWS),
                        tmp,
                        "--output",
                        qsv_slice_csv,
                    ],
                    check=True,
                )
            except subprocess.CalledProcessError as e:
                raise utils.JobError("Cannot create a preview slice: {}".format(e))
            rows_to_copy = conf.PREVIEW_ROWS
            tmp = qsv_slice_csv
        else:
            # conf.PREVIEW_ROWS is negative, slice from the end
            # TODO: do http range request so we don't have to download the whole file
            # to slice from the end
            slice_len = abs(conf.PREVIEW_ROWS)
            logger.info("Preparing {:,}-row preview from the end...".format(slice_len))
            qsv_slice_csv = os.path.join(temp_dir, "qsv_slice.csv")
            try:
                qsv_slice = subprocess.run(
                    [
                        conf.QSV_BIN,
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
            conf.QSV_BIN,
            "datefmt",
            datecols,
            tmp,
            "--output",
            qsv_applydp_csv,
        ]
        if conf.PREFER_DMY:
            qsv_applydp_cmd.append("--prefer-dmy")
        logger.info(
            'Formatting dates "{}" to ISO 8601/RFC 3339 format with PREFER_DMY: {}...'.format(
                datecols, conf.PREFER_DMY
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
    if conf.PII_SCREENING:
        piiscreening_start = time.perf_counter()
        pii_found_abort = conf.PII_FOUND_ABORT

        # DP+ comes with default regex patterns for PII (SSN, credit cards,
        # email, bank account numbers, & phone number). The DP+ admin can
        # use a custom set of regex patterns by pointing to a resource with
        # a text file, with each line having a regex pattern, and an optional
        # label comment prefixed with "#" (e.g. #SSN, #Email, #Visa, etc.)
        if conf.PII_REGEX_RESOURCE_ID:
            pii_regex_resource_exist = dsu.datastore_resource_exists(
                conf.PII_REGEX_RESOURCE_ID
            )
            if pii_regex_resource_exist:
                pii_resource = dsu.get_resource(conf.PII_REGEX_RESOURCE_ID)
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

        if conf.PII_QUICK_SCREEN:
            logger.info("Quickly scanning for PII using {}...".format(pii_regex_file))
            try:
                qsv_searchset = subprocess.run(
                    [
                        conf.QSV_BIN,
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
                        conf.QSV_BIN,
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

        if pii_found and pii_found_abort and not conf.PII_SHOW_CANDIDATES:
            logger.error("PII Candidate/s Found!")
            if conf.PII_QUICK_SCREEN:
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
        elif pii_found and conf.PII_SHOW_CANDIDATES and not conf.PII_QUICK_SCREEN:
            # TODO: Create PII Candidates resource and set package to private if its not private
            # ------------ Create PII Preview Resource ------------------
            logger.warning(
                "PII CANDIDATE/S FOUND! Found {} PII candidate/s in {} row/s. Creating PII preview...".format(
                    pii_total_matches, pii_rows_with_matches
                )
            )
            pii_resource_id = resource_id + "-pii"

            try:
                raw_connection_pii = psycopg2.connect(conf.DATASTORE_WRITE_URL)
            except psycopg2.Error as e:
                raise utils.JobError("Could not connect to the Datastore: {}".format(e))
            else:
                cur_pii = raw_connection_pii.cursor()

            # check if the pii already exist
            existing_pii = dsu.datastore_resource_exists(pii_resource_id)

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

                    dsu.delete_datastore_resource(existing_pii_alias_of)
                    dsu.delete_resource(existing_pii_alias_of)

            pii_alias = [pii_resource_id]

            # run stats on pii preview CSV to get header names and infer data types
            # we don't need summary statistics, so use the --typesonly option
            try:
                qsv_pii_stats = subprocess.run(
                    [
                        conf.QSV_BIN,
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
                dict(id=ele.split(",")[0], type=conf.TYPE_MAPPING[ele.split(",")[1]])
                for idx, ele in enumerate(pii_stats.splitlines()[1:], 1)
            ]

            pii_resource = {
                "package_id": resource["package_id"],
                "name": resource["name"] + " - PII",
                "format": "CSV",
                "mimetype": "text/csv",
            }
            pii_response = dsu.send_resource_to_datastore(
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
            dsu.update_resource(pii_resource)

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

    if conf.PREVIEW_ROWS:
        logger.info("COPYING {:,}-row preview to Datastore...".format(rows_to_copy))
    else:
        logger.info("COPYING {:,} rows to Datastore...".format(rows_to_copy))

    # first, let's create an empty datastore table w/ guessed types
    dsu.send_resource_to_datastore(
        resource=None,
        resource_id=resource["id"],
        headers=headers_dicts,
        records=None,
        aliases=None,
        calculate_record_count=False,
    )

    copied_count = 0
    try:
        raw_connection = psycopg2.connect(conf.DATASTORE_WRITE_URL)
    except psycopg2.Error as e:
        raise utils.JobError("Could not connect to the Datastore: {}".format(e))
    else:
        cur = raw_connection.cursor()

        # truncate table to use copy freeze option and further increase
        # performance as there is no need for WAL logs to be maintained
        # https://www.postgresql.org/docs/current/populate.html#POPULATE-COPY-FROM
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
        with open(tmp, "rb", conf.COPY_READBUFFER_SIZE) as f:
            try:
                cur.copy_expert(copy_sql, f, size=conf.COPY_READBUFFER_SIZE)
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
    # PROCESS DRUF JINJA2 FORMULAE
    # ============================================================
    # Check if there are any fields with DRUF keys in the scheming_yaml
    # There are two types of DRUF keys:
    # 1. "formula": This is used to update the field value DIRECTLY
    #    when the resource is created/updated. It can update both package and resource fields.
    # 2. "suggestion_formula": This is used to populate the suggestion
    #    popovers DURING data entry/curation.
    # DRUF keys are stored as jinja2 template expressions in the scheming_yaml
    # and are rendered using the Jinja2 template engine.
    formulae_start = time.perf_counter()

    # Fetch the scheming_yaml and package
    package_id = resource["package_id"]
    scheming_yaml, package = dsu.get_scheming_yaml(
        package_id, scheming_yaml_type="dataset"
    )

    logger.debug(f"package: {package}")

    # Initialize the formula processor
    formula_processor = j2h.FormulaProcessor(
        scheming_yaml, package, resource_fields_stats, logger
    )

    # FIRST WE PROCESS THE FORMULAE THAT UPDATE THE
    # PACKAGE AND RESOURCE FIELDS DIRECTLY
    # using the package_patch CKAN API so we only update the fields
    # that with formulae
    package_updates = formula_processor.process_formulae(
        "package", "dataset_fields", "formula"
    )
    if package_updates:
        # Update package with formula results
        package.update(package_updates)
        try:
            patched_package = dsu.patch_package(package)
            logger.debug(f"Package after patching: {patched_package}")
            package = patched_package
            logger.info("PACKAGE formulae processed...")
        except Exception as e:
            logger.error(f"Error patching package: {str(e)}")

    # Process resource formulae
    # as this is a direct update, we update the resource fields directly
    resource_updates = formula_processor.process_formulae(
        "resource", "resource_fields", "formula"
    )
    if resource_updates:
        # Update resource with formula results
        resource.update(resource_updates)
        logger.info("RESOURCE formulae processed...")

    # NOW WE PROCESS THE SUGGESTIONS
    # we update the package dpp_suggestions field
    # from which the Suggestion popover UI will pick it up
    package_suggestions = formula_processor.process_formulae(
        "package", "dataset_fields", "suggestion_formula"
    )
    if package_suggestions:
        revise_update_content = {"package": package_suggestions}
        try:
            revised_package = dsu.revise_package(
                package_id, update={"dpp_suggestions": revise_update_content}
            )
            logger.debug(f"Package after revising: {revised_package}")
            package = revised_package
            logger.info("PACKAGE suggestion formulae processed...")
        except Exception as e:
            logger.error(f"Error revising package: {str(e)}")

    # Process resource suggestion formulae
    # Note how we still update the PACKAGE dpp_suggestions field
    # and there is NO RESOURCE dpp_suggestions field.
    # This is because suggestion formulae are used to populate the
    # suggestion popover DURING data entry/curation and suggestion formulae
    # may update both package and resource fields.
    resource_suggestions = formula_processor.process_formulae(
        "resource", "resource_fields", "suggestion_formula"
    )
    if resource_suggestions:
        resource_name = resource["name"]
        revise_update_content = {"resource": {resource_name: resource_suggestions}}

        # Handle existing suggestions
        if package.get("dpp_suggestions"):
            package["dpp_suggestions"].update(revise_update_content["resource"])
        else:
            package["dpp_suggestions"] = revise_update_content["resource"]

        try:
            revised_package = dsu.revise_package(
                package_id, update={"dpp_suggestions": revise_update_content}
            )
            logger.debug(f"Package after revising: {revised_package}")
            package = revised_package
            logger.info("RESOURCE suggestion formulae processed...")
        except Exception as e:
            logger.error(f"Error revising package: {str(e)}")

    # -------------------- FORMULAE PROCESSING DONE --------------------
    formulae_elapsed = time.perf_counter() - formulae_start
    logger.info(
        "FORMULAE PROCESSING DONE! Processed in {:,.2f} seconds.".format(
            formulae_elapsed
        )
    )

    # ============================================================
    # UPDATE METADATA
    # ============================================================
    metadata_start = time.perf_counter()
    logger.info("UPDATING RESOURCE METADATA...")

    # --------------------- AUTO-ALIASING ------------------------
    # aliases are human-readable, and make it easier to use than resource id hash
    # when using the Datastore API and in SQL queries
    alias = None
    if conf.AUTO_ALIAS:
        logger.info(
            "AUTO-ALIASING. Auto-alias-unique: {} ...".format(conf.AUTO_ALIAS_UNIQUE)
        )
        # get package info, so we can construct the alias
        package = dsu.get_package(resource["package_id"])

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
            if conf.AUTO_ALIAS_UNIQUE and alias_count > 1:
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
    if conf.ADD_SUMMARY_STATS_RESOURCE or conf.SUMMARY_STATS_WITH_PREVIEW:
        stats_resource_id = resource_id + "-stats"

        # check if the stats already exist
        existing_stats = dsu.datastore_resource_exists(stats_resource_id)
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

                dsu.delete_datastore_resource(existing_stats_alias_of)
                dsu.delete_resource(existing_stats_alias_of)

        stats_aliases = [stats_resource_id]
        if conf.AUTO_ALIAS:
            auto_alias_stats_id = alias + "-stats"
            stats_aliases.append(auto_alias_stats_id)

            # check if the summary-stats alias already exist. We need to do this as summary-stats resources
            # may end up having the same alias if AUTO_ALIAS_UNIQUE is False, so we need to drop the
            # existing summary stats-alias.
            existing_alias_stats = dsu.datastore_resource_exists(auto_alias_stats_id)
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

                    dsu.delete_datastore_resource(existing_stats_alias_of)
                    dsu.delete_resource(existing_stats_alias_of)

        # run stats on stats CSV to get header names and infer data types
        # we don't need summary statistics, so use the --typesonly option
        try:
            qsv_stats_stats = subprocess.run(
                [
                    conf.QSV_BIN,
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
            dict(id=ele.split(",")[0], type=conf.TYPE_MAPPING[ele.split(",")[1]])
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
        stats_response = dsu.send_resource_to_datastore(
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
        dsu.update_resource(stats_resource)

    cur.close()
    raw_connection.commit()

    resource["datastore_active"] = True
    resource["total_record_count"] = record_count
    if conf.PREVIEW_ROWS < record_count or (conf.PREVIEW_ROWS > 0):
        resource["preview"] = True
        resource["preview_rows"] = copied_count
    else:
        resource["preview"] = False
        resource["preview_rows"] = None
        resource["partial_download"] = False
    dsu.update_resource(resource)

    # tell CKAN to calculate_record_count and set alias if set
    dsu.send_resource_to_datastore(
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
    if (
        conf.AUTO_INDEX_THRESHOLD
        or (conf.AUTO_INDEX_DATES and datetimecols_list)
        or conf.AUTO_UNIQUE_INDEX
    ):
        index_start = time.perf_counter()
        logger.info(
            "AUTO-INDEXING. Auto-index threshold: {:,} unique value/s. Auto-unique index: {} Auto-index dates: {} ...".format(
                conf.AUTO_INDEX_THRESHOLD, conf.AUTO_UNIQUE_INDEX, conf.AUTO_INDEX_DATES
            )
        )
        index_cur = raw_connection.cursor()

        # if auto_index_threshold == -1
        # we index all the columns
        if conf.AUTO_INDEX_THRESHOLD == -1:
            conf.AUTO_INDEX_THRESHOLD = record_count

        index_count = 0
        for idx, cardinality in enumerate(headers_cardinality):
            curr_col = headers[idx]
            if (
                conf.AUTO_INDEX_THRESHOLD > 0
                or conf.AUTO_INDEX_DATES
                or conf.AUTO_UNIQUE_INDEX
            ):
                if cardinality == record_count and conf.AUTO_UNIQUE_INDEX:
                    # all the values are unique for this column, create a unique index
                    if conf.PREVIEW_ROWS > 0:
                        unique_value_count = min(conf.PREVIEW_ROWS, cardinality)
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
                elif cardinality <= conf.AUTO_INDEX_THRESHOLD or (
                    conf.AUTO_INDEX_DATES and (curr_col in datetimecols_list)
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
      Formulae processing: {formulae_elapsed:,.2f}
      COPYing: {copy_elapsed:,.2f}
      Metadata updates: {metadata_elapsed:,.2f}
      Indexing: {index_elapsed:,.2f}
    TOTAL ELAPSED TIME: {total_elapsed:,.2f}
    """
    logger.info(end_msg)
