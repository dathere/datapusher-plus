# -*- coding: utf-8 -*-
# flake8: noqa: E501

# Standard library imports
import csv
import hashlib
import locale
import mimetypes
import os
import subprocess
import tempfile
import time
from urllib.parse import urlsplit, urlparse
import logging
import uuid
import sys
import json
import requests
from pathlib import Path
from typing import Dict, Any, Optional, List

# Third-party imports
import psycopg2
from psycopg2 import sql
from datasize import DataSize
from dateutil.parser import parse as parsedate
import traceback
import sqlalchemy as sa
from rq import get_current_job

import ckanext.datapusher_plus.utils as utils
import ckanext.datapusher_plus.helpers as dph
import ckanext.datapusher_plus.jinja2_helpers as j2h
from ckanext.datapusher_plus.job_exceptions import HTTPError
import ckanext.datapusher_plus.config as conf
import ckanext.datapusher_plus.spatial_helpers as sh
import ckanext.datapusher_plus.datastore_utils as dsu
from ckanext.datapusher_plus.logging_utils import TRACE
from ckanext.datapusher_plus.qsv_utils import QSVCommand
from ckanext.datapusher_plus.pii_screening import screen_for_pii

if locale.getdefaultlocale()[0]:
    lang, encoding = locale.getdefaultlocale()
    locale.setlocale(locale.LC_ALL, locale=(lang, encoding))
else:
    locale.setlocale(locale.LC_ALL, "")


def validate_input(input: Dict[str, Any]) -> None:
    # Especially validate metadata which is provided by the user
    if "metadata" not in input:
        raise utils.JobError("Metadata missing")

    data = input["metadata"]

    if "resource_id" not in data:
        raise utils.JobError("No id provided.")


def callback_datapusher_hook(result_url: str, job_dict: Dict[str, Any]) -> bool:
    api_token = utils.get_dp_plus_user_apitoken()
    headers: Dict[str, str] = {
        "Content-Type": "application/json",
        "Authorization": api_token,
    }

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


def datapusher_plus_to_datastore(input: Dict[str, Any]) -> Optional[str]:
    """
    This is the main function that is called by the datapusher_plus worker

    Errors are caught and logged in the database

    Args:
        input: Dictionary containing metadata and other job information

    Returns:
        Optional[str]: Returns "error" if there was an error, None otherwise
    """
    job_dict: Dict[str, Any] = dict(metadata=input["metadata"], status="running")
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
        log.error(f"Datapusher Plus error: {e}, {traceback.format_exc()}")
        errored = True
    except Exception as e:
        dph.mark_job_as_errored(
            job_id, traceback.format_tb(sys.exc_info()[2])[-1] + repr(e)
        )
        job_dict["status"] = "error"
        job_dict["error"] = str(e)
        log = logging.getLogger(__name__)
        log.error(f"Datapusher Plus error: {e}, {traceback.format_exc()}")
        errored = True
    finally:
        # job_dict is defined in datapusher_hook's docstring
        is_saved_ok = callback_datapusher_hook(
            result_url=input["result_url"], job_dict=job_dict
        )
        errored = errored or not is_saved_ok
    return "error" if errored else None


def push_to_datastore(
    input: Dict[str, Any], task_id: str, dry_run: bool = False
) -> Optional[List[Dict[str, Any]]]:
    """Download and parse a resource push its data into CKAN's DataStore.

    An asynchronous job that gets a resource from CKAN, downloads the
    resource's data file and, if the data file has changed since last time,
    parses the data and posts it into CKAN's DataStore.

    Args:
        input: Dictionary containing metadata and other job information
        task_id: Unique identifier for the task
        dry_run: If True, fetch and parse the data file but don't actually post the
            data to the DataStore, instead return the data headers and rows that
            would have been posted.

    Returns:
        Optional[List[Dict[str, Any]]]: If dry_run is True, returns the headers and rows
            that would have been posted. Otherwise returns None.
    """
    # Ensure temporary files are removed after run
    with tempfile.TemporaryDirectory() as temp_dir:
        return _push_to_datastore(task_id, input, dry_run=dry_run, temp_dir=temp_dir)


def _push_to_datastore(
    task_id: str,
    input: Dict[str, Any],
    dry_run: bool = False,
    temp_dir: Optional[str] = None,
) -> Optional[List[Dict[str, Any]]]:
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

    # set the log level to the config upload_log_level
    try:
        log_level = getattr(logging, conf.UPLOAD_LOG_LEVEL.upper())
    except AttributeError:
        # fallback to our custom TRACE level
        log_level = TRACE

    # set the log level to the config upload_log_level
    logger.setLevel(logging.INFO)
    logger.info(f"Setting log level to {logging.getLevelName(int(log_level))}")
    logger.setLevel(log_level)

    # check if conf.QSV_BIN exists
    if not Path(conf.QSV_BIN).is_file():
        raise utils.JobError(f"{conf.QSV_BIN} not found.")

    # Initialize QSVCommand
    qsv = QSVCommand(logger=logger)

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
    dataset_stats = {}

    # fetch the resource data
    logger.info(f"Fetching from: {resource_url}...")
    headers: Dict[str, str] = {}
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
            logger.info(f"Rewritten resource url to: {resource_url}")

    try:
        kwargs: Dict[str, Any] = {
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
                        f"Resource too large to download: {DataSize(int(cl)):.2MB} > max ({DataSize(int(max_content_length)):.2MB})."
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
                    logger.info(f"Inferred file format: {resource_format}")
                else:
                    raise utils.JobError(
                        "Server did not return content-type. Please specify format."
                    )
            else:
                logger.info(f"File format: {resource_format}")

            tmp = os.path.join(temp_dir, "tmp." + resource_format)
            length = 0
            # using MD5 for file deduplication only
            # no need for it to be cryptographically secure
            m = hashlib.md5()  # DevSkim: ignore DS126858

            # download the file
            if cl:
                logger.info(f"Downloading {DataSize(int(cl)):.2MB} file...")
            else:
                logger.info("Downloading file of unknown size...")

            with open(tmp, "wb") as tmp_file:
                for chunk in response.iter_content(conf.CHUNK_SIZE):
                    length += len(chunk)
                    if length > max_content_length and not conf.PREVIEW_ROWS:
                        raise utils.JobError(
                            f"Resource too large to process: {length} > max ({max_content_length})."
                        )
                    tmp_file.write(chunk)
                    m.update(chunk)

    except requests.HTTPError as e:
        raise HTTPError(
            f"DataPusher+ received a bad HTTP response when trying to download "
            f"the data file from {resource_url}. Status code: {e.response.status_code}, "
            f"Response content: {e.response.content}",
            status_code=e.response.status_code,
            request_url=resource_url,
            response=e.response.content,
        )
    except requests.RequestException as e:
        raise HTTPError(
            message=str(e),
            status_code=None,
            request_url=resource_url,
            response=None,
        )

    file_hash = m.hexdigest()
    dataset_stats["ORIGINAL_FILE_SIZE"] = length

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
        logger.warning(f"Upload skipped as the file hash hasn't changed: {file_hash}.")
        return

    resource["hash"] = file_hash

    fetch_elapsed = time.perf_counter() - timer_start
    logger.info(
        f"Fetched {DataSize(length):.2MB} file in {fetch_elapsed:,.2f} seconds."
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
            f"More than one file in the ZIP file ({file_count} files), saving metadata..."
            if file_count > 1
            else f"Extracted {unzipped_format} file: {extracted_path}"
        )
        tmp = extracted_path

    # ===================================================================================
    # ANALYZE WITH QSV
    # ===================================================================================
    # Start Analysis using qsv instead of messytables, as
    # 1) its type inferences are bullet-proof not guesses as it scans the entire file,
    # 2) its super-fast, and
    # 3) it has addl data-wrangling capabilities we use in DP+ (e.g. stats, dedup, etc.)
    dupe_count = 0
    record_count = 0
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
        logger.info(f"Converting {file_format} sheet {default_excel_sheet} to CSV...")
        # first, we need a temporary spreadsheet filename with the right file extension
        # we only need the filename though, that's why we remove it
        # and create a hardlink to the file we got from CKAN
        qsv_spreadsheet = os.path.join(temp_dir, "qsv_spreadsheet." + file_format)
        os.link(tmp, qsv_spreadsheet)

        # run `qsv excel` and export it to a CSV
        # use --trim option to trim column names and the data
        qsv_excel_csv = os.path.join(temp_dir, "qsv_excel.csv")
        try:
            qsv_excel = qsv.excel(
                qsv_spreadsheet,
                sheet=default_excel_sheet,
                trim=True,
                output_file=qsv_excel_csv,
            )
        except utils.JobError as e:
            raise utils.JobError(
                f"Upload aborted. Cannot export spreadsheet(?) to CSV: {e}"
            )
        excel_export_msg = qsv_excel.stderr
        logger.info(f"{excel_export_msg}...")
        tmp = qsv_excel_csv
    elif resource_format.upper() in ["SHP", "QGIS", "GEOJSON"]:
        logger.info("SHAPEFILE or GEOJSON file detected...")

        qsv_spatial_file = os.path.join(
            temp_dir,
            "qsv_spatial_" + str(uuid.uuid4()) + "." + resource_format,
        )
        os.link(tmp, qsv_spatial_file)
        qsv_spatial_csv = os.path.join(temp_dir, "qsv_spatial.csv")

        if conf.AUTO_SPATIAL_SIMPLIFICATION:
            # Try to convert spatial file to CSV using spatial_helpers
            logger.info(
                f"Converting spatial file to CSV with a simplification relative tolerance of {conf.SPATIAL_SIMPLIFICATION_RELATIVE_TOLERANCE}..."
            )

            try:
                # Use the convert_to_csv function from spatial_helpers
                success, error_message, bounds = sh.process_spatial_file(
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

                    # Check if the simplified resource already exists
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

                        # Add bounds information if available
                        if bounds:
                            minx, miny, maxx, maxy = bounds
                            new_simplified_resource.update(
                                {
                                    "dpp_spatial_extent": {
                                        "type": "BoundingBox",
                                        "coordinates": [
                                            [minx, miny],
                                            [maxx, maxy],
                                        ],
                                    }
                                }
                            )
                            logger.info(
                                f"Added dpp_spatial_extent to resource metadata: {bounds}"
                            )

                        dsu.upload_resource(new_simplified_resource, qsv_spatial_file)

                        # delete the simplified spatial file
                        os.remove(qsv_spatial_file)

                    simplification_failed_flag = False
                else:
                    logger.warning(
                        f"Upload of simplified spatial file failed: {error_message}"
                    )
                    simplification_failed_flag = True
            except Exception as e:
                logger.warning(f"Simplification and conversion failed: {str(e)}")
                logger.warning(
                    f"Simplification and conversion failed. Using qsv geoconvert to convert to CSV, truncating large columns to {conf.QSV_STATS_STRING_MAX_LENGTH} characters..."
                )
                simplification_failed_flag = True
                pass

        # If we are not auto-simplifying or simplification failed, use qsv geoconvert
        if not conf.AUTO_SPATIAL_SIMPLIFICATION or simplification_failed_flag:
            logger.info("Converting spatial file to CSV using qsv geoconvert...")

            # Run qsv geoconvert
            qsv_geoconvert_csv = os.path.join(temp_dir, "qsv_geoconvert.csv")
            try:
                qsv.geoconvert(
                    tmp,
                    resource_format,
                    "csv",
                    max_length=conf.QSV_STATS_STRING_MAX_LENGTH,
                    output_file=qsv_geoconvert_csv,
                )
            except utils.JobError as e:
                raise utils.JobError(f"qsv geoconvert failed: {e}")

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
            logger.info(f"Normalizing/UTF-8 transcoding {resource_format}...")
        else:
            # if not CSV (e.g. TSV, TAB, etc.) we need to normalize to CSV
            logger.info(f"Normalizing/UTF-8 transcoding {resource_format} to CSV...")

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
        logger.info(f"Identified encoding of the file: {file_encoding.stdout}")

        # trim the encoding string
        file_encoding.stdout = file_encoding.stdout.strip()

        # using iconv to re-encode in UTF-8 OR ASCII (as ASCII is a subset of UTF-8)
        if file_encoding.stdout != "UTF-8" and file_encoding.stdout != "ASCII":
            logger.info(
                f"File is not UTF-8 encoded. Re-encoding from {file_encoding.stdout} to UTF-8"
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
                raise utils.JobError(
                    f"Job aborted as the file cannot be re-encoded to UTF-8. {e.stderr}"
                )
            f = open(qsv_input_utf_8_encoded_csv, "wb")
            f.write(cmd.stdout)
            f.close()
            logger.info("Successfully re-encoded to UTF-8")

        else:
            qsv_input_utf_8_encoded_csv = tmp
        try:
            qsv.input(tmp, trim_headers=True, output_file=qsv_input_csv)
        except utils.JobError as e:
            raise utils.JobError(
                f"Job aborted as the file cannot be normalized/transcoded: {e}."
            )
        tmp = qsv_input_csv
        logger.info("Normalized & transcoded...")

    # ------------------------------------- Validate CSV --------------------------------------
    # Run an RFC4180 check with `qsv validate` against the normalized, UTF-8 encoded CSV file.
    # Even excel exported CSVs can be potentially invalid, as it allows the export of "flexible"
    # CSVs - i.e. rows may have different column counts.
    # If it passes validation, we can handle it with confidence downstream as a "normal" CSV.
    logger.info("Validating CSV...")
    try:
        qsv.validate(tmp)
    except utils.JobError as e:
        raise utils.JobError(f"qsv validate failed: {e}")

    logger.info("Well-formed, valid CSV file confirmed...")

    # --------------------- Sortcheck --------------------------
    # if SORT_AND_DUPE_CHECK is True or DEDUP is True
    # check if the file is sorted and if it has duplicates
    # get the record count, unsorted breaks and duplicate count as well
    if conf.SORT_AND_DUPE_CHECK or conf.DEDUP:
        logger.info("Checking for duplicates and if the CSV is sorted...")

        try:
            qsv_sortcheck = qsv.sortcheck(tmp, json_output=True, uses_stdio=True)
        except utils.JobError as e:
            raise utils.JobError(
                f"Failed to check if CSV is sorted and has duplicates: {e}"
            )

        try:
            # Handle both subprocess.CompletedProcess and dict outputs
            stdout_content = (
                qsv_sortcheck.stdout
                if hasattr(qsv_sortcheck, "stdout")
                else qsv_sortcheck.get("stdout")
            )
            sortcheck_json = json.loads(str(stdout_content))
        except (json.JSONDecodeError, AttributeError) as e:
            raise utils.JobError(f"Failed to parse sortcheck JSONoutput: {e}")

        try:
            # Extract and validate required fields
            is_sorted = bool(sortcheck_json.get("sorted", False))
            record_count = int(sortcheck_json.get("record_count", 0))
            unsorted_breaks = int(sortcheck_json.get("unsorted_breaks", 0))
            dupe_count = int(sortcheck_json.get("dupe_count", 0))
            dataset_stats["IS_SORTED"] = is_sorted
            dataset_stats["RECORD_COUNT"] = record_count
            dataset_stats["UNSORTED_BREAKS"] = unsorted_breaks
            dataset_stats["DUPE_COUNT"] = dupe_count
        except (ValueError, TypeError) as e:
            raise utils.JobError(f"Invalid numeric value in sortcheck output: {e}")

        # Format the message with clear statistics
        sortcheck_msg = f"Sorted: {is_sorted}; Unsorted breaks: {unsorted_breaks:,}"
        if is_sorted and dupe_count > 0:
            sortcheck_msg = f"{sortcheck_msg}; Duplicates: {dupe_count:,}"

        logger.info(sortcheck_msg)

    # --------------- Do we need to dedup? ------------------
    if conf.DEDUP and dupe_count > 0:
        qsv_dedup_csv = os.path.join(temp_dir, "qsv_dedup.csv")
        logger.info(f"{dupe_count} duplicate rows found. Deduping...")

        try:
            qsv.extdedup(tmp, qsv_dedup_csv)
        except utils.JobError as e:
            raise utils.JobError(f"Check for duplicates error: {e}")

        dataset_stats["DEDUPED"] = True
        tmp = qsv_dedup_csv
        logger.info(f"Deduped CSV saved to {qsv_dedup_csv}")
    else:
        dataset_stats["DEDUPED"] = False

    # ----------------------- Headers & Safenames ---------------------------
    # get existing header names, so we can use them for data dictionary labels
    # should we need to change the column name to make it "db-safe"
    try:
        qsv_headers = qsv.headers(tmp, just_names=True)
    except utils.JobError as e:
        raise utils.JobError(f"Cannot scan CSV headers: {e}")
    original_headers = str(qsv_headers.stdout).strip()
    original_header_dict = {
        idx: ele for idx, ele in enumerate(original_headers.splitlines())
    }

    # now, ensure our column/header names identifiers are "safe names"
    # i.e. valid postgres/CKAN Datastore identifiers
    qsv_safenames_csv = os.path.join(temp_dir, "qsv_safenames.csv")
    logger.info('Checking for "database-safe" header names...')
    try:
        qsv_safenames = qsv.safenames(
            tmp,
            mode="json",
            reserved=conf.RESERVED_COLNAMES,
            prefix=conf.UNSAFE_PREFIX,
            uses_stdio=True,
        )
    except utils.JobError as e:
        raise utils.JobError(f"Cannot scan CSV headers: {e}")

    unsafe_json = json.loads(str(qsv_safenames.stdout))
    unsafe_headers = unsafe_json["unsafe_headers"]

    if unsafe_headers:
        logger.info(
            f'"{len(unsafe_headers)} unsafe" header names found ({unsafe_headers}). Sanitizing..."'
        )
        qsv_safenames = qsv.safenames(
            tmp, mode="conditional", output_file=qsv_safenames_csv
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
        qsv.index(tmp)
    except utils.JobError as e:
        raise utils.JobError(f"Cannot index CSV: {e}")

    # if SORT_AND_DUPE_CHECK = True, we already know the record count
    # so we can skip qsv count.
    if not conf.SORT_AND_DUPE_CHECK:
        # get record count, this is instantaneous with an index
        try:
            qsv_count = qsv.count(tmp)
            record_count = int(str(qsv_count.stdout).strip())
            dataset_stats["RECORD_COUNT"] = record_count
        except utils.JobError as e:
            raise utils.JobError(f"Cannot count records in CSV: {e}")

    # its empty, nothing to do
    if record_count == 0:
        logger.warning("Upload skipped as there are zero records.")
        return

    # log how many records we detected
    unique_qualifier = ""
    if conf.DEDUP:
        unique_qualifier = "unique"
    logger.info(f"{record_count} {unique_qualifier} records detected...")

    # run qsv stats to get data types and summary statistics
    logger.info("Inferring data types and compiling statistics...")
    headers = []
    types = []
    headers_min = []
    headers_max = []
    headers_cardinality = []
    qsv_stats_csv = os.path.join(temp_dir, "qsv_stats.csv")

    try:
        # If the file is a spatial format, we need to use --max-length
        # to truncate overly long strings from causing issues with
        # Python's CSV reader and Postgres's limits with the COPY command
        if spatial_format_flag:
            env = os.environ.copy()
            env["QSV_STATS_STRING_MAX_LENGTH"] = str(conf.QSV_STATS_STRING_MAX_LENGTH)
            qsv_stats = qsv.stats(
                tmp,
                infer_dates=True,
                dates_whitelist=conf.QSV_DATES_WHITELIST,
                stats_jsonl=True,
                prefer_dmy=conf.PREFER_DMY,
                cardinality=bool(conf.AUTO_INDEX_THRESHOLD),
                summary_stats_options=conf.SUMMARY_STATS_OPTIONS,
                output_file=qsv_stats_csv,
                env=env,
            )
        else:
            qsv_stats = qsv.stats(
                tmp,
                infer_dates=True,
                dates_whitelist=conf.QSV_DATES_WHITELIST,
                stats_jsonl=True,
                prefer_dmy=conf.PREFER_DMY,
                cardinality=bool(conf.AUTO_INDEX_THRESHOLD),
                summary_stats_options=conf.SUMMARY_STATS_OPTIONS,
                output_file=qsv_stats_csv,
            )
    except utils.JobError as e:
        raise utils.JobError(f"Cannot infer data types and compile statistics: {e}")

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
        logger.info(f'Deleting existing resource "{resource_id}" from datastore.')
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

    logger.info(f"Determined headers and types: {headers_dicts}...")

    # save stats to the datastore by loading qsv_stats_csv directly using COPY
    stats_table = sql.Identifier(resource_id + "-druf-stats")

    try:
        raw_connection_statsfreq = psycopg2.connect(conf.DATASTORE_WRITE_URL)
    except psycopg2.Error as e:
        raise utils.JobError(f"Could not connect to the Datastore: {e}")
    else:
        cur_statsfreq = raw_connection_statsfreq.cursor()

    # Save stats to the datastore
    try:
        qsv.save_stats_to_datastore(
            qsv_stats_csv, resource_id, conf.DATASTORE_WRITE_URL, logger=logger
        )
    except utils.JobError as e:
        raise utils.JobError(f"Failed to save stats to datastore: {e}")

    # ----------------------- Frequency Table ---------------------------
    # compile a frequency table for each column
    qsv_freq_csv = os.path.join(temp_dir, "qsv_freq.csv")

    try:
        qsv.frequency(tmp, limit=conf.QSV_FREQ_LIMIT, output_file=qsv_freq_csv)
    except utils.JobError as e:
        raise utils.JobError(f"Cannot create a frequency table: {e}")

    resource_fields_freqs = {}
    try:
        resource_fields_freqs = qsv.save_freq_to_datastore(
            qsv_freq_csv, resource_id, conf.DATASTORE_WRITE_URL, logger=logger
        )
    except utils.JobError as e:
        raise utils.JobError(f"Failed to save frequency to datastore: {e}")

    # ------------------- Do we need to create a Preview?  -----------------------
    # if conf.PREVIEW_ROWS is not zero, create a preview using qsv slice
    # we do the rows_to_copy > conf.PREVIEW_ROWS to check if we don't need to slice
    # the CSV anymore if we only did a partial download of N conf.PREVIEW_ROWS already
    rows_to_copy = record_count
    if conf.PREVIEW_ROWS and record_count > conf.PREVIEW_ROWS:
        if conf.PREVIEW_ROWS > 0:
            # conf.PREVIEW_ROWS is positive, slice from the beginning
            logger.info(f"Preparing {conf.PREVIEW_ROWS}-row preview...")
            qsv_slice_csv = os.path.join(temp_dir, "qsv_slice.csv")
            try:
                qsv.slice(tmp, length=conf.PREVIEW_ROWS, output_file=qsv_slice_csv)
            except utils.JobError as e:
                raise utils.JobError(f"Cannot create a preview slice: {e}")
            rows_to_copy = conf.PREVIEW_ROWS
            tmp = qsv_slice_csv
        else:
            # conf.PREVIEW_ROWS is negative, slice from the end
            # TODO: do http range request so we don't have to download the whole file
            # to slice from the end
            slice_len = abs(conf.PREVIEW_ROWS)
            logger.info(f"Preparing {slice_len}-row preview from the end...")
            qsv_slice_csv = os.path.join(temp_dir, "qsv_slice.csv")
            try:
                qsv.slice(tmp, start=-1, length=slice_len, output_file=qsv_slice_csv)
            except utils.JobError as e:
                raise utils.JobError(f"Cannot create a preview slice from the end: {e}")
            rows_to_copy = slice_len
            tmp = qsv_slice_csv

        dataset_stats["PREVIEW_FILE_SIZE"] = os.path.getsize(tmp)
        dataset_stats["PREVIEW_RECORD_COUNT"] = rows_to_copy

    # ---------------- Normalize dates to RFC3339 format --------------------
    # if there are any datetime fields, normalize them to RFC3339 format
    # so we can readily insert them as timestamps into postgresql with COPY
    if datetimecols_list:
        qsv_applydp_csv = os.path.join(temp_dir, "qsv_applydp.csv")
        datecols = ",".join(datetimecols_list)

        logger.info(
            f'Formatting dates "{datecols}" to ISO 8601/RFC 3339 format with PREFER_DMY: {conf.PREFER_DMY}...'
        )
        try:
            qsv.datefmt(
                datecols,
                tmp,
                prefer_dmy=conf.PREFER_DMY,
                output_file=qsv_applydp_csv,
            )
        except utils.JobError as e:
            raise utils.JobError(f"Applydp error: {e}")
        tmp = qsv_applydp_csv

    # -------------------- QSV ANALYSIS DONE --------------------
    analysis_elapsed = time.perf_counter() - analysis_start
    logger.info(
        f"ANALYSIS DONE! Analyzed and prepped in {analysis_elapsed:,.2f} seconds."
    )

    # ----------------------------- PII Screening ------------------------------
    # we scan for Personally Identifiable Information (PII) using qsv's powerful
    # searchset command which can SIMULTANEOUSLY compare several regexes per
    # field in one pass
    piiscreening_start = 0
    piiscreening_elapsed = 0
    pii_found = False

    if conf.PII_SCREENING:
        piiscreening_start = time.perf_counter()
        pii_found = screen_for_pii(tmp, resource, qsv, temp_dir, logger)
        piiscreening_elapsed = time.perf_counter() - piiscreening_start

    dataset_stats["PII_SCREENING"] = conf.PII_SCREENING
    dataset_stats["PII_FOUND"] = pii_found

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
        logger.info(f"COPYING {rows_to_copy}-row preview to Datastore...")
    else:
        logger.info(f"COPYING {rows_to_copy} rows to Datastore...")

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
        raise utils.JobError(f"Could not connect to the Datastore: {e}")
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
            logger.warning(f"Could not TRUNCATE: {e}")

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
                raise utils.JobError(f"Postgres COPY failed: {e}")
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
        f'...copying done. Copied {copied_count} rows to "{resource_id}" in {copy_elapsed:,.2f} seconds.'
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
        logger.info(f"AUTO-ALIASING. Auto-alias-unique: {conf.AUTO_ALIAS_UNIQUE} ...")
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
                    f'Dropping existing alias "{alias}" for resource "{existing_alias_of}"...'
                )
                try:
                    cur.execute(
                        sql.SQL("DROP VIEW IF EXISTS {}").format(sql.Identifier(alias))
                    )
                except psycopg2.Error as e:
                    logger.warning(f"Could not drop alias/view: {e}")

        else:
            logger.warning(
                f"Cannot create alias: {resource_name}-{package_name}-{owner_org}"
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
            logger.info(f'Deleting existing summary stats "{stats_resource_id}".')

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
                    f'Deleting existing alias summary stats "{auto_alias_stats_id}".'
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
            qsv_stats_stats = qsv.stats(
                qsv_stats_csv,
                typesonly=True,
            )
        except utils.JobError as e:
            raise utils.JobError(f"Cannot run stats on CSV stats: {e}")

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
            f'ADDING SUMMARY STATISTICS {col_names_list} in "{new_stats_resource_id}" with alias/es "{stats_aliases}"...'
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
                raise utils.JobError(f"Postgres COPY failed: {e}")

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
        logger.info(f'Created alias "{alias}" for "{resource_id}"...')

    metadata_elapsed = time.perf_counter() - metadata_start
    logger.info(
        f"METADATA UPDATES DONE! Resource metadata updated in {metadata_elapsed:,.2f} seconds."
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
            f"AUTO-INDEXING. Auto-index threshold: {conf.AUTO_INDEX_THRESHOLD} unique value/s. Auto-unique index: {conf.AUTO_UNIQUE_INDEX} Auto-index dates: {conf.AUTO_INDEX_DATES} ..."
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
                        f'Creating UNIQUE index on "{curr_col}" for {unique_value_count} unique values...'
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
                            f'Could not CREATE UNIQUE INDEX on "{curr_col}": {e}'
                        )
                    index_count += 1
                elif cardinality <= conf.AUTO_INDEX_THRESHOLD or (
                    conf.AUTO_INDEX_DATES and (curr_col in datetimecols_list)
                ):
                    # cardinality <= auto_index_threshold or its a date and auto_index_date is true
                    # create an index
                    if curr_col in datetimecols_list:
                        logger.info(
                            f'Creating index on "{curr_col}" date column for {cardinality} unique value/s...'
                        )
                    else:
                        logger.info(
                            f'Creating index on "{curr_col}" for {cardinality} unique value/s...'
                        )
                    try:
                        index_cur.execute(
                            sql.SQL("CREATE INDEX ON {} ({})").format(
                                sql.Identifier(resource_id),
                                sql.Identifier(curr_col),
                            )
                        )
                    except psycopg2.Error as e:
                        logger.warning(f'Could not CREATE INDEX on "{curr_col}": {e}')
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
            f'...indexing/vacuum analysis done. Indexed {index_count} column/s in "{resource_id}" in {index_elapsed:,.2f} seconds.'
        )

    raw_connection.close()

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

    # Clear all lru_cache before processing formulae
    dsu.datastore_info.cache_clear()
    dsu.index_exists.cache_clear()
    dsu.datastore_search.cache_clear()
    dsu.datastore_search_sql.cache_clear()
    j2h.get_column_stats.cache_clear()
    j2h.get_frequency_top_values.cache_clear()

    # Fetch the scheming_yaml and package
    package_id = resource["package_id"]
    scheming_yaml, package = dsu.get_scheming_yaml(
        package_id, scheming_yaml_type="dataset"
    )

    # check if package dpp_suggestions field does not exist
    # and there are "suggestion_formula" keys in the scheming_yaml
    if "dpp_suggestions" not in package:
        # Check for suggestion_formula in dataset_fields
        has_suggestion_formula = any(
            isinstance(field, dict)
            and any(key.startswith("suggestion_formula") for key in field.keys())
            for field in scheming_yaml["dataset_fields"]
        )

        if not has_suggestion_formula:
            logger.error(
                '"dpp_suggestions" field required but not found in package to process Suggestion Formulae. Ensure that your scheming.yaml file contains the "dpp_suggestions" field as a json_object.'
            )
            return

    logger.trace(f"package: {package}")

    # FIRST, INITIALIZE THE FORMULA PROCESSOR
    formula_processor = j2h.FormulaProcessor(
        scheming_yaml,
        package,
        resource,
        resource_fields_stats,
        resource_fields_freqs,
        dataset_stats,
        logger,
    )

    # SECOND, WE PROCESS THE FORMULAE THAT UPDATE THE
    # PACKAGE AND RESOURCE FIELDS DIRECTLY
    # using the package_patch CKAN API so we only update the fields
    # with formulae
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
    # as this is a direct update, we update the resource dictionary directly
    resource_updates = formula_processor.process_formulae(
        "resource", "resource_fields", "formula"
    )
    if resource_updates:
        # Update resource with formula results
        resource.update(resource_updates)
        logger.info("RESOURCE formulae processed...")

    # THIRD, WE PROCESS THE SUGGESTIONS THAT SHOW UP IN THE SUGGESTION POPOVER
    # we update the package dpp_suggestions field
    # from which the Suggestion popover UI will pick it up
    package_suggestions = formula_processor.process_formulae(
        "package", "dataset_fields", "suggestion_formula"
    )
    if package_suggestions:
        logger.trace(f"package_suggestions: {package_suggestions}")
        revise_update_content = {"package": package_suggestions}
        try:
            revised_package = dsu.revise_package(
                package_id, update={"dpp_suggestions": revise_update_content}
            )
            logger.trace(f"Package after revising: {revised_package}")
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
        logger.trace(f"resource_suggestions: {resource_suggestions}")
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
            logger.trace(f"Package after revising: {revised_package}")
            package = revised_package
            logger.info("RESOURCE suggestion formulae processed...")
        except Exception as e:
            logger.error(f"Error revising package: {str(e)}")

    # -------------------- FORMULAE PROCESSING DONE --------------------
    formulae_elapsed = time.perf_counter() - formulae_start
    logger.info(
        f"FORMULAE PROCESSING DONE! Processed in {formulae_elapsed:,.2f} seconds."
    )

    total_elapsed = time.perf_counter() - timer_start
    newline_var = "\n"
    end_msg = f"""
    DATAPUSHER+ JOB DONE!
      Download: {fetch_elapsed:,.2f}
      Analysis: {analysis_elapsed:,.2f}{(newline_var + f"  PII Screening: {piiscreening_elapsed:,.2f}") if piiscreening_elapsed > 0 else ""}
      COPYing: {copy_elapsed:,.2f}
      Metadata updates: {metadata_elapsed:,.2f}
      Indexing: {index_elapsed:,.2f}
      Formulae processing: {formulae_elapsed:,.2f}
    TOTAL ELAPSED TIME: {total_elapsed:,.2f}
    """
    logger.info(end_msg)
