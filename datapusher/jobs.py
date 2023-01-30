# -*- coding: utf-8 -*-

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
from pathlib import Path
from datasize import DataSize
from psycopg2 import sql

import ckanserviceprovider.job as job
import ckanserviceprovider.util as util
from ckanserviceprovider import web
from datapusher.config import config


if locale.getdefaultlocale()[0]:
    lang, encoding = locale.getdefaultlocale()
    locale.setlocale(locale.LC_ALL, locale=(lang, encoding))
else:
    locale.setlocale(locale.LC_ALL, "")

USE_PROXY = "DOWNLOAD_PROXY" in config
if USE_PROXY:
    DOWNLOAD_PROXY = config.get("DOWNLOAD_PROXY")

if not config.get("SSL_VERIFY"):
    requests.packages.urllib3.disable_warnings()

POSTGRES_INT_MAX = 2147483647
POSTGRES_INT_MIN = -2147483648
POSTGRES_BIGINT_MAX = 9223372036854775807
POSTGRES_BIGINT_MIN = -9223372036854775808

DATASTORE_URLS = {
    "datastore_delete": "{ckan_url}/api/action/datastore_delete",
    "resource_update": "{ckan_url}/api/action/resource_update",
}


class HTTPError(util.JobError):
    """Exception that's raised if a job fails due to an HTTP problem."""

    def __init__(self, message, status_code, request_url, response):
        """Initialise a new HTTPError.

        :param message: A human-readable error message
        :type message: string

        :param status_code: The status code of the errored HTTP response,
            e.g. 500
        :type status_code: int

        :param request_url: The URL that was requested
        :type request_url: string

        :param response: The body of the errored HTTP response as unicode
            (if you have a requests.Response object then response.text will
            give you this)
        :type response: unicode

        """
        super(HTTPError, self).__init__(message)
        self.status_code = status_code
        self.request_url = request_url
        self.response = response

    def as_dict(self):
        """Return a JSON-serializable dictionary representation of this error.

        Suitable for ckanserviceprovider to return to the client site as the
        value for the "error" key in the job dict.

        """
        if self.response and len(self.response) > 200:
            response = self.response[:200]
        else:
            response = self.response
        return {
            "message": self.message,
            "HTTP status code": self.status_code,
            "Requested URL": self.request_url,
            "Response": response,
        }

    def __str__(self):
        return "{} status={} url={} response={}".format(
            self.message, self.status_code, self.request_url, self.response
        ).encode("ascii", "replace")


def get_url(action, ckan_url):
    """
    Get url for ckan action
    """
    if not urlsplit(ckan_url).scheme:
        ckan_url = "http://" + ckan_url.lstrip("/")  # DevSkim: ignore DS137138
    ckan_url = ckan_url.rstrip("/")
    return "{ckan_url}/api/3/action/{action}".format(ckan_url=ckan_url, action=action)


def check_response(
    response, request_url, who, good_status=(201, 200), ignore_no_success=False
):
    """
    Checks the response and raises exceptions if something went terribly wrong

    :param who: A short name that indicated where the error occurred
                (for example "CKAN")
    :param good_status: Status codes that should not raise an exception

    """
    if not response.status_code:
        raise HTTPError(
            "DataPusher received an HTTP response with no status code",
            status_code=None,
            request_url=request_url,
            response=response.text,
        )

    message = "{who} bad response. Status code: {code} {reason}. At: {url}."
    try:
        if response.status_code not in good_status:
            json_response = response.json()
            if not ignore_no_success or json_response.get("success"):
                try:
                    message = json_response["error"]["message"]
                except Exception:
                    message = message.format(
                        who=who,
                        code=response.status_code,
                        reason=response.reason,
                        url=request_url,
                    )
                raise HTTPError(
                    message,
                    status_code=response.status_code,
                    request_url=request_url,
                    response=response.text,
                )
    except ValueError:
        message = message.format(
            who=who,
            code=response.status_code,
            reason=response.reason,
            url=request_url,
            resp=response.text[:200],
        )
        raise HTTPError(
            message,
            status_code=response.status_code,
            request_url=request_url,
            response=response.text,
        )


class DatastoreEncoder(json.JSONEncoder):
    # Custom JSON encoder
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        if isinstance(obj, decimal.Decimal):
            return str(obj)

        return json.JSONEncoder.default(self, obj)


def delete_datastore_resource(resource_id, api_key, ckan_url):
    try:
        delete_url = get_url("datastore_delete", ckan_url)
        response = requests.post(
            delete_url,
            verify=config.get("SSL_VERIFY"),
            data=json.dumps({"id": resource_id, "force": True}),
            headers={"Content-Type": "application/json", "Authorization": api_key},
        )
        check_response(
            response,
            delete_url,
            "CKAN",
            good_status=(201, 200, 404),
            ignore_no_success=True,
        )
    except requests.exceptions.RequestException:
        raise util.JobError("Deleting existing datastore failed.")


def delete_resource(resource_id, api_key, ckan_url):
    try:
        delete_url = get_url("resource_delete", ckan_url)
        response = requests.post(
            delete_url,
            verify=config.get("SSL_VERIFY"),
            data=json.dumps({"id": resource_id, "force": True}),
            headers={"Content-Type": "application/json", "Authorization": api_key},
        )
        check_response(
            response,
            delete_url,
            "CKAN",
            good_status=(201, 200, 404),
            ignore_no_success=True,
        )
    except requests.exceptions.RequestException:
        raise util.JobError("Deleting existing resource failed.")


def datastore_resource_exists(resource_id, api_key, ckan_url):
    try:
        search_url = get_url("datastore_search", ckan_url)
        response = requests.post(
            search_url,
            verify=config.get("SSL_VERIFY"),
            data=json.dumps({"id": resource_id, "limit": 0}),
            headers={"Content-Type": "application/json", "Authorization": api_key},
        )
        if response.status_code == 404:
            return False
        elif response.status_code == 200:
            return response.json().get("result", {"fields": []})
        else:
            raise HTTPError(
                "Error getting datastore resource.",
                response.status_code,
                search_url,
                response,
            )
    except requests.exceptions.RequestException as e:
        raise util.JobError("Error getting datastore resource ({!s}).".format(e))


def send_resource_to_datastore(
    resource,
    resource_id,
    headers,
    api_key,
    ckan_url,
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

    url = get_url("datastore_create", ckan_url)
    r = requests.post(
        url,
        verify=config.get("SSL_VERIFY"),
        data=json.dumps(request, cls=DatastoreEncoder),
        headers={"Content-Type": "application/json", "Authorization": api_key},
    )
    check_response(r, url, "CKAN DataStore")
    return r.json()


def update_resource(resource, ckan_url, api_key):
    url = get_url("resource_update", ckan_url)
    r = requests.post(
        url,
        verify=config.get("SSL_VERIFY"),
        data=json.dumps(resource),
        headers={"Content-Type": "application/json", "Authorization": api_key},
    )

    check_response(r, url, "CKAN")


def get_resource(resource_id, ckan_url, api_key):
    """
    Gets available information about the resource from CKAN
    """
    url = get_url("resource_show", ckan_url)
    r = requests.post(
        url,
        verify=config.get("SSL_VERIFY"),
        data=json.dumps({"id": resource_id}),
        headers={"Content-Type": "application/json", "Authorization": api_key},
    )
    check_response(r, url, "CKAN")

    return r.json()["result"]


def get_package(package_id, ckan_url, api_key):
    """
    Gets available information about a package from CKAN
    """
    url = get_url("package_show", ckan_url)
    r = requests.post(
        url,
        verify=config.get("SSL_VERIFY"),
        data=json.dumps({"id": package_id}),
        headers={"Content-Type": "application/json", "Authorization": api_key},
    )
    check_response(r, url, "CKAN")

    return r.json()["result"]


def validate_input(input):
    # Especially validate metadata which is provided by the user
    if "metadata" not in input:
        raise util.JobError("Metadata missing")

    data = input["metadata"]

    if "resource_id" not in data:
        raise util.JobError("No id provided.")
    if "ckan_url" not in data:
        raise util.JobError("No ckan_url provided.")
    if not input.get("api_key"):
        raise util.JobError("No CKAN API key provided")


@job.asynchronous
def push_to_datastore(task_id, input, dry_run=False):
    """Download and parse a resource push its data into CKAN's DataStore.

    An asynchronous job that gets a resource from CKAN, downloads the
    resource's data file and, if the data file has changed since last time,
    parses the data and posts it into CKAN's DataStore.

    :param dry_run: Fetch and parse the data file but don't actually post the
        data to the DataStore, instead return the data headers and rows that
        would have been posted.
    :type dry_run: boolean

    """
    handler = util.StoringHandler(task_id, input)
    logger = logging.getLogger(task_id)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    # check if QSV_BIN and FILE_BIN exists
    qsv_bin = config.get("QSV_BIN")
    qsv_path = Path(qsv_bin)
    if not qsv_path.is_file():
        raise util.JobError("{} not found.".format(qsv_bin))
    file_bin = config.get("FILE_BIN")

    file_path = Path(file_bin)
    if not file_path.is_file():
        raise util.JobError("{} not found.".format(file_bin))

    # make sure qsv binary variant is up-to-date
    try:
        qsv_version = subprocess.run(
            [qsv_bin, "--version"],
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as e:
        raise util.JobError("qsv version check error: {}".format(e))
    qsv_version_info = str(qsv_version.stdout)
    qsv_semver = qsv_version_info[
        qsv_version_info.find(" ") : qsv_version_info.find("-")
    ].lstrip()
    try:
        if semver.compare(qsv_semver, "0.85.0") < 0:
            raise util.JobError(
                "At least qsv version 0.85.0 required. Found {}. You can get the latest release at https://github.com/jqnatividad/qsv/releases/latest".format(
                    qsv_version_info
                )
            )
    except ValueError as e:
        raise util.JobError("Cannot parse qsv version info: {}".format(e))

    validate_input(input)

    data = input["metadata"]

    ckan_url = data["ckan_url"]
    resource_id = data["resource_id"]
    api_key = input.get("api_key")

    try:
        resource = get_resource(resource_id, ckan_url, api_key)
    except util.JobError:
        # try again in 5 seconds just incase CKAN is slow at adding resource
        time.sleep(5)
        resource = get_resource(resource_id, ckan_url, api_key)

    # check if the resource url_type is a datastore
    if resource.get("url_type") == "datastore":
        logger.info("Dump files are managed with the Datastore API")
        return

    # check scheme
    url = resource.get("url")
    scheme = urlsplit(url).scheme
    if scheme not in ("http", "https", "ftp"):
        raise util.JobError("Only http, https, and ftp resources may be fetched.")

    # ==========================================================================
    # DOWNLOAD
    # ==========================================================================
    timer_start = time.perf_counter()

    # fetch the resource data
    logger.info("Fetching from: {0}...".format(url))
    headers = {}
    preview_rows = int(config.get("PREVIEW_ROWS"))
    download_preview_only = config.get("DOWNLOAD_PREVIEW_ONLY")
    if resource.get("url_type") == "upload":
        # If this is an uploaded file to CKAN, authenticate the request,
        # otherwise we won't get file from private resources
        headers["Authorization"] = api_key
    try:
        kwargs = {
            "headers": headers,
            "timeout": config.get("DOWNLOAD_TIMEOUT"),
            "verify": config.get("SSL_VERIFY"),
            "stream": True,
        }
        if USE_PROXY:
            kwargs["proxies"] = {"http": DOWNLOAD_PROXY, "https": DOWNLOAD_PROXY}
        response = requests.get(url, **kwargs)
        response.raise_for_status()

        cl = response.headers.get("content-length")
        max_content_length = int(config.get("MAX_CONTENT_LENGTH"))
        try:
            if (
                cl
                and int(cl) > max_content_length
                and (preview_rows > 0 and not download_preview_only)
            ):
                raise util.JobError(
                    "Resource too large to download: {cl:.2MB} > max ({max_cl:.2MB}).".format(
                        cl=DataSize(int(cl)), max_cl=DataSize(int(max_content_length))
                    )
                )
        except ValueError:
            pass

        tmp = tempfile.NamedTemporaryFile(suffix="." + resource.get("format").lower())
        length = 0
        m = hashlib.md5()

        if download_preview_only and preview_rows > 0:
            # if download_preview_only and preview_rows is greater than zero
            # we're downloading the preview rows only, not the whole file
            logger.info(
                "Downloading only first {:,} row preview from {:.2MB} file...".format(
                    preview_rows, DataSize(int(cl))
                )
            )

            curr_line = 0
            for line in response.iter_lines(int(config.get("CHUNK_SIZE"))):
                curr_line += 1
                # add back the linefeed as iter_lines removes it
                line = line + "\n".encode("ascii")
                length += len(line)

                tmp.write(line)
                m.update(line)
                if curr_line > preview_rows:
                    break
        else:
            # download the entire file otherwise
            # TODO: handle when preview_rows is negative (get the preview from the
            # end of the file) so we can use http range request to get the preview from
            # the end without downloading the entire file
            logger.info("Downloading {:.2MB} file...".format(DataSize(int(cl))))

            for chunk in response.iter_content(int(config.get("CHUNK_SIZE"))):
                length += len(chunk)
                if length > max_content_length and not preview_rows:
                    raise util.JobError(
                        "Resource too large to process: {cl} > max ({max_cl}).".format(
                            cl=length, max_cl=max_content_length
                        )
                    )
                tmp.write(chunk)
                m.update(chunk)

        ct = response.headers.get("content-type", "").split(";", 1)[0]
    except requests.HTTPError as e:
        raise HTTPError(
            "DataPusher+ received a bad HTTP response when trying to download "
            "the data file",
            status_code=e.response.status_code,
            request_url=url,
            response=e.response.content,
        )
    except requests.RequestException as e:
        raise HTTPError(
            message=str(e), status_code=None, request_url=url, response=None
        )

    file_hash = m.hexdigest()
    tmp.seek(0)

    if (
        resource.get("hash") == file_hash
        and not data.get("ignore_hash")
        and not config.get("IGNORE_FILE_HASH")
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
        default_excel_sheet = config.get("DEFAULT_EXCEL_SHEET")
        logger.info(
            "Converting {} sheet {} to CSV...".format(format, default_excel_sheet)
        )
        # first, we need a temporary spreadsheet filename with the right file extension
        # we only need the filename though, that's why we remove it
        # and create a hardlink to the file we got from CKAN
        qsv_spreadsheet = tempfile.NamedTemporaryFile(suffix="." + format)
        os.remove(qsv_spreadsheet.name)
        os.link(tmp.name, qsv_spreadsheet.name)

        # run `qsv excel` and export it to a CSV
        # use --trim option to trim column names and the data
        qsv_excel_csv = tempfile.NamedTemporaryFile(suffix=".csv")
        try:
            qsv_excel = subprocess.run(
                [
                    qsv_bin,
                    "excel",
                    qsv_spreadsheet.name,
                    "--sheet",
                    str(default_excel_sheet),
                    "--trim",
                    "--output",
                    qsv_excel_csv.name,
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
                [file_bin, qsv_spreadsheet.name],
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
        qsv_spreadsheet.close()
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
        qsv_input_csv = tempfile.NamedTemporaryFile(suffix=".csv")
        if format.upper() == "CSV":
            logger.info("Normalizing/UTF-8 transcoding {}...".format(format))
        else:
            logger.info("Normalizing/UTF-8 transcoding {} to CSV...".format(format))
        try:
            qsv_input = subprocess.run(
                [
                    qsv_bin,
                    "input",
                    tmp.name,
                    "--trim-headers",
                    "--output",
                    qsv_input_csv.name,
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
        qsv_validate = subprocess.run(
            [qsv_bin, "validate", tmp.name], check=True, capture_output=True, text=True
        )
    except subprocess.CalledProcessError as e:
        # return as we can't push an invalid CSV file
        validate_error_msg = qsv_validate.stderr
        logger.error("Invalid CSV! Job aborted: {}.".format(validate_error_msg))
        return
    logger.info("Well-formed, valid CSV file confirmed...")

    # --------------------- Sortcheck --------------------------
    # if SORT_AND_DUPE_CHECK is True or DEDUP is True
    # check if the file is sorted and if it has duplicates
    # get the record count, unsorted breaks and duplicate count as well
    sort_and_dupe_check = config.get("SORT_AND_DUPE_CHECK")
    dedup = config.get("DEDUP")

    if sort_and_dupe_check or dedup:
        logger.info("Checking for duplicates and if the CSV is sorted...")
        try:
            qsv_sortcheck = subprocess.run(
                [qsv_bin, "sortcheck", tmp.name, "--json"],
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as e:
            raise util.JobError("Sortcheck error: {}".format(e))
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
    # note that deduping also ends up creating a sorted CSV
    if dedup and dupe_count > 0:
        qsv_dedup_csv = tempfile.NamedTemporaryFile(suffix=".csv")
        logger.info("{:.} duplicate rows found. Deduping...".format(dupe_count))
        qsv_dedup_cmd = [qsv_bin, "dedup", tmp.name, "--output", qsv_dedup_csv.name]

        # if the file is already sorted,
        # we can save a lot of time by passing the --sorted flag
        # we also get to "stream" the file and not load it into memory,
        # as we don't need to sort it first
        if is_sorted:
            qsv_dedup_cmd.append("--sorted")
        try:
            qsv_dedup = subprocess.run(
                qsv_dedup_cmd,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as e:
            raise util.JobError("Check for duplicates error: {}".format(e))
        dupe_count = int(str(qsv_dedup.stderr).strip())
        if dupe_count > 0:
            tmp = qsv_dedup_csv
            logger.warning(
                "{:,} duplicates found and removed. Note that deduping results in a sorted CSV.".format(
                    dupe_count
                )
            )
    elif dupe_count > 0:
        logger.warning("{:,} duplicates found but not deduping...".format(dupe_count))

    # ----------------------- Headers & Safenames ---------------------------
    # get existing header names, so we can use them for data dictionary labels
    # should we need to change the column name to make it "db-safe"
    try:
        qsv_headers = subprocess.run(
            [qsv_bin, "headers", "--just-names", tmp.name],
            capture_output=True,
            check=True,
            text=True,
        )
    except subprocess.CalledProcessError as e:
        raise util.JobError("Cannot scan CSV headers: {}".format(e))
    original_headers = str(qsv_headers.stdout).strip()
    original_header_dict = {
        idx: ele for idx, ele in enumerate(original_headers.splitlines())
    }

    # now, ensure our column/header names identifiers are "safe names"
    # i.e. valid postgres/CKAN Datastore identifiers
    qsv_safenames_csv = tempfile.NamedTemporaryFile(suffix=".csv")
    logger.info('Checking for "database-safe" header names...')
    try:
        qsv_safenames = subprocess.run(
            [
                qsv_bin,
                "safenames",
                tmp.name,
                "--mode",
                "json",
            ],
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as e:
        raise util.JobError("Safenames error: {}".format(e))

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
                qsv_bin,
                "safenames",
                tmp.name,
                "--mode",
                "conditional",
                "--output",
                qsv_safenames_csv.name,
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
        qsv_index_file = tmp.name + ".idx"
        subprocess.run([qsv_bin, "index", tmp.name], check=True)
    except subprocess.CalledProcessError as e:
        raise util.JobError("Cannot index CSV: {}".format(e))

    # if SORT_AND_DUPE_CHECK = True, we already know the record count
    # so we can skip qsv count.
    if not sort_and_dupe_check:
        # get record count, this is instantaneous with an index
        try:
            qsv_count = subprocess.run(
                [qsv_bin, "count", tmp.name], capture_output=True, check=True, text=True
            )
        except subprocess.CalledProcessError as e:
            raise util.JobError("Cannot count records in CSV: {}".format(e))
        record_count = int(str(qsv_count.stdout).strip())

    # its empty, nothing to do
    if record_count == 0:
        logger.warning("Upload skipped as there are zero records.")
        return

    # log how many records we detected
    unique_qualifier = ""
    if dedup:
        unique_qualifier = "unique"
    logger.info("{:,} {} records detected...".format(record_count, unique_qualifier))

    # run qsv stats to get data types and summary statistics
    logger.info("Inferring data types and compiling statistics...")
    headers = []
    types = []
    headers_min = []
    headers_max = []
    headers_cardinality = []
    qsv_stats_csv = tempfile.NamedTemporaryFile(suffix=".csv")
    qsv_stats_cmd = [
        qsv_bin,
        "stats",
        tmp.name,
        "--infer-dates",
        "--dates-whitelist",
        "all",
        "--output",
        qsv_stats_csv.name,
    ]
    prefer_dmy = config.get("PREFER_DMY")
    if prefer_dmy:
        qsv_stats_cmd.append("--prefer_dmy")
    auto_index_threshold = config.get("AUTO_INDEX_THRESHOLD")
    if auto_index_threshold:
        qsv_stats_cmd.append("--cardinality")
    summary_stats_options = config.get("SUMMARY_STATS_OPTIONS")
    if summary_stats_options:
        qsv_stats_cmd.append(summary_stats_options)

    try:
        qsv_stats = subprocess.run(qsv_stats_cmd, check=True)
    except subprocess.CalledProcessError as e:
        raise util.JobError(
            "Cannot infer data types and compile statistics: {}".format(e)
        )

    with open(qsv_stats_csv.name, mode="r") as inp:
        reader = csv.DictReader(inp)
        for row in reader:
            headers.append(row["field"])
            types.append(row["type"])
            headers_min.append(row["min"])
            headers_max.append(row["max"])
            if auto_index_threshold:
                headers_cardinality.append(int(row["cardinality"]))

    existing = datastore_resource_exists(resource_id, api_key, ckan_url)
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
        delete_datastore_resource(resource_id, api_key, ckan_url)

    # 1st pass of building headers_dict
    # here we map inferred types to postgresql data types
    type_mapping = config.get("TYPE_MAPPING")
    temp_headers_dicts = [
        dict(id=field[0], type=type_mapping[str(field[1])])
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
        info_dict = dict(label=original_header_dict[idx])
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
                if type_override in list(type_mapping.values()):
                    h["type"] = type_override

    logger.info(
        "Determined headers and types: {headers}...".format(headers=headers_dicts)
    )

    # ------------------- Do we need to create a Preview?  -----------------------
    # if PREVIEW_ROWS is not zero, create a preview using qsv slice
    # we do the rows_to_copy > preview_rows to check if we don't need to slice
    # the CSV anymore if we only did a partial download of N preview_rows already
    rows_to_copy = record_count
    if preview_rows and record_count > preview_rows:
        if preview_rows > 0:
            # PREVIEW_ROWS is positive, slice from the beginning
            logger.info("Preparing {:,}-row preview...".format(preview_rows))
            qsv_slice_csv = tempfile.NamedTemporaryFile(suffix=".csv")
            try:
                qsv_slice = subprocess.run(
                    [
                        qsv_bin,
                        "slice",
                        "--len",
                        str(preview_rows),
                        tmp.name,
                        "--output",
                        qsv_slice_csv.name,
                    ],
                    check=True,
                )
            except subprocess.CalledProcessError as e:
                raise util.JobError("Cannot create a preview slice: {}".format(e))
            rows_to_copy = preview_rows
            tmp = qsv_slice_csv
        else:
            # PREVIEW_ROWS is negative, slice from the end
            # TODO: do http range request so we don't have to download the whole file
            # to slice from the end
            slice_len = abs(preview_rows)
            logger.info("Preparing {:,}-row preview from the end...".format(slice_len))
            qsv_slice_csv = tempfile.NamedTemporaryFile(suffix=".csv")
            try:
                qsv_slice = subprocess.run(
                    [
                        qsv_bin,
                        "slice",
                        "--start",
                        "-1",
                        "--len",
                        str(slice_len),
                        tmp.name,
                        "--output",
                        qsv_slice_csv.name,
                    ],
                    check=True,
                )
            except subprocess.CalledProcessError as e:
                raise util.JobError(
                    "Cannot create a preview slice from the end: {}".format(e)
                )
            rows_to_copy = slice_len
            tmp = qsv_slice_csv

    # ---------------- Normalize dates to RFC3339 format --------------------
    # if there are any datetime fields, normalize them to RFC3339 format
    # so we can readily insert them as timestamps into postgresql with COPY
    if datetimecols_list:
        qsv_applydp_csv = tempfile.NamedTemporaryFile(suffix=".csv")
        datecols = ",".join(datetimecols_list)

        qsv_applydp_cmd = [
            qsv_bin,
            "applydp",
            "datefmt",
            datecols,
            tmp.name,
            "--output",
            qsv_applydp_csv.name,
        ]
        if prefer_dmy:
            qsv_applydp_cmd.append("--prefer_dmy")
        logger.info(
            'Formatting dates "{}" to ISO 8601/RFC 3339 format with PREFER_DMY: {}...'.format(
                datecols, prefer_dmy
            )
        )
        try:
            qsv_applydp = subprocess.run(qsv_applydp_cmd, check=True)
        except subprocess.CalledProcessError as e:
            raise util.JobError("Applydp error: {}".format(e))
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
    if config.get("PII_SCREENING"):
        piiscreening_start = time.perf_counter()
        pii_found_abort = config.get("PII_FOUND_ABORT")

        # DP+ comes with default regex patterns for PII (SSN, credit cards,
        # email, bank account numbers, & phone number). The DP+ admin can
        # use a custom set of regex patterns by pointing to a resource with
        # a text file, with each line having a regex pattern, and an optional
        # label comment prefixed with "#" (e.g. #SSN, #Email, #Visa, etc.)
        pii_regex_resource_id = config.get("PII_REGEX_RESOURCE_ID_OR_ALIAS")
        if pii_regex_resource_id:
            pii_regex_resource_exist = datastore_resource_exists(
                pii_regex_resource_id, api_key, ckan_url
            )
            if pii_regex_resource_exist:
                pii_resource = get_resource(pii_regex_resource_id, ckan_url, api_key)
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

        pii_quick_screen = config.get("PII_QUICK_SCREEN")
        if pii_quick_screen:
            logger.info("Quickly scanning for PII using {}...".format(pii_regex_file))
            try:
                qsv_searchset = subprocess.run(
                    [
                        qsv_bin,
                        "searchset",
                        "--ignore-case",
                        "--quick",
                        pii_regex_fname,
                        tmp.name,
                    ],
                    capture_output=True,
                    text=True,
                )
            except subprocess.CalledProcessError as e:
                raise util.JobError("Cannot quickly search CSV for PII: {}".format(e))
            pii_candidate_row = str(qsv_searchset.stderr)
            if pii_candidate_row:
                pii_found = True

        else:
            logger.info("Scanning for PII using {}...".format(pii_regex_file))
            qsv_searchset_csv = tempfile.NamedTemporaryFile(suffix=".csv")
            try:
                qsv_searchset = subprocess.run(
                    [
                        qsv_bin,
                        "searchset",
                        "--ignore-case",
                        "--flag",
                        "PII_info",
                        "--flag-matches-only",
                        "--json",
                        pii_regex_file,
                        tmp.name,
                        "--output",
                        qsv_searchset_csv.name,
                    ],
                    capture_output=True,
                    text=True,
                )
            except subprocess.CalledProcessError as e:
                raise util.JobError("Cannot search CSV for PII: {}".format(e))
            pii_json = json.loads(str(qsv_searchset.stderr))
            pii_total_matches = int(pii_json["total_matches"])
            pii_rows_with_matches = int(pii_json["rows_with_matches"])
            if pii_total_matches > 0:
                pii_found = True

        pii_show_candidates = config.get("PII_SHOW_CANDIDATES")
        if pii_found and pii_found_abort and not pii_show_candidates:
            logger.error("PII Candidate/s Found!")
            if pii_quick_screen:
                raise util.JobError(
                    "PII CANDIDATE FOUND on row {}! Job aborted.".format(
                        pii_candidate_row.rstrip()
                    )
                )
            else:
                raise util.JobError(
                    "PII CANDIDATE/S FOUND! Job aborted. Found {} PII candidate/s in {} row/s.".format(
                        pii_total_matches, pii_rows_with_matches
                    )
                )
        elif pii_found and pii_show_candidates and not pii_quick_screen:
            # TODO: Create PII Candidates resource and set package to private if its not private
            # ------------ Create PII Preview Resource ------------------
            logger.warning(
                "PII CANDIDATE/S FOUND! Found {} PII candidate/s in {} row/s. Creating PII preview...".format(
                    pii_total_matches, pii_rows_with_matches
                )
            )
            pii_resource_id = resource_id + "-pii"

            try:
                raw_connection_pii = psycopg2.connect(config.get("WRITE_ENGINE_URL"))
            except psycopg2.Error as e:
                raise util.JobError("Could not connect to the Datastore: {}".format(e))
            else:
                cur_pii = raw_connection_pii.cursor()

            # check if the pii already exist
            existing_pii = datastore_resource_exists(pii_resource_id, api_key, ckan_url)

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

                    delete_datastore_resource(existing_pii_alias_of, api_key, ckan_url)
                    delete_resource(existing_pii_alias_of, api_key, ckan_url)

            pii_alias = [pii_resource_id]

            # run stats on pii preview CSV to get header names and infer data types
            # we don't need summary statistics, so use the --typesonly option
            try:
                qsv_pii_stats = subprocess.run(
                    [
                        qsv_bin,
                        "stats",
                        "--typesonly",
                        qsv_searchset_csv.name,
                    ],
                    capture_output=True,
                    check=True,
                    text=True,
                )
            except subprocess.CalledProcessError as e:
                raise util.JobError("Cannot run stats on PII preview CSV: {}".format(e))

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
                api_key=api_key,
                ckan_url=ckan_url,
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

            with open(qsv_searchset_csv.name, "rb") as f:
                try:
                    cur_pii.copy_expert(copy_sql, f)
                except psycopg2.Error as e:
                    raise util.JobError("Postgres COPY failed: {}".format(e))
                else:
                    pii_copied_count = cur_pii.rowcount

            raw_connection_pii.commit()
            cur_pii.close()

            pii_resource["id"] = new_pii_resource_id
            pii_resource["pii_preview"] = True
            pii_resource["pii_of_resource"] = resource_id
            pii_resource["total_record_count"] = pii_rows_with_matches
            update_resource(pii_resource, ckan_url, api_key)

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
                raise util.JobError(pii_msg)
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

    if preview_rows:
        logger.info("COPYING {:,}-row preview to Datastore...".format(rows_to_copy))
    else:
        logger.info("COPYING {:,} rows to Datastore...".format(rows_to_copy))

    # first, let's create an empty datastore table w/ guessed types
    send_resource_to_datastore(
        resource=None,
        resource_id=resource["id"],
        headers=headers_dicts,
        api_key=api_key,
        ckan_url=ckan_url,
        records=None,
        aliases=None,
        calculate_record_count=False,
    )

    copied_count = 0
    try:
        raw_connection = psycopg2.connect(config.get("WRITE_ENGINE_URL"))
    except psycopg2.Error as e:
        raise util.JobError("Could not connect to the Datastore: {}".format(e))
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
        with open(tmp.name, "rb") as f:
            try:
                cur.copy_expert(copy_sql, f)
            except psycopg2.Error as e:
                raise util.JobError("Postgres COPY failed: {}".format(e))
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
    # UPDATE METADATA
    # ============================================================
    metadata_start = time.perf_counter()
    logger.info("UPDATING RESOURCE METADATA...")

    # --------------------- AUTO-ALIASING ------------------------
    # aliases are human-readable, and make it easier to use than resource id hash
    # when using the Datastore API and in SQL queries
    auto_alias = config.get("AUTO_ALIAS")
    auto_alias_unique = config.get("AUTO_ALIAS_UNIQUE")
    alias = None
    if auto_alias:
        logger.info(
            "AUTO-ALIASING. Auto-alias-unique: {} ...".format(auto_alias_unique)
        )
        # get package info, so we can construct the alias
        package = get_package(resource["package_id"], ckan_url, api_key)

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
            if auto_alias_unique and alias_count > 1:
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
    if (config.get("ADD_SUMMARY_STATS_RESOURCE") and not download_preview_only) or (
        download_preview_only and config.get("SUMMARY_STATS_WITH_PREVIEW")
    ):

        stats_resource_id = resource_id + "-stats"

        # check if the stats already exist
        existing_stats = datastore_resource_exists(stats_resource_id, api_key, ckan_url)
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

                delete_datastore_resource(existing_stats_alias_of, api_key, ckan_url)
                delete_resource(existing_stats_alias_of, api_key, ckan_url)

        stats_aliases = [stats_resource_id]
        if auto_alias:
            auto_alias_stats_id = alias + "-stats"
            stats_aliases.append(auto_alias_stats_id)

            # check if the summary-stats alias already exist. We need to do this as summary-stats resources
            # may end up having the same alias if AUTO_ALIAS_UNIQUE is False, so we need to drop the
            # existing summary stats-alias.
            existing_alias_stats = datastore_resource_exists(
                auto_alias_stats_id, api_key, ckan_url
            )
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

                    delete_datastore_resource(
                        existing_stats_alias_of, api_key, ckan_url
                    )
                    delete_resource(existing_stats_alias_of, api_key, ckan_url)

        # run stats on stats CSV to get header names and infer data types
        # we don't need summary statistics, so use the --typesonly option
        try:
            qsv_stats_stats = subprocess.run(
                [
                    qsv_bin,
                    "stats",
                    "--typesonly",
                    qsv_stats_csv.name,
                ],
                capture_output=True,
                check=True,
                text=True,
            )
        except subprocess.CalledProcessError as e:
            raise util.JobError("Cannot run stats on CSV stats: {}".format(e))

        stats_stats = str(qsv_stats_stats.stdout).strip()
        stats_stats_dict = [
            dict(id=ele.split(",")[0], type=type_mapping[ele.split(",")[1]])
            for idx, ele in enumerate(stats_stats.splitlines()[1:], 1)
        ]

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
            api_key=api_key,
            ckan_url=ckan_url,
            records=None,
            aliases=stats_aliases,
            calculate_record_count=False,
        )

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

        with open(qsv_stats_csv.name, "rb") as f:
            try:
                cur.copy_expert(copy_sql, f)
            except psycopg2.Error as e:
                raise util.JobError("Postgres COPY failed: {}".format(e))

        stats_resource["id"] = new_stats_resource_id
        stats_resource["summary_statistics"] = True
        stats_resource["summary_of_resource"] = resource_id
        update_resource(stats_resource, ckan_url, api_key)

    cur.close()
    raw_connection.commit()

    resource["datastore_active"] = True
    resource["total_record_count"] = record_count
    if preview_rows < record_count or (preview_rows > 0 and download_preview_only):
        resource["preview"] = True
        resource["preview_rows"] = copied_count
        resource["partial_download"] = download_preview_only
    update_resource(resource, ckan_url, api_key)

    # tell CKAN to calculate_record_count and set alias if set
    send_resource_to_datastore(
        resource=None,
        resource_id=resource["id"],
        headers=headers_dicts,
        api_key=api_key,
        ckan_url=ckan_url,
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
    auto_index_dates = config.get("AUTO_INDEX_DATES")
    if auto_index_threshold or (auto_index_dates and datetimecols_list):
        index_start = time.perf_counter()
        logger.info(
            "AUTO-INDEXING. Auto-index threshold: {:,} unique value/s. Auto-index dates: {} ...".format(
                auto_index_threshold, auto_index_dates
            )
        )
        index_cur = raw_connection.cursor()

        # if auto_index_threshold == -1
        # we index all the columns
        if auto_index_threshold == -1:
            auto_index_threshold = record_count

        index_count = 0
        for idx, cardinality in enumerate(headers_cardinality):

            curr_col = headers[idx]
            if auto_index_threshold > 0 or auto_index_dates:
                if cardinality == record_count:
                    # all the values are unique for this column, create a unique index
                    unique_value_count = min(preview_rows, cardinality)
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
                elif cardinality <= auto_index_threshold or (
                    auto_index_dates and (curr_col in datetimecols_list)
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
      Analysis: {analysis_elapsed:,.2f}{(newline_var + f"  PII Screening: {piiscreening_elapsed:,.2f}") if piiscreening_elapsed > 0 else ""}
      COPYing: {copy_elapsed:,.2f}
      Metadata updates: {metadata_elapsed:,.2f}
      Indexing: {index_elapsed:,.2f}
    TOTAL ELAPSED TIME: {total_elapsed:,.2f}
    """
    logger.info(end_msg)
