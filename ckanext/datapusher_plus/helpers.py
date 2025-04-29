# encoding: utf-8
# flake8: noqa: E501

from __future__ import annotations

import json
import zipfile
import csv
import logging
import datetime
from pathlib import Path
from typing import Any, Optional, Union

import ckan.plugins.toolkit as toolkit

from ckanext.datapusher_plus.model import Jobs, Metadata, Logs
import ckanext.datapusher_plus.job_exceptions as jex
import ckanext.datapusher_plus.config as conf

_ = toolkit._

logger = logging.getLogger(__name__)


def datapusher_status(resource_id: str):
    try:
        return toolkit.get_action("datapusher_status")({}, {"resource_id": resource_id})
    except toolkit.ObjectNotFound:
        return {"status": "unknown"}


def datapusher_status_description(status: dict[str, Any]):

    CAPTIONS = {
        "complete": _("Complete"),
        "pending": _("Pending"),
        "submitting": _("Submitting"),
        "error": _("Error"),
    }

    DEFAULT_STATUS = _("Not Uploaded Yet")

    try:
        job_status = status["task_info"]["status"]
        return CAPTIONS.get(job_status, job_status.capitalize())
    except (KeyError, TypeError):
        return DEFAULT_STATUS


def get_job(job_id, limit=None, use_aps_id=False):
    """Return the job with the given job_id as a dict.

    The dict also includes any metadata or logs associated with the job.

    Returns None instead of a dict if there's no job with the given job_id.

    The keys of a job dict are:

    "job_id": The unique identifier for the job (unicode)

    "job_type": The name of the job function that will be executed for this
        job (unicode)

    "status": The current status of the job, e.g. "pending", "complete", or
        "error" (unicode)

    "data": Any output data returned by the job if it has completed
        successfully. This may be any JSON-serializable type, e.g. None, a
        string, a dict, etc.

    "error": If the job failed with an error this will be a dict with a
        "message" key whose value is a string error message. The dict may also
        have other keys specific to the particular type of error. If the job
        did not fail with an error then "error" will be None.

    "requested_timestamp": The time at which the job was requested (string)

    "finished_timestamp": The time at which the job finished (string)

    "sent_data": The input data for the job, provided by the client site.
        This may be any JSON-serializable type, e.g. None, a string, a dict,
        etc.

    "result_url": The callback URL that CKAN Service Provider will post the
        result to when the job finishes (unicode)

    "api_key": The API key that CKAN Service Provider will use when posting
        the job result to the result_url (unicode or None). A None here doesn't
        mean that there was no API key: CKAN Service Provider deletes the API
        key from the database after it has posted the result to the result_url.

    "job_key": The key that users must provide (in the Authorization header of
        the HTTP request) to be authorized to modify the job (unicode).
        For example requests to the CKAN Service Provider API need this to get
        the status or output data of a job or to delete a job.
        If you login to CKAN Service Provider as an administrator then you can
        administer any job without providing its job_key.

    "metadata": Any custom metadata associated with the job (dict)

    "logs": Any logs associated with the job (list)

    """
    # Avoid SQLAlchemy "Unicode type received non-unicode bind param value"
    # warnings.
    if job_id:
        job_id = str(job_id)
    if use_aps_id:
        result = Jobs.get_by_aps_id(use_aps_id)
    else:
        result = Jobs.get(job_id)

    if not result:
        return None

    # Turn the result into a dictionary representation of the job.
    result_dict = {}
    for field in list(result.keys()):
        value = getattr(result, field)
        if value is None:
            result_dict[field] = value
        elif field in ("sent_data", "data", "error"):
            result_dict[field] = json.loads(value)
        elif isinstance(value, datetime.datetime):
            result_dict[field] = value.isoformat()
        else:
            result_dict[field] = str(value)

    result_dict["metadata"] = Metadata.get(job_id)
    result_dict["logs"] = Logs.get_with_limit(job_id, limit=limit)

    return result_dict


def add_pending_job(
    job_id, api_key, job_type, job_key=None, data=None, metadata=None, result_url=None
):
    """Add a new job with status "pending" to the jobs table.

    All code that adds jobs to the jobs table should go through this function.
    Code that adds to the jobs table manually should be refactored to use this
    function.

    May raise unspecified exceptions from Python core, SQLAlchemy or JSON!
    TODO: Document and unit test these!

    :param job_id: a unique identifier for the job, used as the primary key in
        ckanserviceprovider's "jobs" database table
    :type job_id: unicode

    :param job_key: the key required to administer the job via the API
    :type job_key: unicode

    :param job_type: the name of the job function that will be executed for
        this job
    :type job_key: unicode

    :param api_key: the client site API key that ckanserviceprovider will use
        when posting the job result to the result_url
    :type api_key: unicode

    :param data: The input data for the job (called sent_data elsewhere)
    :type data: Any JSON-serializable type

    :param metadata: A dict of arbitrary (key, value) metadata pairs to be
        stored along with the job. The keys should be strings, the values can
        be strings or any JSON-encodable type.
    :type metadata: dict

    :param result_url: the callback URL that ckanserviceprovider will post the
        job result to when the job has finished
    :type result_url: unicode

    """
    if not data:
        data = {}
    data = json.dumps(data)

    # Turn strings into unicode to stop SQLAlchemy
    # "Unicode type received non-unicode bind param value" warnings.
    if job_id:
        job_id = str(job_id)
    if job_type:
        job_type = str(job_type)
    if result_url:
        result_url = str(result_url)
    if api_key:
        api_key = str(api_key)
    if job_key:
        job_key = str(job_key)
    data = str(data)

    if not metadata:
        metadata = {}

    job = Jobs(
        job_id,
        job_type,
        "pending",
        data,
        None,
        None,
        None,
        None,
        None,
        result_url,
        api_key,
        job_key,
    )
    try:
        job.save()
    except Exception as e:
        raise e

    inserts = {}
    for key, value in metadata.items():
        type_ = "string"
        if not isinstance(value, str):
            value = json.dumps(value)
            type_ = "json"

        # Turn strings into unicode to stop SQLAlchemy
        # "Unicode type received non-unicode bind param value" warnings.
        key = str(key)
        value = str(value)

        inserts.update({"job_id": job_id, "key": key, "value": value, "type": type_})
        if inserts:
            md = Metadata(**inserts)
            try:
                md.save()
            except Exception as e:
                raise e


def validate_error(error):
    """Validate and return the given error object.

    Based on the given error object, return either None or a dict with a
    "message" key whose value is a string (the dict may also have any other
    keys that it wants).

    The given "error" object can be:

    - None, in which case None is returned

    - A string, in which case a dict like this will be returned:
      {"message": error_string}

    - A dict with a "message" key whose value is a string, in which case the
      dict will be returned unchanged

    :param error: the error object to validate

    :raises InvalidErrorObjectError: If the error object doesn't match any of
        the allowed types

    """
    if error is None:
        return None
    elif isinstance(error, str):
        return {"message": error}
    else:
        try:
            message = error["message"]
            if isinstance(message, str):
                return error
            else:
                raise jex.InvalidErrorObjectError("error['message'] must be a string")
        except (TypeError, KeyError):
            raise jex.InvalidErrorObjectError(
                "error must be either a string or a dict with a message key"
            )


def update_job(job_id, job_dict):  # sourcery skip: raise-specific-error
    """Update the database row for the given job_id with the given job_dict.

    All functions that update rows in the jobs table do it by calling this
    helper function.

    job_dict is a dict with values corresponding to the database columns that
    should be updated, e.g.:

      {"status": "complete", "data": ...}

    """
    # Avoid SQLAlchemy "Unicode type received non-unicode bind param value"
    # warnings.
    if job_id:
        job_id = str(job_id)

    if "error" in job_dict:
        job_dict["error"] = validate_error(job_dict["error"])
        job_dict["error"] = json.dumps(job_dict["error"])
        # Avoid SQLAlchemy "Unicode type received non-unicode bind param value"
        # warnings.
        job_dict["error"] = str(job_dict["error"])

    # Avoid SQLAlchemy "Unicode type received non-unicode bind param value"
    # warnings.
    if "data" in job_dict:
        job_dict["data"] = str(job_dict["data"])

    try:
        job = Jobs.get(job_id)
        if not job:
            raise Exception("Job not found")
        # dicticize the job
        jobs_dict = job.as_dict()
        jobs_dict.update(job_dict)

        Jobs.update(jobs_dict)

    except Exception as e:
        logger.error("Failed to update job %s: %s", job_id, e)
        raise e


def mark_job_as_completed(job_id, data=None):
    """Mark a job as completed successfully.

    :param job_id: the job_id of the job to be updated
    :type job_id: unicode

    :param data: the output data returned by the job
    :type data: any JSON-serializable type (including None)

    """
    update_dict = {
        "status": "complete",
        "data": json.dumps(data),
        "finished_timestamp": datetime.datetime.now(),
    }
    update_job(job_id, update_dict)


def mark_job_as_errored(job_id, error_object):
    """Mark a job as failed with an error.

    :param job_id: the job_id of the job to be updated
    :type job_id: unicode

    :param error_object: the error returned by the job
    :type error_object: either a string or a dict with a "message" key whose
        value is a string

    """
    update_dict = {
        "status": "error",
        "error": error_object,
        "finished_timestamp": datetime.datetime.now(),
    }
    update_job(job_id, update_dict)


def mark_job_as_failed_to_post_result(job_id):
    """Mark a job as 'failed to post result'.

    This happens when a job completes (either successfully or with an error)
    then trying to post the job result back to the job's callback URL fails.

    FIXME: This overwrites any error from the job itself!

    :param job_id: the job_id of the job to be updated
    :type job_id: unicode

    """
    update_dict = {
        "error": "Process completed but unable to post to result_url",
    }
    update_job(job_id, update_dict)


def delete_api_key(job_id):
    """Delete the given job's API key from the database.

    The API key is used when posting the job's result to the client's callback
    URL. This function should be called to delete the API key after the result
    has been posted - the API key is no longer needed.

    """
    update_job(job_id, {"api_key": None})


def set_aps_job_id(job_id, aps_job_id):

    update_job(job_id, {"aps_job_id": aps_job_id})


def extract_zip_or_metadata(
    zip_path: Union[str, Path],
    output_dir: Optional[Union[str, Path]] = None,
    task_logger: Optional[logging.Logger] = None,
):
    """
    Extract metadata from ZIP archive and save to CSV file.
    If the ZIP file contains only one item of a supported format, extract it directly.

    Args:
        zip_path: Path to the ZIP file
        output_dir: Directory to save the extracted or metadata file (defaults to zip_path's directory)
        task_logger: Optional logger to use for logging (if not provided, module logger will be used)

    Returns:
        tuple: (int, str, str) - (file_count, result_path, unzipped_format)
            - file_count: Number of files in the ZIP
            - result_path: Path to the extracted file or metadata CSV
            - unzipped_format: Format of the extracted file (e.g., "csv", "json", etc.)
    """
    import os

    logger = task_logger if task_logger is not None else logger

    if output_dir is None:
        output_dir = os.path.dirname(zip_path)
    # Default result path for metadata
    result_path = os.path.join(output_dir, "zip_metadata.csv")

    try:
        with zipfile.ZipFile(zip_path, "r") as zip_file:
            file_list = [info for info in zip_file.infolist() if not info.is_dir()]
            file_count = len(file_list)

            if file_count == 1:
                file_info = file_list[0]
                file_name = file_info.filename
                file_ext = os.path.splitext(file_name)[1][1:].upper()

                if file_ext in [fmt.upper() for fmt in conf.FORMATS]:
                    logger.info(
                        f"ZIP contains a single supported file: {file_name}. Extracting directly for analysis..."
                    )
                    # Extract to output_dir with correct extension
                    result_path = os.path.join(output_dir, f"zip_data.{file_ext}")
                    with zip_file.open(file_name) as source, open(
                        result_path, "wb"
                    ) as target:
                        target.write(source.read())
                    logger.debug(
                        f"Successfully extracted '{file_name}' to '{result_path}'"
                    )
                    return file_count, result_path, file_ext
                else:
                    logger.warning(
                        f"ZIP contains a single file that is not supported: {file_name}"
                    )

            # Otherwise, write metadata CSV
            logger.info(
                f"ZIP file contains {file_count} file/s. Saving ZIP metadata..."
            )
            with open(result_path, "w", newline="") as csv_file:
                fieldnames = [
                    "filename",
                    "compressed_size",
                    "file_size",
                    "compression_ratio",
                    "date_time",
                    "create_system",
                    "create_version",
                    "extract_version",
                    "flag_bits",
                    "internal_attr",
                    "external_attr",
                    "CRC",
                    "compress_type",
                ]
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                writer.writeheader()
                for file_info in file_list:
                    if file_info.file_size > 0:
                        compression_ratio = (
                            file_info.compress_size / file_info.file_size
                        ) * 100
                    else:
                        compression_ratio = 0
                    date_time = datetime.datetime(*file_info.date_time).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    writer.writerow(
                        {
                            "filename": file_info.filename,
                            "compressed_size": file_info.compress_size,
                            "file_size": file_info.file_size,
                            "compression_ratio": f"{compression_ratio:.2f}%",
                            "date_time": date_time,
                            "create_system": file_info.create_system,
                            "create_version": file_info.create_version,
                            "extract_version": file_info.extract_version,
                            "flag_bits": file_info.flag_bits,
                            "internal_attr": file_info.internal_attr,
                            "external_attr": file_info.external_attr,
                            "CRC": file_info.CRC,
                            "compress_type": file_info.compress_type,
                        }
                    )
                return file_count, result_path, "CSV"

    except zipfile.BadZipFile:
        logger.error(f"Error: '{zip_path}' is not a valid ZIP file.")
        return 0, "", ""
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return 0, "", ""
