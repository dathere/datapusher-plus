# encoding: utf-8
# flake8: noqa: E501

from __future__ import annotations

import json
import zipfile
import csv
import logging
import datetime
from pathlib import Path
from typing import Any, Optional, Union, Dict, List, Tuple

import ckan.plugins.toolkit as toolkit

from ckanext.datapusher_plus.model import Jobs, Metadata, Logs
import ckanext.datapusher_plus.job_exceptions as jex
import ckanext.datapusher_plus.config as conf

_ = toolkit._

logger = logging.getLogger(__name__)


def datapusher_status(resource_id: str) -> Dict[str, str]:
    try:
        return toolkit.get_action("datapusher_status")({}, {"resource_id": resource_id})
    except toolkit.ObjectNotFound:
        return {"status": "unknown"}


def datapusher_status_description(status: Dict[str, Any]) -> str:

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


def get_job(
    job_id: Optional[str], limit: Optional[int] = None, use_aps_id: bool = False
) -> Optional[Dict[str, Any]]:
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
    job_id: str,
    api_key: str,
    job_type: str,
    job_key: Optional[str] = None,
    data: Optional[Dict[str, Any]] = None,
    metadata: Optional[Dict[str, Any]] = None,
    result_url: Optional[str] = None,
) -> None:
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


def validate_error(
    error: Optional[Union[str, Dict[str, Any]]],
) -> Optional[Dict[str, str]]:
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


def update_job(
    job_id: str, job_dict: Dict[str, Any]
) -> None:  # sourcery skip: raise-specific-error
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


def mark_job_as_completed(job_id: str, data: Optional[Any] = None) -> None:
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


def mark_job_as_errored(job_id: str, error_object: Union[str, Dict[str, str]]) -> None:
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


def mark_job_as_failed_to_post_result(job_id: str) -> None:
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


def delete_api_key(job_id: str) -> None:
    """Delete the given job's API key from the database.

    The API key is used when posting the job's result to the client's callback
    URL. This function should be called to delete the API key after the result
    has been posted - the API key is no longer needed.

    """
    update_job(job_id, {"api_key": None})


def set_aps_job_id(job_id: str, aps_job_id: str) -> None:

    update_job(job_id, {"aps_job_id": aps_job_id})


def extract_zip_or_metadata(
    zip_path: Union[str, Path],
    output_dir: Optional[Union[str, Path]] = None,
    task_logger: Optional[logging.Logger] = None,
):
    """
    Extract metadata from ZIP archive and save to CSV file.
    If the ZIP file contains only one item of a supported format and
    AUTO_UNZIP_ONE_FILE is True, extract it directly.

    Args:
        zip_path: Path to the ZIP file
        output_dir: Directory to save the extracted or metadata file
                    (defaults to zip_path's directory)
        task_logger: Optional logger to use for logging
                     (if not provided, module logger will be used)

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

            if file_count == 1 and conf.AUTO_UNZIP_ONE_FILE:
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


def scheming_field_suggestion(field):
    """
    Returns suggestion data for a field if it exists
    """
    suggestion_label = field.get('suggestion_label', field.get('label', ''))
    suggestion_formula = field.get('suggestion_formula', field.get('suggest_jinja2', None))

    if suggestion_formula:
        return {
            'label': suggestion_label,
            'formula': suggestion_formula
        }
    return None



def scheming_get_suggestion_value(field_name, data=None, errors=None, lang=None):
    if not data:
        return ''

    try:
        # Log the field name
        logger.info(f"Field name extracted: {field_name}")

        # Get package data (where dpp_suggestions is stored)
        package_data = data
        logger.info(f"Data passed to scheming_get_suggestion_value: {data}")

        # Check if dpp_suggestions exists and has the package section
        if (package_data and 'dpp_suggestions' in package_data and 
            isinstance(package_data['dpp_suggestions'], dict) and
            'package' in package_data['dpp_suggestions']):

            # Get the suggestion value if it exists
            if field_name in package_data['dpp_suggestions']['package']:
                logger.info(f"Suggestion value found for field '{field_name}': {package_data['dpp_suggestions']['package'][field_name]}")
                return package_data['dpp_suggestions']['package'][field_name]

        # No suggestion value found
        return ''
    except Exception as e:
        # Log the error but don't crash
        logger.warning(f"Error getting suggestion value: {e}")
        return ''

def scheming_is_valid_suggestion(field, value):
    """
    Check if a suggested value is valid for a field, particularly for select fields
    """
    # If not a select/choice field, always valid
    if not field.get('choices') and not field.get('choices_helper'):
        return True

    # Get all valid choices for this field
    choices = scheming_field_choices(field)
    if not choices:
        return True

    # Check if the value is in the list of valid choices
    for choice in choices:
        if choice['value'] == value:
            return True

    return False

def is_preformulated_field(field):
    """
    Check if a field is preformulated (has formula attribute)
    This helper returns True only if the field has a 'formula' key with a non-empty value
    """
    return bool(field.get('formula', False))
 

def get_primary_key_candidates(resource_id):
    """
    Get primary key candidates for a resource from dpp_suggestions.
    
    Returns list of column names that are potential primary keys based on:
    - Cardinality equals record count (all values unique)
    - No null values
    
    Args:
        resource_id: ID of the resource to get primary key candidates for
        
    Returns:
        list: List of column names that are primary key candidates
    """
    try:
        # Get the resource information
        resource = toolkit.get_action('resource_show')({}, {'id': resource_id})
        
        # Get the package to access dpp_suggestions
        package_id = resource.get('package_id')
        if not package_id:
            return []
            
        package = toolkit.get_action('package_show')({}, {'id': package_id})
        
        # Check if dpp_suggestions exists and has primary key candidates
        dpp_suggestions = package.get('dpp_suggestions', {})
        if isinstance(dpp_suggestions, dict):
            # Look for primary key candidates in the suggestions
            primary_key_candidates = dpp_suggestions.get('PRIMARY_KEY_CANDIDATES', [])
            if primary_key_candidates:
                logger.debug(f"Found primary key candidates for resource {resource_id}: {primary_key_candidates}")
                return primary_key_candidates
                
        # Fallback: if no candidates found in suggestions, return empty list
        logger.debug(f"No primary key candidates found for resource {resource_id}")
        return []
        
    except (toolkit.ObjectNotFound, toolkit.NotAuthorized, KeyError, TypeError) as e:
        # If we can't get the data, return empty list
        logger.warning(f"Error getting primary key candidates for resource {resource_id}: {e}")
        return []


def get_datastore_fields_with_cardinality(resource_id):
    """
    Get datastore fields along with their cardinality information from dpp_suggestions.
    
    Args:
        resource_id: ID of the resource
        
    Returns:
        list: List of dicts with field info and cardinality, or fallback to basic datastore dictionary
    """
    try:
        # Get the resource information
        resource = toolkit.get_action('resource_show')({}, {'id': resource_id})
        
        # Get the package to access dpp_suggestions
        package_id = resource.get('package_id')
        if not package_id:
            # Fallback to basic datastore dictionary
            return toolkit.h.datastore_dictionary(resource_id)
            
        package = toolkit.get_action('package_show')({}, {'id': package_id})
        
        # Get basic datastore dictionary
        basic_fields = toolkit.h.datastore_dictionary(resource_id)
        
        # Enhance with cardinality information from dpp_suggestions
        dpp_suggestions = package.get('dpp_suggestions', {})
        cardinality_info = {}
        
        if isinstance(dpp_suggestions, dict):
            cardinality_info = dpp_suggestions.get('CARDINALITY', {})
            
        # Add cardinality to field information
        enhanced_fields = []
        for field in basic_fields:
            field_copy = field.copy()
            field_name = field['id']
            field_copy['cardinality'] = cardinality_info.get(field_name, 0)
            enhanced_fields.append(field_copy)
            
        return enhanced_fields
        
    except (toolkit.ObjectNotFound, toolkit.NotAuthorized, KeyError, TypeError):
        # Fallback to basic datastore dictionary
        try:
            return toolkit.h.datastore_dictionary(resource_id)
        except:
            return []