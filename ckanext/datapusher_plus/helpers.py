# encoding: utf-8
from __future__ import annotations


import json
import logging
import datetime
from typing import Any
from sqlalchemy.orm import Query

import ckan.plugins.toolkit as toolkit
from ckan import model as ckan_model

from ckanext.datapusher_plus.model import Jobs, Metadata, Logs
import ckanext.datapusher_plus.job_exceptions as jex

_ = toolkit._

log = logging.getLogger(__name__)


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


def datapusher_plus_calculate_field(resource: dict[str, Any], expression: str):
    """Calculate the field using a Jinja2 expression.
    The Jinja2 expression is evaluated in the context of the resource.
    The resource is passed to the Jinja2 template as a dict.
    The resource dict is the same as the resource dict returned by the
    get_resource action.

    To access the value of a field in the resource dict, use the following syntax:
    {{ resource.field_name }}

    Further, the resource dict is augmented with the following variables:
    - stats: a dict of stats for the resource
    - freq: a dict of frequency for the resource

    To access the stats or freq dicts, use the following syntax:
    {{ stats.field_name.stat_name }}
    {{ freq.field_name.freq_values }}

    The field_name is the name of the field to calculate the value of.
    The stat_name is the name of the stat to access.
    The freq_values is a list of frequency values for the field.
    Each freq_value is a dict with the following keys:
    - value: the value of the frequency
    - count: the count of the frequency
    - percentage: the percentage of the frequency
    """
    from jinja2 import Template, Environment, meta

    # Create a sandboxed environment
    env = Environment(autoescape=True)

    try:
        # Create template from expression
        template = env.from_string(expression)

        # Create context with resource and its augmented data
        context = {
            "resource": resource,
            "stats": resource.get("stats", {}),
            "freq": resource.get("freq", {}),
        }

        # Render the template with the context
        result = template.render(**context)
        return result

    except Exception as e:
        log.error(f"Error calculating field: {str(e)}")
        return None


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
        log.error("Failed to update job %s: %s", job_id, e)
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


# Jinja2 filters and functions
# Helper function to truncate text to a specific length and append ellipsis.
def truncate_with_ellipsis(text, length=50, ellipsis="..."):
    """Truncate text to a specific length and append ellipsis."""
    if not text or len(text) <= length:
        return text
    return text[:length] + ellipsis


def format_number(value, decimals=2):
    """Format numbers with thousands separator and decimal places

    Example:
    {{ stats.population.sum | format_number }} -> 1,234,567.89
    """
    return f"{float(value):,.{decimals}f}"


def format_bytes(bytes):
    """Format byte sizes into human readable format

    Example:
    {{ stats.file_size.max | format_bytes }} -> 1.5 GB
    """
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes < 1024:
            return f"{bytes:.1f} {unit}"
        bytes /= 1024


def format_date(value, format="%Y-%m-%d"):
    """Format dates in specified format

    Example:
    {{ stats.created_date.max | format_date("%B %d, %Y") }} -> January 1, 2024
    """
    return value.strftime(format)


def calculate_percentage(part, whole):
    """Calculate percentage

    Example:
    {{ calculate_percentage(stats.nullcount, stats.total_rows) }} -> 12.5
    """
    return (part / whole) * 100 if whole else 0


def get_unique_ratio(field):
    """Get ratio of unique values

    Example:
    {{ get_unique_ratio(stats.user_id) }} -> 0.95
    """
    return field.cardinality / field.total_rows if field.total_rows else 0


def format_range(min_val, max_val, separator=" to "):
    """Format a range of values

    Example:
    {{ format_range(stats.temperature.min, stats.temperature.max) }} -> "-10 to 35"
    """
    return f"{min_val}{separator}{max_val}"


def format_coordinates(lat, lon, precision=6):
    """Format coordinates nicely

    Example:
    {{ format_coordinates(stats.latitude.mean, stats.longitude.mean) }}
    -> "40.7128°N, 74.0060°W"
    """
    lat_dir = "N" if lat >= 0 else "S"
    lon_dir = "E" if lon >= 0 else "W"
    return f"{abs(lat):.{precision}f}°{lat_dir}, {abs(lon):.{precision}f}°{lon_dir}"


def calculate_bbox_area(min_lon, min_lat, max_lon, max_lat):
    """Calculate approximate area of bounding box in square kilometers

    Example:
    {{ calculate_bbox_area(bbox.min_lon, bbox.min_lat, bbox.max_lon, bbox.max_lat) }}
    -> 1234.56
    """
    from math import radians, cos

    earth_radius = 6371  # km
    width = abs(max_lon - min_lon) * cos(radians((min_lat + max_lat) / 2))
    height = abs(max_lat - min_lat)
    return width * height * (earth_radius**2)


# Jinja2 Functions
def spatial_extent_wkt(
    min_lon: float, min_lat: float, max_lon: float, max_lat: float
) -> str:
    """Convert min/max WGS84 coordinates to WKT polygon format.

    Args:
        min_lon: Minimum longitude coordinate
        min_lat: Minimum latitude coordinate
        max_lon: Maximum longitude coordinate
        max_lat: Maximum latitude coordinate

    Returns:
        str: WKT polygon string representing the spatial extent

    Example:
        >>> spatial_extent_wkt(-180, -90, 180, 90)
        'POLYGON((-180 -90, -180 90, 180 90, 180 -90, -180 -90))'
    """
    # Create WKT polygon string from coordinates
    wkt = f"SRID=4326;POLYGON(({min_lon} {min_lat}, {min_lon} {max_lat}, {max_lon} {max_lat}, {max_lon} {min_lat}, {min_lon} {min_lat}))"
    return wkt


def spatial_extent_feature_collection(
    name: str, bbox: list[float], type: str = "calculated"
) -> str:
    """Convert a bounding box to a namedGeoJSON feature collection.

    Args:
        name: Name of the feature
        bbox: List of floats representing the bounding box [min_lon, min_lat, max_lon, max_lat]
        type: Type of the feature, defaults to "calculated"

    Returns:
        str: GeoJSON feature collection string

    Example:
        >>> spatial_extent_feature_collection("User Drawn Polygon 1", "draw", [-180, -90, 180, 90])
        '{"type": "FeatureCollection", "features": [{"type": "Feature", "properties":{"name":"User Drawn Polygon 1","type":"draw"}, "geometry": {"type": "Polygon", "coordinates": [[[-180, -90], [-180, 90], [180, 90], [180, -90], [-180, -90]]]}, "properties": {}}]}
    """
    return f'{{"type": "FeatureCollection", "features": [{{"type": "Feature", "properties": {{"name": "{name}", "type": "{type}"}}, "geometry": {{"type": "Polygon", "coordinates": [[{bbox[0]} {bbox[1]}, {bbox[0]} {bbox[3]}, {bbox[2]} {bbox[3]}, {bbox[2]} {bbox[1]}, {bbox[0]} {bbox[1]}]]}}, "properties": {{}}}}]}}'
