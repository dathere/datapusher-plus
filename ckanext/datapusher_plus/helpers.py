# encoding: utf-8
from __future__ import annotations


import json 
import datetime
from typing import Any

import ckan.plugins.toolkit as toolkit

import ckanext.datapusher_plus.model as model

def datapusher_status(resource_id: str):
    try:
        return toolkit.get_action('datapusher_status')(
            {}, {'resource_id': resource_id})
    except toolkit.ObjectNotFound:
        return {
            'status': 'unknown'
        }


def datapusher_status_description(status: dict[str, Any]):
    _ = toolkit._

    if status.get('status'):
        captions = {
            'complete': _('Complete'),
            'pending': _('Pending'),
            'submitting': _('Submitting'),
            'error': _('Error'),
        }

        return captions.get(status['status'], status['status'].capitalize())
    else:
        return _('Not Uploaded Yet')


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
        result = model.Jobs.get_by_aps_id(use_aps_id).first()
    else:
        result = model.Jobs.get(job_id).first()

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

    result_dict["metadata"] = model.Metadata.get(job_id)
    result_dict["logs"] = model.Logs.get_with_limit(job_id, limit=limit)

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
    
    job = model.Jobs(job_id, job_type, "pending", data, None, None, None, None, None, result_url, api_key, job_key)

