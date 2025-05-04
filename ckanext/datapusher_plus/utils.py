# encoding: utf-8
# flake8: noqa: E501

from __future__ import division
from __future__ import absolute_import

import logging
import json
import datetime

import ckan.plugins.toolkit as tk
from ckanext.datapusher_plus.model import Logs

from .job_exceptions import HTTPError


class StoringHandler(logging.Handler):
    """A handler that stores the logging records in a database."""

    def __init__(self, task_id, input):
        logging.Handler.__init__(self)
        self.task_id = task_id
        self.input = input

    def emit(self, record):
        message = str(record.getMessage())
        level = str(record.levelname)
        module = str(record.module)
        funcName = str(record.funcName)
        job_log = Logs(
            job_id=self.task_id,
            timestamp=datetime.datetime.now(),
            message=message,
            level=level,
            module=module,
            funcName=funcName,
            lineno=record.lineno,
        )
        job_log.save()


class DatetimeJsonEncoder(json.JSONEncoder):
    # Custom JSON encoder
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()

        return json.JSONEncoder.default(self, obj)


class JobError(Exception):
    """The exception type that jobs raise to signal failure."""

    def __init__(self, message):
        """Initialize a JobError with the given error message string.
        The error message string that you give here will be returned to the
        client site in the job dict's "error" key.
        """
        self.message = message

    def as_dict(self):
        """Return a dictionary representation of this JobError object.
        Returns a dictionary with a "message" key whose value is a string error
        message - suitable for use as the "error" key in a ckanserviceprovider
        job dict.
        """
        return {"message": self.message}

    def __str__(self):
        return self.message


def get_dp_plus_user_apitoken():
    """Returns the API Token for authentication.
    datapusher plus actions require an authenticated user to perform the actions. This
    method returns the api_token set in the config file.
    """
    api_token = tk.config.get("ckanext.datapusher_plus.api_token", None)
    if api_token:
        return api_token

    # Consider also the CKAN default api_token for backward compatibility
    api_token = tk.config.get("ckan.datapusher.api_token", None)
    if api_token:
        return api_token
    else:
        raise Exception(
            "No API token found. Required for downloading private resources."
        )


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
            "DP+ received an HTTP response with no status code",
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
