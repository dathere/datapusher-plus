# -*- coding: utf-8 -*-
# flake8: noqa: E501
"""
Utility functions for interacting with CKAN's datastore and resources.
"""
import json
import datetime
import decimal
import ckan.plugins.toolkit as tk

import ckanext.datapusher_plus.utils as utils


class DatastoreEncoder(json.JSONEncoder):
    """Custom JSON encoder for datastore values."""

    def default(self, obj: object) -> object:
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        if isinstance(obj, decimal.Decimal):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


def delete_datastore_resource(resource_id: str) -> None:
    """Delete a resource from datastore."""
    try:
        tk.get_action("datastore_delete")(
            {"ignore_auth": True}, {"resource_id": resource_id, "force": True}
        )
    except tk.ObjectNotFound:
        raise utils.JobError("Deleting existing datastore failed.")


def delete_resource(resource_id: str) -> None:
    """Delete a resource from CKAN."""
    try:
        tk.get_action("resource_delete")(
            {"ignore_auth": True}, {"id": resource_id, "force": True}
        )
    except tk.ObjectNotFound:
        raise utils.JobError("Deleting existing resource failed.")


def datastore_resource_exists(resource_id: str) -> dict:
    """Check if a resource exists in datastore."""
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
        return None


def datastore_search_sql(sql: str) -> dict:
    """Search datastore using SQL."""
    context = {"ignore_auth": True}
    data_dict = {
        "sql": sql,
    }
    try:
        result = tk.get_action("datastore_search_sql")(context, data_dict)
        return result
    except Exception as e:
        raise utils.JobError(f'Error running datastore_search_sql "{sql}": {e}')


def datastore_info(resource_id: str) -> dict:
    """Get datastore info for a resource."""
    context = {"ignore_auth": True}
    data_dict = {"id": resource_id}
    try:
        result = tk.get_action("datastore_info")(context, data_dict)
        return result
    except Exception as e:
        raise utils.JobError(f'Error getting datastore info for "{resource_id}": {e}')


def send_resource_to_datastore(
    resource: dict,
    resource_id: str,
    headers: list,
    records: list,
    aliases: list,
    calculate_record_count: bool,
) -> dict:
    """Store records in CKAN datastore."""
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


def upload_resource(new_resource: dict, file: str) -> None:
    """Upload a new resource to CKAN."""
    site_user = tk.get_action("get_site_user")({"ignore_auth": True}, {})
    context = {
        "package_id": new_resource["package_id"],
        "ignore_auth": True,
        "user": site_user["name"],
        "auth_user_obj": None,
    }

    with open(file, "rb") as f:
        new_resource["upload"] = f
        try:
            tk.get_action("resource_create")(context, new_resource)
        except tk.ObjectNotFound:
            raise utils.JobError("Creating resource failed.")


def update_resource(resource: dict) -> None:
    """Update resource metadata."""
    site_user = tk.get_action("get_site_user")({"ignore_auth": True}, {})
    context = {"ignore_auth": True, "user": site_user["name"], "auth_user_obj": None}
    try:
        tk.get_action("resource_update")(context, resource)
    except tk.ObjectNotFound:
        raise utils.JobError("Updating existing resource failed.")


def get_resource(resource_id: str) -> dict:
    """Get available information about the resource from CKAN."""
    resource_dict = tk.get_action("resource_show")(
        {"ignore_auth": True}, {"id": resource_id}
    )
    return resource_dict


def get_package(package_id: str) -> dict:
    """Get available information about a package from CKAN."""
    dataset_dict = tk.get_action("package_show")(
        {"ignore_auth": True}, {"id": package_id}
    )
    return dataset_dict


def resource_exists(package_id: str, resource_name: str) -> tuple[bool, str | None]:
    """
    Check if a resource name exists in a package.
    Returns:
        False if package or resource not found
        (True, resource_id) if resource found
    """
    package = get_package(package_id)
    if not package:
        return False, None
    for resource in package["resources"]:
        if resource["name"] == resource_name:
            return True, resource["id"]
    return False, None


def patch_package(package: dict) -> dict:
    """Patch package metadata."""
    site_user = tk.get_action("get_site_user")({"ignore_auth": True}, {})
    context = {"ignore_auth": True, "user": site_user["name"], "auth_user_obj": None}
    patched_package = tk.get_action("package_patch")(context, package)
    return patched_package


def revise_package(
    package_id: str,
    match: dict | None = None,
    filter: list | None = None,
    update: dict | None = None,
    include: list | None = None,
) -> dict:
    """
    Revise package metadata using the package_revise action API.

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

    # If match dict wasn't provided, initialize it as empty dict
    # Then add the package_id to ensure we're updating the correct package
    match = match or {}
    match["id"] = package_id

    data_dict = {
        "match": match,
        "filter": filter or [],  # Must be a list
        "update": update or {},
        "include": include or [],  # Must be a list
    }

    revised_package = tk.get_action("package_revise")(context, data_dict)
    return revised_package


def get_scheming_yaml(
    package_id: str, scheming_yaml_type: str = "dataset"
) -> tuple[dict, dict]:
    """Get the scheming yaml for a package."""
    package = get_package(package_id)
    if not package:
        raise utils.JobError("Package not found")

    scheming_yaml = tk.get_action("scheming_dataset_schema_show")(
        {"ignore_auth": True}, {"type": scheming_yaml_type}
    )

    return scheming_yaml, package
