# encoding: utf-8
# flake8: noqa: E501

from __future__ import annotations

from typing import Any
import ckan.plugins as p
import ckanext.datastore.logic.auth as auth

if p.toolkit.check_ckan_version("2.10"):
    from ckan.types import AuthResult, Context


def datapusher_submit(context: Context, data_dict: dict[str, Any]) -> AuthResult:
    return auth.datastore_auth(context, data_dict)


def datapusher_status(context: Context, data_dict: dict[str, Any]) -> AuthResult:
    return auth.datastore_auth(context, data_dict)
