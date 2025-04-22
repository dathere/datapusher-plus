# encoding: utf-8
from __future__ import annotations

from ckan.common import CKANConfig
import logging
from typing import Any, Callable

import ckan.model as model
import ckan.plugins as p
import ckanext.datapusher_plus.views as views
import ckanext.datapusher_plus.helpers as dph
import ckanext.datapusher_plus.logic.action as action
import ckanext.datapusher_plus.logic.auth as auth
import ckanext.datapusher_plus.cli as cli

tk = p.toolkit

log = logging.getLogger(__name__)

try:
    config_declarations = tk.blanket.config_declarations
except AttributeError:
    # CKAN 2.9 does not have config_declarations.
    # Remove when dropping support.
    def config_declarations(cls):
        return cls


# Get ready for CKAN 2.10 upgrade
if tk.check_ckan_version("2.10"):
    from ckan.types import Action, AuthFunction, Context


class DatastoreException(Exception):
    pass


@config_declarations
class DatapusherPlusPlugin(p.SingletonPlugin):
    p.implements(p.IConfigurer, inherit=True)
    p.implements(p.IConfigurable, inherit=True)
    p.implements(p.IActions)
    p.implements(p.IAuthFunctions)
    p.implements(p.IResourceUrlChange)
    p.implements(p.IResourceController, inherit=True)
    p.implements(p.ITemplateHelpers)
    p.implements(p.IBlueprint)
    p.implements(p.IClick)

    legacy_mode = False
    resource_show_action = None

    def update_config(self, config: CKANConfig):
        tk.add_template_directory(config, "templates")
        tk.add_public_directory(config, "public")
        tk.add_resource("assets", "datapusher_plus")

    # IResourceUrlChange
    def notify(self, resource: model.Resource):
        context = {
            "model": model,
            "ignore_auth": True,
        }
        resource_dict = tk.get_action("resource_show")(
            context,
            {
                "id": resource.id,
            },
        )
        self._submit_to_datapusher(resource_dict)

    # IResourceController

    # def before_resource_create(self, context, resource_dict: dict[str, Any]):
    #     self._submit_to_datapusher(resource_dict)

    def after_resource_create(self, context, resource_dict: dict[str, Any]):
        self._submit_to_datapusher(resource_dict)

    if not tk.check_ckan_version("2.10"):

        def after_create(self, context, resource_dict):
            self.after_resource_create(context, resource_dict)

    def _submit_to_datapusher(self, resource_dict: dict[str, Any]):
        context = {"model": model, "ignore_auth": True, "defer_commit": True}

        resource_format = resource_dict.get("format")
        supported_formats = tk.config.get("ckan.datapusher.formats") or tk.config.get(
            "ckanext.datapusher_plus.formats"
        )
        if not supported_formats:
            log.debug(
                "No supported formats configured,\
                    using DataPusher Plus internals"
            )
            supported_formats = ["csv", "xls", "xlsx", "tsv"]

        submit = (
            resource_format
            and resource_format.lower() in supported_formats
            and resource_dict.get("url_type") != "datapusher"
        )

        if not submit:
            return

        try:
            task = tk.get_action("task_status_show")(
                context,
                {
                    "entity_id": resource_dict["id"],
                    "task_type": "datapusher_plus",
                    "key": "datapusher_plus",
                },
            )

            if task.get("state") in ("pending", "submitting"):
                # There already is a pending DataPusher submission,
                # skip this one ...
                log.debug(
                    "Skipping DataPusher Plus submission for "
                    "resource {0}".format(resource_dict["id"])
                )
                return
        except tk.ObjectNotFound:
            pass

        try:
            log.debug(
                "Submitting resource {0}".format(resource_dict["id"])
                + " to DataPusher Plus"
            )
            tk.get_action("datapusher_submit")(
                context, {"resource_id": resource_dict["id"]}
            )
        except tk.ValidationError as e:
            # If datapusher is offline want to catch error instead
            # of raising otherwise resource save will fail with 500
            log.critical(e)
            pass

    def get_actions(self) -> dict[str, Action]:
        return {
            "datapusher_submit": action.datapusher_submit,
            "datapusher_hook": action.datapusher_hook,
            "datapusher_status": action.datapusher_status,
        }

    def get_auth_functions(self) -> dict[str, AuthFunction]:
        return {
            "datapusher_submit": auth.datapusher_submit,
            "datapusher_status": auth.datapusher_status,
        }

    def get_helpers(self) -> dict[str, Callable[..., Any]]:
        return {
            "datapusher_plus_status": dph.datapusher_status,
            "datapusher_plus_status_description": dph.datapusher_status_description,
        }

    # IBlueprint

    def get_blueprint(self):
        return views.get_blueprints()

    # IClick
    def get_commands(self):
        return cli.get_commands()
