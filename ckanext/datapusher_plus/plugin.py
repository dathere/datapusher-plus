# encoding: utf-8
from __future__ import annotations

from ckan.common import CKANConfig
import logging
from typing import Any, Callable, cast

import ckan.model as model
import ckan.plugins as p
import ckanext.datapusher_plus.views as views
import ckanext.datapusher_plus.helpers as helpers
import ckanext.datapusher_plus.logic.action as action
import ckanext.datapusher_plus.logic.auth as auth

log = logging.getLogger(__name__)


# Get ready for CKAN 2.10 upgrade
if p.toolkit.check_ckan_version("2.10"):
    from ckan.types import Action, AuthFunction, Context


class DatastoreException(Exception):
    pass


class DatapusherPlusPlugin(p.SingletonPlugin):
    p.implements(p.IConfigurer, inherit=True)
    p.implements(p.IConfigurable, inherit=True)
    p.implements(p.IActions)
    p.implements(p.IAuthFunctions)
    p.implements(p.IResourceUrlChange)
    p.implements(p.IResourceController, inherit=True)
    p.implements(p.ITemplateHelpers)
    p.implements(p.IBlueprint)

    legacy_mode = False
    resource_show_action = None

    def update_config(self, config: CKANConfig):
        p.toolkit.add_template_directory(config, "templates")
        p.toolkit.add_public_directory(config, "public")
        p.toolkit.add_resource("assets", "datapusher_plus")

    def configure(self, config: CKANConfig):
        self.config = config

        if not config.get("ckan.site_url"):
            raise Exception(
                "Config option `{0}` must be set to use the DataPusher.".format(
                    "ckan.site_url"
                )
            )

    # IResourceUrlChange

    def notify(self, resource: model.Resource):
        context = {
            "model": model,
            "ignore_auth": True,
        }
        resource_dict = p.toolkit.get_action("resource_show")(
            context,
            {
                "id": resource.id,
            },
        )
        self._submit_to_datapusher(resource_dict)

    # IResourceController

    def after_resource_create(self, context, resource_dict: dict[str, Any]):
        self._submit_to_datapusher(resource_dict)

    if not p.toolkit.check_ckan_version("2.10"):

        def after_create(self, context, resource_dict):
            self.after_resource_create(context, resource_dict)

    def _submit_to_datapusher(self, resource_dict: dict[str, Any]):
        context = {"model": model, "ignore_auth": True, "defer_commit": True}

        resource_format = resource_dict.get("format")
        supported_formats = p.toolkit.config.get("ckan.datapusher.formats")

        submit = (
            resource_format
            and resource_format.lower() in supported_formats
            and resource_dict.get("url_type") != "datapusher"
        )

        if not submit:
            return

        try:
            task = p.toolkit.get_action("task_status_show")(
                context,
                {
                    "entity_id": resource_dict["id"],
                    "task_type": "datapusher",
                    "key": "datapusher",
                },
            )

            if task.get("state") in ("pending", "submitting"):
                # There already is a pending DataPusher submission,
                # skip this one ...
                log.debug(
                    "Skipping DataPusher submission for "
                    "resource {0}".format(resource_dict["id"])
                )
                return
        except p.toolkit.ObjectNotFound:
            pass

        try:
            log.debug(
                "Submitting resource {0}".format(resource_dict["id"]) + " to DataPusher"
            )
            p.toolkit.get_action("datapusher_submit")(
                context, {"resource_id": resource_dict["id"]}
            )
        except p.toolkit.ValidationError as e:
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
            "datapusher_status": helpers.datapusher_status,
            "datapusher_status_description": helpers.datapusher_status_description,
        }

    # IBlueprint

    def get_blueprint(self):
        return views.get_blueprints()
