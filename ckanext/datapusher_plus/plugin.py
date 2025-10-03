# encoding: utf-8
# flake8: noqa: E501

from __future__ import annotations

from ckan.common import CKANConfig
import logging
import types
from typing import Any, Callable, Optional, Literal

from ckan.plugins.toolkit import add_template_directory, h
from ckan.types import Action, AuthFunction, Context

import ckan.model as model
import ckan.plugins as p
import ckanext.datapusher_plus.views as views
import ckanext.datapusher_plus.helpers as dph
import ckanext.datapusher_plus.logic.action as action
import ckanext.datapusher_plus.logic.auth as auth
import ckanext.datapusher_plus.cli as cli

tk = p.toolkit

log = logging.getLogger(__name__)


config_declarations = tk.blanket.config_declarations


class DatastoreException(Exception):
    pass


class DatastoreException(Exception):
    pass


@config_declarations
class DatapusherPlusPlugin(p.SingletonPlugin):
    p.implements(p.IConfigurer, inherit=True)
    p.implements(p.IConfigurable, inherit=True)
    p.implements(p.IActions)
    p.implements(p.IAuthFunctions)
    p.implements(p.IPackageController, inherit=True)
    p.implements(p.IResourceUrlChange)
    p.implements(p.IResourceController, inherit=True)
    p.implements(p.ITemplateHelpers)
    p.implements(p.IBlueprint)
    p.implements(p.IClick)
    
    # Always implement IFormRedirect if available, methods will check config
    try:
        p.implements(p.IFormRedirect)
    except (ImportError, AttributeError):
        # IFormRedirect not available in this CKAN version
        pass

    legacy_mode = False
    resource_show_action = None

    def configure(self, config):
        """Called when the plugin is loaded."""
        # Check configuration for optional features and store for reference
        self.enable_form_redirect = tk.asbool(
            config.get('ckanext.datapusher_plus.enable_form_redirect', False)
        )
        self.enable_druf = tk.asbool(
            config.get('ckanext.datapusher_plus.enable_druf', False)
        )
        
        if self.enable_form_redirect:
            log.info("IFormRedirect functionality enabled for DataPusher Plus")
        else:
            log.debug("IFormRedirect functionality disabled")
            
        if self.enable_druf:
            log.info("DRUF functionality enabled for DataPusher Plus")

    def update_config(self, config: CKANConfig):
        # Always add base templates
        tk.add_template_directory(config, "templates")
        tk.add_public_directory(config, "public")
        tk.add_resource("assets", "datapusher_plus")
        
        # Check configuration for optional features directly from config
        enable_druf = tk.asbool(config.get('ckanext.datapusher_plus.enable_druf', False))
        enable_form_redirect = tk.asbool(config.get('ckanext.datapusher_plus.enable_form_redirect', False))
        
        # Conditionally add DRUF templates if enabled
        if enable_druf:
            # Add DRUF-specific template overrides
            tk.add_template_directory(config, "templates/druf")
            log.info("DataPusher Plus: DRUF template overrides loaded")
        
        # Log configuration status
        if enable_form_redirect:
            log.info("DataPusher Plus: IFormRedirect functionality enabled")
        if enable_druf:
            log.info("DataPusher Plus: DRUF (Dataset Resource Upload First) functionality enabled")

    # IPackageController
    def before_dataset_index(self, dataset_dict: dict[str, Any]):
        dataset_dict.pop("dpp_suggestions", None)
        return dataset_dict

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
            supported_formats = [
                "csv",
                "xls",
                "xlsx",
                "tsv",
                "ssv",
                "tab",
                "ods",
                "geojson",
                "shp",
                "qgis",
                "zip",
            ]

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
            "scheming_field_suggestion": dph.scheming_field_suggestion,
            "scheming_get_suggestion_value": dph.scheming_get_suggestion_value,
            "scheming_is_valid_suggestion": dph.scheming_is_valid_suggestion,
            "is_preformulated_field": dph.is_preformulated_field,
            "get_primary_key_candidates": dph.get_primary_key_candidates,
            "get_datastore_fields_with_cardinality": dph.get_datastore_fields_with_cardinality,
        }

    # IBlueprint
    def get_blueprint(self):
        """Register plugin blueprints"""
        blueprints = []
        blueprints.extend(views.get_blueprints())
        
        # Only include DRUF blueprints if enabled
        enable_druf = tk.asbool(tk.config.get('ckanext.datapusher_plus.enable_druf', False))
        if enable_druf:
            try:
                import ckanext.datapusher_plus.druf_view as druf_view
                blueprints.extend(druf_view.get_blueprints())
                log.debug("DRUF blueprints registered")
            except ImportError as e:
                log.error(f"Failed to import DRUF views: {e}")
        
        return blueprints 

    # IClick
    def get_commands(self):
        return cli.get_commands()

    # IFormRedirect methods - always present but check config before acting
    def dataset_save_redirect(
            self, package_type: str, package_name: str,
            action: Literal['create', 'edit'], save_action: Optional[str],
            data: dict[str, Any],
            ) -> Optional[str]:
        # Check if IFormRedirect is enabled
        enable_form_redirect = tk.asbool(tk.config.get('ckanext.datapusher_plus.enable_form_redirect', False))
        if not enable_form_redirect:
            log.debug("IFormRedirect disabled, using default dataset redirect")
            return None
            
        log.debug(f"IFormRedirect dataset save: {action}, save_action: {save_action}")
        # Only redirect after successful dataset creation, not during editing
        if action == 'create':
            return h.url_for(f'{package_type}.read', id=package_name)
        return None

    def resource_save_redirect(
            self, package_type: str, package_name: str, resource_id: Optional[str],
            action: Literal['create', 'edit'], save_action: str,
            data: dict[str, Any],
            ) -> Optional[str]:
        # Check if IFormRedirect is enabled
        enable_form_redirect = tk.asbool(tk.config.get('ckanext.datapusher_plus.enable_form_redirect', False))
        if not enable_form_redirect:
            log.debug("IFormRedirect disabled, using default resource redirect")
            return None
            
        log.debug(f"IFormRedirect resource save: {action}, save_action: {save_action}")
        if action == 'edit':
            return h.url_for(
                f'{package_type}_resource.read',
                id=package_name, resource_id=resource_id
            )
        if save_action == 'again':
            return h.url_for(
                '{}_resource.new'.format(package_type), id=package_name,
            )
        # For normal resource creation, let CKAN handle the default flow
        return None

    # IFormRedirect methods - always present but only active when enabled
    def dataset_save_redirect(
            self, package_type: str, package_name: str,
            action: Literal['create', 'edit'], save_action: Optional[str],
            data: dict[str, Any],
            ) -> Optional[str]:
        # Check if form redirect is enabled
        enable_form_redirect = tk.asbool(tk.config.get('ckanext.datapusher_plus.enable_form_redirect', False))
        if not enable_form_redirect:
            log.debug(f"IFormRedirect disabled - letting CKAN handle dataset redirect for {package_name}")
            return None  # Let CKAN handle normal redirects
        
        log.debug(f"IFormRedirect: dataset_save_redirect called - action: {action}, save_action: {save_action}")
        
        # Only redirect in specific scenarios, not all dataset saves
        # For dataset creation, be very careful not to break normal workflow
        if action == 'create':
            # Let CKAN handle the normal flow (usually to add resources)
            # Only redirect to dataset view if explicitly requested with special save action
            if save_action == 'go-dataset':
                log.debug(f"IFormRedirect: redirecting to dataset view for {package_name}")
                return h.url_for(f'{package_type}.read', id=package_name)
            else:
                log.debug(f"IFormRedirect: letting CKAN handle normal dataset creation flow for {package_name}")
                return None  # Let CKAN handle the normal flow
        elif action == 'edit':
            # For edits, redirect to dataset view
            log.debug(f"IFormRedirect: redirecting to dataset view after edit for {package_name}")
            return h.url_for(f'{package_type}.read', id=package_name)
        
        # Default: let CKAN handle the redirect
        log.debug(f"IFormRedirect: default case - letting CKAN handle redirect for {package_name}")
        return None

    def resource_save_redirect(
            self, package_type: str, package_name: str, resource_id: Optional[str],
            action: Literal['create', 'edit'], save_action: str,
            data: dict[str, Any],
            ) -> Optional[str]:
        # Check if form redirect is enabled
        enable_form_redirect = tk.asbool(tk.config.get('ckanext.datapusher_plus.enable_form_redirect', False))
        if not enable_form_redirect:
            log.debug(f"IFormRedirect disabled - letting CKAN handle resource redirect for {resource_id}")
            return None  # Let CKAN handle normal redirects
            
        log.debug(f"IFormRedirect: resource_save_redirect called - action: {action}, save_action: {save_action}")
        
        if action == 'edit':
            return h.url_for(
                f'{package_type}_resource.read',
                id=package_name, resource_id=resource_id
            )
        if save_action == 'again':
            return h.url_for(
                '{}_resource.new'.format(package_type), id=package_name,
            )
        # After resource creation, go to the dataset edit page to allow adding more resources
        return h.url_for(u'{}.edit'.format(package_type), id=package_name)
