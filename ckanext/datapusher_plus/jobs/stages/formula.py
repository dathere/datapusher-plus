# -*- coding: utf-8 -*-
"""
Formula stage for the DataPusher Plus pipeline.

Handles DRUF (Data Resource Update Formulae) processing using Jinja2.
"""

import time
from typing import Dict, Any, Optional

import ckanext.datapusher_plus.datastore_utils as dsu
import ckanext.datapusher_plus.jinja2_helpers as j2h
from ckanext.datapusher_plus.jobs.stages.base import BaseStage
from ckanext.datapusher_plus.jobs.context import ProcessingContext


class FormulaStage(BaseStage):
    """
    Processes DRUF formulae using Jinja2 templates.

    This stage is optional and requires the ckanext-scheming extension.
    If scheming is not available, the stage will be skipped gracefully.

    Responsibilities:
    - Fetch scheming YAML and package metadata
    - Process package formulae (direct updates)
    - Process resource formulae (direct updates)
    - Process package suggestion formulae
    - Process resource suggestion formulae

    DRUF formulae come in two types:
    1. "formula": Direct field updates (package/resource)
    2. "suggestion_formula": Populates suggestion popovers for data entry
    """

    def __init__(self):
        super().__init__(name="FormulaProcessing")

    def should_skip(self, context: ProcessingContext) -> bool:
        """
        Skip this stage if ckanext-scheming is not enabled in ckan.plugins.

        Args:
            context: Processing context

        Returns:
            True if scheming plugin is not enabled, False otherwise
        """
        try:
            # Check if scheming is in the ckan.plugins configuration
            import ckan.plugins.toolkit as tk

            # Get the list of enabled plugins from config
            plugins_config = tk.config.get('ckan.plugins', '')
            enabled_plugins = [p.strip() for p in plugins_config.split()]

            # Check for scheming-related plugins
            scheming_plugins = ['scheming_datasets', 'scheming_groups',
                              'scheming_organizations', 'scheming']

            if any(plugin in enabled_plugins for plugin in scheming_plugins):
                return False  # Scheming is enabled, don't skip

            # Scheming not enabled in config
            context.logger.info(
                "Skipping FormulaProcessing stage - ckanext-scheming not enabled in ckan.plugins"
            )
            return True

        except Exception as e:
            # If we can't read config, log and skip
            context.logger.warning(
                f"Unable to check ckan.plugins configuration: {e}. "
                "Skipping FormulaProcessing stage."
            )
            return True

    def process(self, context: ProcessingContext) -> ProcessingContext:
        """
        Process DRUF formulae.

        Args:
            context: Processing context

        Returns:
            Updated context

        Raises:
            Returns early (None) if critical errors occur
        """
        formulae_start = time.perf_counter()

        # Fetch scheming YAML and package
        package_id = context.resource["package_id"]
        try:
            scheming_yaml, package = dsu.get_scheming_yaml(
                package_id, scheming_yaml_type="dataset"
            )
        except Exception as e:
            context.logger.warning(
                f"Unable to fetch scheming YAML (scheming may not be configured): {e}"
            )
            context.logger.info("Skipping formula processing")
            return context  # Skip formula processing but continue pipeline

        # Validate scheming YAML
        if not scheming_yaml or not isinstance(scheming_yaml, dict):
            context.logger.info("No valid scheming YAML found, skipping formula processing")
            return context

        # Check for suggestion formulae
        has_suggestion_formula = self._check_for_suggestion_formulae(scheming_yaml)

        if has_suggestion_formula:
            context.logger.info("Found suggestion formulae in schema")

            # Validate and setup dpp_suggestions field
            if not self._setup_dpp_suggestions(context, scheming_yaml, package):
                return None  # Critical error, abort
        else:
            context.logger.info("No suggestion formulae found")

        context.logger.log(5, f"package: {package}")

        # Get resource field stats (need to retrieve from context or pass in)
        resource_fields_stats = self._get_resource_field_stats(context)
        resource_fields_freqs = self._get_resource_field_freqs(context)

        # Initialize formula processor
        formula_processor = j2h.FormulaProcessor(
            scheming_yaml,
            package,
            context.resource,
            resource_fields_stats,
            resource_fields_freqs,
            context.dataset_stats,
            context.logger,
        )

        # Update status
        package.setdefault("dpp_suggestions", {})[
            "STATUS"
        ] = "STARTING FORMULAE PROCESSING..."
        dsu.patch_package(package)

        # Clear LRU caches
        self._clear_caches()

        # Process package formulae (direct updates)
        package = self._process_package_formulae(
            context, formula_processor, package
        )

        # Process resource formulae (direct updates)
        self._process_resource_formulae(context, formula_processor)

        # Process package suggestion formulae
        package = self._process_package_suggestions(
            context, formula_processor, package, package_id
        )

        # Process resource suggestion formulae
        package = self._process_resource_suggestions(
            context, formula_processor, package, package_id
        )

        # Formulae processing complete
        formulae_elapsed = time.perf_counter() - formulae_start
        context.logger.info(
            f"FORMULAE PROCESSING DONE! Processed in {formulae_elapsed:,.2f} seconds."
        )

        return context

    def _check_for_suggestion_formulae(self, scheming_yaml: Dict[str, Any]) -> bool:
        """
        Check if scheming YAML contains suggestion formulae.

        Args:
            scheming_yaml: Scheming YAML dictionary

        Returns:
            True if suggestion formulae exist
        """
        return any(
            isinstance(field, dict)
            and any(key.startswith("suggestion_formula") for key in field.keys())
            for field in scheming_yaml["dataset_fields"]
        )

    def _setup_dpp_suggestions(
        self,
        context: ProcessingContext,
        scheming_yaml: Dict[str, Any],
        package: Dict[str, Any],
    ) -> bool:
        """
        Validate and setup dpp_suggestions field.

        Args:
            context: Processing context
            scheming_yaml: Scheming YAML dictionary
            package: Package dictionary

        Returns:
            True if setup successful, False if critical error
        """
        # Check if schema has dpp_suggestions field
        schema_has_dpp_suggestions = any(
            isinstance(field, dict) and field.get("field_name") == "dpp_suggestions"
            for field in scheming_yaml["dataset_fields"]
        )

        if not schema_has_dpp_suggestions:
            context.logger.error(
                '"dpp_suggestions" field required but not found in your schema. '
                "Ensure that your scheming.yaml file contains the "
                '"dpp_suggestions" field as a json_object.'
            )
            return False
        else:
            context.logger.info('Found "dpp_suggestions" field in schema')

        # Add dpp_suggestions to package if missing
        if "dpp_suggestions" not in package:
            context.logger.warning(
                'Warning: "dpp_suggestions" field required to process Suggestion '
                "Formulae is not found in this package. "
                'Adding "dpp_suggestions" to package'
            )

            try:
                package["dpp_suggestions"] = {}
                dsu.patch_package(package)
                context.logger.warning('"dpp_suggestions" field added to package')
            except Exception as e:
                context.logger.error(f'Error adding "dpp_suggestions" field {e}')
                return False

        return True

    def _get_resource_field_stats(self, context: ProcessingContext) -> Dict[str, Any]:
        """
        Get resource field statistics from context.

        Args:
            context: Processing context

        Returns:
            Resource field statistics dictionary
        """
        return context.resource_fields_stats

    def _get_resource_field_freqs(self, context: ProcessingContext) -> Dict[str, Any]:
        """
        Get resource field frequencies from context.

        Args:
            context: Processing context

        Returns:
            Resource field frequencies dictionary
        """
        return context.resource_fields_freqs

    def _clear_caches(self) -> None:
        """Clear LRU caches before processing formulae."""
        dsu.datastore_search.cache_clear()
        dsu.datastore_search_sql.cache_clear()
        dsu.datastore_info.cache_clear()
        dsu.index_exists.cache_clear()

    def _process_package_formulae(
        self,
        context: ProcessingContext,
        formula_processor: j2h.FormulaProcessor,
        package: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Process package formulae (direct updates).

        Args:
            context: Processing context
            formula_processor: Formula processor instance
            package: Package dictionary

        Returns:
            Updated package dictionary
        """
        package_updates = formula_processor.process_formulae(
            "package", "dataset_fields", "formula"
        )

        if package_updates:
            package.update(package_updates)
            status_msg = "PACKAGE formulae processed..."
            package["dpp_suggestions"]["STATUS"] = status_msg

            try:
                patched_package = dsu.patch_package(package)
                context.logger.debug(f"Package after patching: {patched_package}")
                package = patched_package
                context.logger.info(status_msg)
            except Exception as e:
                context.logger.error(f"Error patching package: {str(e)}")

        return package

    def _process_resource_formulae(
        self,
        context: ProcessingContext,
        formula_processor: j2h.FormulaProcessor,
    ) -> None:
        """
        Process resource formulae (direct updates).

        Args:
            context: Processing context
            formula_processor: Formula processor instance
        """
        resource_updates = formula_processor.process_formulae(
            "resource", "resource_fields", "formula"
        )

        if resource_updates:
            context.resource.update(resource_updates)
            status_msg = "RESOURCE formulae processed..."

            if context.resource.get("dpp_suggestions"):
                context.resource["dpp_suggestions"]["STATUS"] = status_msg
            else:
                context.resource["dpp_suggestions"] = {"STATUS": status_msg}

            context.logger.info(status_msg)

    def _process_package_suggestions(
        self,
        context: ProcessingContext,
        formula_processor: j2h.FormulaProcessor,
        package: Dict[str, Any],
        package_id: str,
    ) -> Dict[str, Any]:
        """
        Process package suggestion formulae.

        Args:
            context: Processing context
            formula_processor: Formula processor instance
            package: Package dictionary
            package_id: Package ID

        Returns:
            Updated package dictionary
        """
        package_suggestions = formula_processor.process_formulae(
            "package", "dataset_fields", "suggestion_formula"
        )

        if package_suggestions:
            context.logger.log(5, f"package_suggestions: {package_suggestions}")
            revise_update_content = {"package": package_suggestions}

            try:
                status_msg = "PACKAGE suggestion formulae processed..."
                revise_update_content["STATUS"] = status_msg
                revised_package = dsu.revise_package(
                    package_id, update={"dpp_suggestions": revise_update_content}
                )
                context.logger.log(5, f"Package after revising: {revised_package}")
                package = revised_package
                context.logger.info(status_msg)
            except Exception as e:
                context.logger.error(f"Error revising package: {str(e)}")

        return package

    def _process_resource_suggestions(
        self,
        context: ProcessingContext,
        formula_processor: j2h.FormulaProcessor,
        package: Dict[str, Any],
        package_id: str,
    ) -> Dict[str, Any]:
        """
        Process resource suggestion formulae.

        Note: Updates PACKAGE dpp_suggestions field, not resource.

        Args:
            context: Processing context
            formula_processor: Formula processor instance
            package: Package dictionary
            package_id: Package ID

        Returns:
            Updated package dictionary
        """
        resource_suggestions = formula_processor.process_formulae(
            "resource", "resource_fields", "suggestion_formula"
        )

        if resource_suggestions:
            context.logger.log(5, f"resource_suggestions: {resource_suggestions}")
            resource_name = context.resource["name"]
            revise_update_content = {
                "resource": {resource_name: resource_suggestions}
            }

            # Handle existing suggestions
            if package.get("dpp_suggestions"):
                package["dpp_suggestions"].update(revise_update_content["resource"])
            else:
                package["dpp_suggestions"] = revise_update_content["resource"]

            try:
                status_msg = "RESOURCE suggestion formulae processed..."
                revise_update_content["STATUS"] = status_msg

                revised_package = dsu.revise_package(
                    package_id, update={"dpp_suggestions": revise_update_content}
                )
                context.logger.log(5, f"Package after revising: {revised_package}")
                package = revised_package
                context.logger.info(status_msg)
            except Exception as e:
                context.logger.error(f"Error revising package: {str(e)}")

        return package
