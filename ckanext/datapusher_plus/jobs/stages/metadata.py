# -*- coding: utf-8 -*-
"""
Metadata stage for the DataPusher Plus pipeline.

Handles resource metadata updates, auto-aliasing, and summary statistics.
"""

import os
import time
import psycopg2
from psycopg2 import sql
from typing import Optional

import ckanext.datapusher_plus.utils as utils
import ckanext.datapusher_plus.config as conf
import ckanext.datapusher_plus.datastore_utils as dsu
from ckanext.datapusher_plus.jobs.stages.base import BaseStage
from ckanext.datapusher_plus.jobs.context import ProcessingContext


class MetadataStage(BaseStage):
    """
    Updates resource metadata and creates aliases.

    Responsibilities:
    - Create auto-aliases for resources
    - Create summary statistics resource
    - Update resource metadata (datastore_active, record counts, etc.)
    - Set final aliases and calculate record counts
    """

    def __init__(self):
        super().__init__(name="MetadataUpdate")

    def process(self, context: ProcessingContext) -> ProcessingContext:
        """
        Update resource metadata.

        Args:
            context: Processing context

        Returns:
            Updated context

        Raises:
            utils.JobError: If metadata update fails
        """
        metadata_start = time.perf_counter()
        context.logger.info("UPDATING RESOURCE METADATA...")

        # Connect to database for aliasing operations
        try:
            raw_connection = psycopg2.connect(conf.DATASTORE_WRITE_URL)
        except psycopg2.Error as e:
            raise utils.JobError(f"Could not connect to the Datastore: {e}")

        try:
            cur = raw_connection.cursor()

            # Create auto-alias if configured
            alias = self._create_auto_alias(context, cur)

            # Create summary statistics resource if configured
            self._create_summary_stats_resource(context, cur)

            # Commit database changes
            cur.close()
            raw_connection.commit()

        finally:
            if raw_connection:
                raw_connection.close()

        # Update resource metadata
        self._update_resource_metadata(context)

        # Set alias and calculate record count
        dsu.send_resource_to_datastore(
            resource=None,
            resource_id=context.resource["id"],
            headers=context.headers_dicts,
            records=None,
            aliases=alias,
            calculate_record_count=True,
        )

        if alias:
            context.logger.info(f'Created alias "{alias}" for "{context.resource_id}"...')

        metadata_elapsed = time.perf_counter() - metadata_start
        context.logger.info(
            f"RESOURCE METADATA UPDATES DONE! Resource metadata updated in "
            f"{metadata_elapsed:,.2f} seconds."
        )

        # Mark as done
        package = dsu.get_package(context.resource["package_id"])
        package.setdefault("dpp_suggestions", {})["STATUS"] = "DONE"
        dsu.patch_package(package)

        return context

    def _create_auto_alias(
        self, context: ProcessingContext, cursor: psycopg2.extensions.cursor
    ) -> Optional[str]:
        """
        Create auto-alias for the resource.

        Args:
            context: Processing context
            cursor: Database cursor

        Returns:
            Alias name if created, None otherwise
        """
        if not conf.AUTO_ALIAS:
            return None

        context.logger.info(
            f"AUTO-ALIASING. Auto-alias-unique: {conf.AUTO_ALIAS_UNIQUE} ..."
        )

        # Get package info for alias construction
        package = dsu.get_package(context.resource["package_id"])

        resource_name = context.resource.get("name")
        package_name = package.get("name")
        owner_org = package.get("organization")
        owner_org_name = owner_org.get("name") if owner_org else ""

        if not (resource_name and package_name and owner_org_name):
            context.logger.warning(
                f"Cannot create alias: {resource_name}-{package_name}-{owner_org}"
            )
            return None

        # Create base alias (limited to 55 chars for sequence/stats suffix)
        alias = f"{resource_name}-{package_name}-{owner_org_name}"[:55]

        # Check if alias exists
        cursor.execute(
            "SELECT COUNT(*), alias_of FROM _table_metadata where name like %s group by alias_of",
            (alias + "%",),
        )
        alias_query_result = cursor.fetchone()

        if alias_query_result:
            alias_count = alias_query_result[0]
            existing_alias_of = alias_query_result[1]
        else:
            alias_count = 0
            existing_alias_of = ""

        # Handle alias uniqueness
        if conf.AUTO_ALIAS_UNIQUE and alias_count > 1:
            alias_sequence = alias_count + 1
            while True:
                # Find next available sequence number
                alias = f"{alias}-{alias_sequence:03}"
                cursor.execute(
                    "SELECT COUNT(*), alias_of FROM _table_metadata where name like %s group by alias_of;",
                    (alias + "%",),
                )
                result = cursor.fetchone()
                alias_exists = result[0] if result else 0
                if not alias_exists:
                    break
                alias_sequence += 1
        elif alias_count == 1:
            # Drop existing alias
            context.logger.warning(
                f'Dropping existing alias "{alias}" for resource "{existing_alias_of}"...'
            )
            try:
                cursor.execute(
                    sql.SQL("DROP VIEW IF EXISTS {}").format(sql.Identifier(alias))
                )
            except psycopg2.Error as e:
                context.logger.warning(f"Could not drop alias/view: {e}")

        return alias

    def _create_summary_stats_resource(
        self, context: ProcessingContext, cursor: psycopg2.extensions.cursor
    ) -> None:
        """
        Create summary statistics resource.

        Args:
            context: Processing context
            cursor: Database cursor

        Raises:
            utils.JobError: If stats resource creation fails
        """
        # Check if we should create summary stats
        if not (conf.ADD_SUMMARY_STATS_RESOURCE or conf.SUMMARY_STATS_WITH_PREVIEW):
            return

        record_count = context.dataset_stats.get("RECORD_COUNT", 0)
        if not (conf.PREVIEW_ROWS == 0 or conf.SUMMARY_STATS_WITH_PREVIEW):
            # Skip if preview mode and not explicitly enabled
            return

        stats_resource_id = context.resource_id + "-stats"

        # Delete existing stats resource
        self._delete_existing_stats(context, cursor, stats_resource_id)

        # Prepare aliases for stats resource
        stats_aliases = [stats_resource_id]
        if conf.AUTO_ALIAS:
            # Get base alias from main resource
            package = dsu.get_package(context.resource["package_id"])
            resource_name = context.resource.get("name")
            package_name = package.get("name")
            owner_org = package.get("organization")
            owner_org_name = owner_org.get("name") if owner_org else ""
            base_alias = f"{resource_name}-{package_name}-{owner_org_name}"[:55]

            auto_alias_stats_id = base_alias + "-stats"
            stats_aliases.append(auto_alias_stats_id)

            # Delete existing auto-aliased stats
            self._delete_existing_stats(context, cursor, auto_alias_stats_id)

        # Infer stats schema
        qsv_stats_csv = os.path.join(context.temp_dir, "qsv_stats.csv")
        stats_stats_dict = self._infer_stats_schema(context, qsv_stats_csv)

        # Create stats resource
        resource_name = context.resource.get("name")
        stats_resource = {
            "package_id": context.resource["package_id"],
            "name": resource_name + " - Summary Statistics",
            "format": "CSV",
            "mimetype": "text/csv",
        }

        stats_response = dsu.send_resource_to_datastore(
            stats_resource,
            resource_id=None,
            headers=stats_stats_dict,
            records=None,
            aliases=stats_aliases,
            calculate_record_count=False,
        )

        context.logger.info(f"stats_response: {stats_response}")

        new_stats_resource_id = stats_response["result"]["resource_id"]

        # Copy stats data to datastore
        self._copy_stats_to_datastore(
            context, cursor, qsv_stats_csv, new_stats_resource_id, stats_stats_dict
        )

        # Update stats resource metadata
        stats_resource["id"] = new_stats_resource_id
        stats_resource["summary_statistics"] = True
        stats_resource["summary_of_resource"] = context.resource_id
        dsu.update_resource(stats_resource)

    def _delete_existing_stats(
        self,
        context: ProcessingContext,
        cursor: psycopg2.extensions.cursor,
        stats_id: str,
    ) -> None:
        """
        Delete existing stats resource if it exists.

        Args:
            context: Processing context
            cursor: Database cursor
            stats_id: Stats resource ID or alias
        """
        existing_stats = dsu.datastore_resource_exists(stats_id)
        if existing_stats:
            context.logger.info(f'Deleting existing summary stats "{stats_id}".')

            cursor.execute(
                "SELECT alias_of FROM _table_metadata where name like %s group by alias_of;",
                (stats_id + "%",),
            )
            stats_alias_result = cursor.fetchone()

            if stats_alias_result:
                existing_stats_alias_of = stats_alias_result[0]
                dsu.delete_datastore_resource(existing_stats_alias_of)
                dsu.delete_resource(existing_stats_alias_of)

    def _infer_stats_schema(
        self, context: ProcessingContext, qsv_stats_csv: str
    ) -> list:
        """
        Infer schema for stats CSV.

        Args:
            context: Processing context
            qsv_stats_csv: Path to stats CSV

        Returns:
            List of stats field dictionaries

        Raises:
            utils.JobError: If schema inference fails
        """
        try:
            qsv_stats_stats = context.qsv.stats(
                qsv_stats_csv,
                typesonly=True,
            )
        except utils.JobError as e:
            raise utils.JobError(f"Cannot run stats on CSV stats: {e}")

        stats_stats = str(qsv_stats_stats.stdout).strip()
        stats_stats_dict = [
            dict(id=ele.split(",")[0], type=conf.TYPE_MAPPING[ele.split(",")[1]])
            for idx, ele in enumerate(stats_stats.splitlines()[1:], 1)
        ]

        context.logger.info(f"stats_stats_dict: {stats_stats_dict}")

        return stats_stats_dict

    def _copy_stats_to_datastore(
        self,
        context: ProcessingContext,
        cursor: psycopg2.extensions.cursor,
        qsv_stats_csv: str,
        stats_resource_id: str,
        stats_stats_dict: list,
    ) -> None:
        """
        Copy stats data to datastore.

        Args:
            context: Processing context
            cursor: Database cursor
            qsv_stats_csv: Path to stats CSV
            stats_resource_id: Stats resource ID
            stats_stats_dict: Stats schema

        Raises:
            utils.JobError: If COPY fails
        """
        col_names_list = [h["id"] for h in stats_stats_dict]
        stats_aliases_str = f"{stats_resource_id}, ..."

        context.logger.info(
            f'ADDING SUMMARY STATISTICS {col_names_list} in "{stats_resource_id}" '
            f'with alias/es "{stats_aliases_str}"...'
        )

        column_names = sql.SQL(",").join(sql.Identifier(c) for c in col_names_list)

        copy_sql = sql.SQL(
            "COPY {} ({}) FROM STDIN WITH (FORMAT CSV, HEADER 1, ENCODING 'UTF8');"
        ).format(
            sql.Identifier(stats_resource_id),
            column_names,
        )

        with open(qsv_stats_csv, "rb") as f:
            try:
                cursor.copy_expert(copy_sql, f)
            except psycopg2.Error as e:
                raise utils.JobError(f"Postgres COPY failed: {e}")

    def _update_resource_metadata(self, context: ProcessingContext) -> None:
        """
        Update resource metadata fields.

        Args:
            context: Processing context
        """
        record_count = context.dataset_stats.get("RECORD_COUNT", 0)

        context.resource["datastore_active"] = True
        context.resource["total_record_count"] = record_count

        if conf.PREVIEW_ROWS < record_count or (conf.PREVIEW_ROWS > 0):
            context.resource["preview"] = True
            context.resource["preview_rows"] = context.copied_count
        else:
            context.resource["preview"] = False
            context.resource["preview_rows"] = None
            context.resource["partial_download"] = False

        dsu.update_resource(context.resource)
