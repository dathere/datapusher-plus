# -*- coding: utf-8 -*-
"""
Database stage for the DataPusher Plus pipeline.

Handles copying data to the PostgreSQL datastore.
"""

import time
import psycopg2
from psycopg2 import sql

import ckanext.datapusher_plus.utils as utils
import ckanext.datapusher_plus.config as conf
import ckanext.datapusher_plus.datastore_utils as dsu
from ckanext.datapusher_plus.jobs.stages.base import BaseStage
from ckanext.datapusher_plus.jobs.context import ProcessingContext


class DatabaseStage(BaseStage):
    """
    Copies data to PostgreSQL datastore.

    Responsibilities:
    - Create empty datastore table with schema
    - Use PostgreSQL COPY to efficiently load data
    - Run VACUUM ANALYZE for performance
    """

    def __init__(self):
        super().__init__(name="Database")

    def should_skip(self, context: ProcessingContext) -> bool:
        """Skip if in dry run mode."""
        return context.dry_run

    def process(self, context: ProcessingContext) -> ProcessingContext:
        """
        Copy data to datastore.

        Args:
            context: Processing context

        Returns:
            Updated context

        Raises:
            utils.JobError: If database operations fail
        """
        if context.dry_run:
            context.logger.warning(
                "Dry run only. Returning without copying to the Datastore..."
            )
            return context

        copy_start = time.perf_counter()

        if conf.PREVIEW_ROWS:
            context.logger.info(
                f"COPYING {context.rows_to_copy}-row preview to Datastore..."
            )
        else:
            context.logger.info(
                f"COPYING {context.rows_to_copy} rows to Datastore..."
            )

        # Create empty datastore table
        self._create_datastore_table(context)

        # Copy data using PostgreSQL COPY
        copied_count = self._copy_data(context)

        context.copied_count = copied_count

        copy_elapsed = time.perf_counter() - copy_start
        context.logger.info(
            f'...copying done. Copied {copied_count} rows to "{context.resource_id}" '
            f"in {copy_elapsed:,.2f} seconds."
        )

        return context

    def _create_datastore_table(self, context: ProcessingContext) -> None:
        """
        Create empty datastore table with schema.

        Args:
            context: Processing context
        """
        dsu.send_resource_to_datastore(
            resource=None,
            resource_id=context.resource["id"],
            headers=context.headers_dicts,
            records=None,
            aliases=None,
            calculate_record_count=False,
        )

    def _copy_data(self, context: ProcessingContext) -> int:
        """
        Copy data to datastore using PostgreSQL COPY.

        Args:
            context: Processing context

        Returns:
            Number of rows copied

        Raises:
            utils.JobError: If COPY operation fails
        """
        try:
            raw_connection = psycopg2.connect(conf.DATASTORE_WRITE_URL)
        except psycopg2.Error as e:
            raise utils.JobError(f"Could not connect to the Datastore: {e}")

        try:
            cur = raw_connection.cursor()

            # Truncate table for COPY FREEZE optimization
            self._truncate_table(cur, context.resource_id)

            # Prepare COPY SQL
            col_names_list = [h["id"] for h in context.headers_dicts]
            column_names = sql.SQL(",").join(sql.Identifier(c) for c in col_names_list)
            copy_sql = sql.SQL(
                "COPY {} ({}) FROM STDIN "
                "WITH (FORMAT CSV, FREEZE 1, "
                "HEADER 1, ENCODING 'UTF8');"
            ).format(
                sql.Identifier(context.resource_id),
                column_names,
            )

            # Execute COPY
            with open(context.tmp, "rb", conf.COPY_READBUFFER_SIZE) as f:
                try:
                    cur.copy_expert(copy_sql, f, size=conf.COPY_READBUFFER_SIZE)
                except psycopg2.Error as e:
                    raise utils.JobError(f"Postgres COPY failed: {e}")
                copied_count = cur.rowcount

            raw_connection.commit()

            # VACUUM ANALYZE for performance
            self._vacuum_analyze(raw_connection, context.resource_id)

            return copied_count

        finally:
            if raw_connection:
                raw_connection.close()

    def _truncate_table(self, cursor: psycopg2.extensions.cursor, resource_id: str) -> None:
        """
        Truncate table to enable COPY FREEZE optimization.

        Args:
            cursor: Database cursor
            resource_id: Resource ID (table name)
        """
        try:
            cursor.execute(
                sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(resource_id))
            )
        except psycopg2.Error as e:
            # Non-fatal, log warning but continue
            # (table might not exist yet)
            pass

    def _vacuum_analyze(
        self, connection: psycopg2.extensions.connection, resource_id: str
    ) -> None:
        """
        Run VACUUM ANALYZE on the table.

        Args:
            connection: Database connection
            resource_id: Resource ID (table name)
        """
        # Set isolation level for VACUUM
        connection.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
        )

        analyze_cur = connection.cursor()
        try:
            analyze_cur.execute(
                sql.SQL("VACUUM ANALYZE {}").format(sql.Identifier(resource_id))
            )
        finally:
            analyze_cur.close()
