# -*- coding: utf-8 -*-
"""
Indexing stage for the DataPusher Plus pipeline.

Handles automatic index creation based on cardinality and configuration.
"""

import time
import psycopg2
from psycopg2 import sql
from typing import List

import ckanext.datapusher_plus.utils as utils
import ckanext.datapusher_plus.config as conf
from ckanext.datapusher_plus.jobs.stages.base import BaseStage
from ckanext.datapusher_plus.jobs.context import ProcessingContext


class IndexingStage(BaseStage):
    """
    Creates database indexes automatically based on cardinality.

    Responsibilities:
    - Create unique indexes for columns with all unique values
    - Create regular indexes for low-cardinality columns
    - Create indexes on date columns if configured
    - Optimize table with VACUUM ANALYZE
    """

    def __init__(self):
        super().__init__(name="Indexing")

    def should_skip(self, context: ProcessingContext) -> bool:
        """
        Skip indexing if not configured.

        Args:
            context: Processing context

        Returns:
            True if indexing should be skipped
        """
        # Get date-like columns (Postgres ``timestamp`` or ``date``).
        date_like_cols_list = self._get_date_like_columns(context)

        return not (
            conf.AUTO_INDEX_THRESHOLD
            or (conf.AUTO_INDEX_DATES and date_like_cols_list)
            or conf.AUTO_UNIQUE_INDEX
        )

    def process(self, context: ProcessingContext) -> ProcessingContext:
        """
        Create database indexes.

        Args:
            context: Processing context

        Returns:
            Updated context

        Raises:
            utils.JobError: If indexing fails
        """
        index_start = time.perf_counter()

        # Get date-like columns (Postgres ``timestamp`` or ``date``).
        date_like_cols_list = self._get_date_like_columns(context)

        context.logger.info(
            f"AUTO-INDEXING. Auto-index threshold range: "
            f"[{conf.AUTO_INDEX_MIN_THRESHOLD}, {conf.AUTO_INDEX_THRESHOLD}] "
            f"unique value/s. Auto-unique index: {conf.AUTO_UNIQUE_INDEX} "
            f"Auto-index dates: {conf.AUTO_INDEX_DATES} ..."
        )

        # Get cardinality data
        headers_cardinality = context.dataset_stats.get("HEADERS_CARDINALITY", [])
        record_count = context.dataset_stats.get("RECORD_COUNT", 0)

        # Adjust threshold if set to -1 (index all columns)
        auto_index_threshold = conf.AUTO_INDEX_THRESHOLD
        if auto_index_threshold == -1:
            auto_index_threshold = record_count

        auto_index_min_threshold = conf.AUTO_INDEX_MIN_THRESHOLD
        # Issue #142: a MIN > MAX range yields zero indexes — flag that
        # explicitly so operators don't have to puzzle out why nothing
        # got indexed when they expected indexes.
        #
        # Two cases we deliberately DON'T warn on:
        # - ``conf.AUTO_INDEX_THRESHOLD == 0`` — the operator explicitly
        #   disabled cardinality-based indexing; warning would be noise.
        # - ``auto_index_threshold == 0`` *after* the ``-1`` →
        #   ``record_count`` remap (i.e. ``-1`` on a zero-row dataset).
        #   The operator asked for "index every column" on an empty
        #   table; flagging that as misconfiguration would be
        #   confusing (caught by roborev #2275 LOW).
        if (
            auto_index_min_threshold > auto_index_threshold
            and conf.AUTO_INDEX_THRESHOLD
            and auto_index_threshold > 0
        ):
            context.logger.warning(
                f"Auto-index range is empty: "
                f"min_threshold ({auto_index_min_threshold}) > "
                f"threshold ({conf.AUTO_INDEX_THRESHOLD}). "
                "No cardinality-based indexes will be created. Date / "
                "unique-index auto-creation is unaffected."
            )

        # Create indexes
        index_count = self._create_indexes(
            context,
            headers_cardinality,
            date_like_cols_list,
            record_count,
            auto_index_threshold,
            auto_index_min_threshold,
        )

        index_elapsed = time.perf_counter() - index_start
        context.logger.info(
            f'...indexing/vacuum analysis done. Indexed {index_count} column/s '
            f'in "{context.resource_id}" in {index_elapsed:,.2f} seconds.'
        )

        return context

    def _get_date_like_columns(self, context: ProcessingContext) -> List[str]:
        """
        Extract date / datetime column names from headers_dicts.

        Both Postgres ``timestamp`` and ``date`` columns count as
        "date-like" for auto-indexing purposes — the ``AUTO_INDEX_DATES``
        knob means "index columns that represent points in time",
        whether or not those points include a time-of-day component.
        Pre-#179, only ``timestamp`` columns existed (the buggy
        declaration default mapped qsv ``Date`` to ``timestamp``), so
        a single match was sufficient. After #179 a date-only column
        is Postgres ``date`` and would silently lose its auto-index
        unless we include ``"date"`` here.

        Args:
            context: Processing context

        Returns:
            List of date / datetime column names
        """
        date_like_cols_list = []
        for header in context.headers_dicts:
            if header.get("type") in ("timestamp", "date"):
                date_like_cols_list.append(header["id"])
        return date_like_cols_list

    def _create_indexes(
        self,
        context: ProcessingContext,
        headers_cardinality: List[int],
        date_like_cols_list: List[str],
        record_count: int,
        auto_index_threshold: int,
        auto_index_min_threshold: int,
    ) -> int:
        """
        Create indexes on appropriate columns.

        A regular index is created when a column's cardinality falls in
        the closed range ``[auto_index_min_threshold,
        auto_index_threshold]``. See ``config.py`` and issue #142 for
        the design rationale (Postgres planner rejects very-low-
        cardinality indexes; DataTables-style filtering wants the small
        enum range).

        Args:
            context: Processing context
            headers_cardinality: List of cardinality values for each column
            date_like_cols_list: List of date-like column names
                (Postgres ``timestamp`` or ``date``)
            record_count: Total number of records
            auto_index_threshold: Upper-bound cardinality (inclusive) for
                auto-indexing
            auto_index_min_threshold: Lower-bound cardinality (inclusive)
                for auto-indexing — skips useless indexes on single-value
                columns

        Returns:
            Number of indexes created

        Raises:
            utils.JobError: If database connection fails
        """
        try:
            raw_connection = psycopg2.connect(conf.DATASTORE_WRITE_URL)
        except psycopg2.Error as e:
            raise utils.JobError(f"Could not connect to the Datastore: {e}")

        try:
            index_cur = raw_connection.cursor()
            index_count = 0

            # Iterate through columns
            for idx, cardinality in enumerate(headers_cardinality):
                if idx >= len(context.headers):
                    break

                curr_col = context.headers[idx]

                # Check if we should create a unique index
                if cardinality == record_count and conf.AUTO_UNIQUE_INDEX:
                    if self._create_unique_index(
                        context, index_cur, curr_col, cardinality
                    ):
                        index_count += 1

                # Check if we should create a regular index. Issue #142:
                # both ends of the range are inclusive; the MIN floor
                # skips useless single-value indexes. Date-like columns
                # bypass the cardinality range entirely (the
                # ``AUTO_INDEX_DATES`` knob means "always index temporal
                # columns regardless of how unique they are").
                elif (
                    auto_index_min_threshold <= cardinality <= auto_index_threshold
                ) or (
                    conf.AUTO_INDEX_DATES and (curr_col in date_like_cols_list)
                ):
                    if self._create_regular_index(
                        context, index_cur, curr_col, cardinality, date_like_cols_list
                    ):
                        index_count += 1

            index_cur.close()
            raw_connection.commit()

            # VACUUM ANALYZE to optimize indexes
            self._vacuum_analyze(context, raw_connection)

            return index_count

        finally:
            if raw_connection:
                raw_connection.close()

    def _create_unique_index(
        self,
        context: ProcessingContext,
        cursor: psycopg2.extensions.cursor,
        column: str,
        cardinality: int,
    ) -> bool:
        """
        Create a unique index on a column.

        Args:
            context: Processing context
            cursor: Database cursor
            column: Column name
            cardinality: Column cardinality

        Returns:
            True if index was created successfully, False otherwise
        """
        if conf.PREVIEW_ROWS > 0:
            unique_value_count = min(conf.PREVIEW_ROWS, cardinality)
        else:
            unique_value_count = cardinality

        context.logger.info(
            f'Creating UNIQUE index on "{column}" for {unique_value_count} unique values...'
        )

        try:
            cursor.execute(
                sql.SQL("CREATE UNIQUE INDEX ON {} ({})").format(
                    sql.Identifier(context.resource_id),
                    sql.Identifier(column),
                )
            )
            return True
        except psycopg2.Error as e:
            context.logger.warning(f'Could not CREATE UNIQUE INDEX on "{column}": {e}')
            return False

    def _create_regular_index(
        self,
        context: ProcessingContext,
        cursor: psycopg2.extensions.cursor,
        column: str,
        cardinality: int,
        date_like_cols_list: List[str],
    ) -> bool:
        """
        Create a regular index on a column.

        Args:
            context: Processing context
            cursor: Database cursor
            column: Column name
            cardinality: Column cardinality
            date_like_cols_list: List of date-like columns
                (Postgres ``timestamp`` or ``date``)

        Returns:
            True if index was created successfully, False otherwise
        """
        if column in date_like_cols_list:
            context.logger.info(
                f'Creating index on "{column}" date column for {cardinality} unique value/s...'
            )
        else:
            context.logger.info(
                f'Creating index on "{column}" for {cardinality} unique value/s...'
            )

        try:
            cursor.execute(
                sql.SQL("CREATE INDEX ON {} ({})").format(
                    sql.Identifier(context.resource_id),
                    sql.Identifier(column),
                )
            )
            return True
        except psycopg2.Error as e:
            context.logger.warning(f'Could not CREATE INDEX on "{column}": {e}')
            return False

    def _vacuum_analyze(
        self, context: ProcessingContext, connection: psycopg2.extensions.connection
    ) -> None:
        """
        Run VACUUM ANALYZE to optimize indexes.

        Args:
            context: Processing context
            connection: Database connection
        """
        context.logger.info("Vacuum Analyzing table to optimize indices...")

        connection.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
        )
        analyze_cur = connection.cursor()
        try:
            analyze_cur.execute(
                sql.SQL("VACUUM ANALYZE {}").format(sql.Identifier(context.resource_id))
            )
        finally:
            analyze_cur.close()
