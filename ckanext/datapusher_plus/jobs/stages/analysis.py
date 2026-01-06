# -*- coding: utf-8 -*-
"""
Analysis stage for the DataPusher Plus pipeline.

Handles type inference, statistics, frequency tables, and PII screening.
"""

import os
import csv
import time
import json
from typing import List, Dict, Any

import ckanext.datapusher_plus.utils as utils
import ckanext.datapusher_plus.config as conf
import ckanext.datapusher_plus.datastore_utils as dsu
from ckanext.datapusher_plus.pii_screening import screen_for_pii
from ckanext.datapusher_plus.jobs.stages.base import BaseStage
from ckanext.datapusher_plus.jobs.context import ProcessingContext


class AnalysisStage(BaseStage):
    """
    Analyzes CSV file to infer types and generate statistics.

    Responsibilities:
    - Extract and sanitize headers
    - Infer data types
    - Generate statistics
    - Create frequency tables
    - Generate preview if needed
    - Normalize dates to RFC3339
    - Screen for PII
    """

    def __init__(self):
        super().__init__(name="Analysis")

    def process(self, context: ProcessingContext) -> ProcessingContext:
        """
        Analyze CSV file and infer schema.

        Args:
            context: Processing context

        Returns:
            Updated context with schema information

        Raises:
            utils.JobError: If analysis fails
        """
        analysis_start = time.perf_counter()

        # Extract headers and sanitize
        original_header_dict = self._extract_headers(context)
        self._sanitize_headers(context)

        # Create index for faster operations
        self._create_index(context)

        # Get record count if not already available
        record_count = context.dataset_stats.get("RECORD_COUNT")
        if not record_count:
            record_count = self._count_records(context)

        # Check if empty
        if record_count == 0:
            context.logger.warning("Upload skipped as there are zero records.")
            return None

        # Log record count
        unique_qualifier = "unique" if conf.DEDUP else ""
        context.logger.info(f"{record_count} {unique_qualifier} records detected...")

        # Infer types and generate statistics
        headers_dicts, datetimecols_list, resource_fields_stats = (
            self._infer_types_and_stats(context, original_header_dict)
        )

        # Store headers in context
        context.headers_dicts = headers_dicts
        context.headers = [h["id"] for h in headers_dicts]
        context.original_header_dict = original_header_dict

        # Generate frequency tables
        resource_fields_freqs = self._generate_frequency_tables(context)

        # Update field stats with frequency data
        for field, freqs in resource_fields_freqs.items():
            if field in resource_fields_stats:
                resource_fields_stats[field]["freqs"] = freqs

        # Store field stats in context for FormulaStage
        context.resource_fields_stats = resource_fields_stats
        context.resource_fields_freqs = resource_fields_freqs

        # Generate preview if needed
        context.rows_to_copy = record_count
        if conf.PREVIEW_ROWS and record_count > conf.PREVIEW_ROWS:
            context.rows_to_copy = self._generate_preview(context, record_count)

        # Normalize dates to RFC3339
        if datetimecols_list:
            self._normalize_dates(context, datetimecols_list)

        # Analysis complete
        analysis_elapsed = time.perf_counter() - analysis_start
        context.logger.info(
            f"ANALYSIS DONE! Analyzed and prepped in {analysis_elapsed:,.2f} seconds."
        )

        # PII Screening
        self._screen_pii(context)

        # Remove index file
        if context.qsv_index_file and os.path.exists(context.qsv_index_file):
            os.remove(context.qsv_index_file)

        return context

    def _extract_headers(self, context: ProcessingContext) -> Dict[int, str]:
        """
        Extract original headers from CSV.

        Args:
            context: Processing context

        Returns:
            Dictionary mapping column index to original header name

        Raises:
            utils.JobError: If headers cannot be extracted
        """
        try:
            qsv_headers = context.qsv.headers(context.tmp, just_names=True)
        except utils.JobError as e:
            raise utils.JobError(f"Cannot scan CSV headers: {e}")

        original_headers = str(qsv_headers.stdout).strip()
        original_header_dict = {
            idx: ele for idx, ele in enumerate(original_headers.splitlines())
        }
        return original_header_dict

    def _sanitize_headers(self, context: ProcessingContext) -> None:
        """
        Sanitize headers to be database-safe.

        Args:
            context: Processing context

        Raises:
            utils.JobError: If header sanitization fails
        """
        context.logger.info('Checking for "database-safe" header names...')

        try:
            qsv_safenames = context.qsv.safenames(
                context.tmp,
                mode="json",
                reserved=conf.RESERVED_COLNAMES,
                prefix=conf.UNSAFE_PREFIX,
                uses_stdio=True,
            )
        except utils.JobError as e:
            raise utils.JobError(f"Cannot scan CSV headers: {e}")

        unsafe_json = json.loads(str(qsv_safenames.stdout))
        unsafe_headers = unsafe_json["unsafe_headers"]

        if unsafe_headers:
            context.logger.info(
                f'"{len(unsafe_headers)} unsafe" header names found '
                f"({unsafe_headers}). Sanitizing...\""
            )
            qsv_safenames_csv = os.path.join(context.temp_dir, "qsv_safenames.csv")
            context.qsv.safenames(
                context.tmp, mode="conditional", output_file=qsv_safenames_csv
            )
            context.update_tmp(qsv_safenames_csv)
        else:
            context.logger.info("No unsafe header names found...")

    def _create_index(self, context: ProcessingContext) -> None:
        """
        Create QSV index for faster operations.

        Args:
            context: Processing context

        Raises:
            utils.JobError: If index creation fails
        """
        try:
            context.qsv_index_file = context.tmp + ".idx"
            context.qsv.index(context.tmp)
        except utils.JobError as e:
            raise utils.JobError(f"Cannot index CSV: {e}")

    def _count_records(self, context: ProcessingContext) -> int:
        """
        Count records in CSV.

        Args:
            context: Processing context

        Returns:
            Number of records

        Raises:
            utils.JobError: If counting fails
        """
        try:
            qsv_count = context.qsv.count(context.tmp)
            record_count = int(str(qsv_count.stdout).strip())
            context.add_stat("RECORD_COUNT", record_count)
            return record_count
        except utils.JobError as e:
            raise utils.JobError(f"Cannot count records in CSV: {e}")

    def _infer_types_and_stats(
        self, context: ProcessingContext, original_header_dict: Dict[int, str]
    ) -> tuple[List[Dict[str, Any]], List[str], Dict[str, Any]]:
        """
        Infer data types and compile statistics.

        Args:
            context: Processing context
            original_header_dict: Mapping of column index to original header

        Returns:
            Tuple of (headers_dicts, datetimecols_list, resource_fields_stats)

        Raises:
            utils.JobError: If type inference fails
        """
        context.logger.info("Inferring data types and compiling statistics...")

        qsv_stats_csv = os.path.join(context.temp_dir, "qsv_stats.csv")

        # Determine if we need special handling for spatial formats
        spatial_format_flag = context.resource.get("format", "").upper() in [
            "SHP",
            "QGIS",
            "GEOJSON",
        ]

        # Run qsv stats
        try:
            if spatial_format_flag:
                env = os.environ.copy()
                env["QSV_STATS_STRING_MAX_LENGTH"] = str(
                    conf.QSV_STATS_STRING_MAX_LENGTH
                )
                context.qsv.stats(
                    context.tmp,
                    infer_dates=True,
                    dates_whitelist=conf.QSV_DATES_WHITELIST,
                    stats_jsonl=True,
                    prefer_dmy=conf.PREFER_DMY,
                    cardinality=bool(conf.AUTO_INDEX_THRESHOLD),
                    summary_stats_options=conf.SUMMARY_STATS_OPTIONS,
                    output_file=qsv_stats_csv,
                    env=env,
                )
            else:
                context.qsv.stats(
                    context.tmp,
                    infer_dates=True,
                    dates_whitelist=conf.QSV_DATES_WHITELIST,
                    stats_jsonl=True,
                    prefer_dmy=conf.PREFER_DMY,
                    cardinality=bool(conf.AUTO_INDEX_THRESHOLD),
                    summary_stats_options=conf.SUMMARY_STATS_OPTIONS,
                    output_file=qsv_stats_csv,
                )
        except utils.JobError as e:
            raise utils.JobError(f"Cannot infer data types and compile statistics: {e}")

        # Parse stats
        return self._parse_stats(
            context, qsv_stats_csv, original_header_dict
        )

    def _parse_stats(
        self,
        context: ProcessingContext,
        stats_csv: str,
        original_header_dict: Dict[int, str],
    ) -> tuple[List[Dict[str, Any]], List[str], Dict[str, Any]]:
        """
        Parse statistics CSV and build headers dictionary.

        Args:
            context: Processing context
            stats_csv: Path to stats CSV
            original_header_dict: Mapping of column index to original header

        Returns:
            Tuple of (headers_dicts, datetimecols_list, resource_fields_stats)
        """
        headers = []
        types = []
        headers_min = []
        headers_max = []
        headers_cardinality = []
        resource_fields_stats = {}

        with open(stats_csv, mode="r") as inp:
            reader = csv.DictReader(inp)
            for row in reader:
                # Add to stats dictionary
                resource_fields_stats[row["field"]] = {"stats": row}

                fr = {k: v for k, v in row.items()}
                schema_field = fr.get("field", "Unnamed Column")
                if schema_field.startswith("qsv_"):
                    break

                headers.append(schema_field)
                types.append(fr.get("type", "String"))
                headers_min.append(fr["min"])
                headers_max.append(fr["max"])
                if conf.AUTO_INDEX_THRESHOLD:
                    headers_cardinality.append(int(fr.get("cardinality") or 0))

        # Store cardinality for indexing stage
        if conf.AUTO_INDEX_THRESHOLD:
            context.add_stat("HEADERS_CARDINALITY", headers_cardinality)

        # Check for existing datastore resource
        existing = dsu.datastore_resource_exists(context.resource_id)
        context.existing_info = None
        if existing:
            context.existing_info = dict(
                (f["id"], f["info"]) for f in existing.get("fields", []) if "info" in f
            )

        # Override with types from Data Dictionary
        if context.existing_info:
            types = [
                {
                    "text": "String",
                    "numeric": "Float",
                    "timestamp": "DateTime",
                }.get(context.existing_info.get(h, {}).get("type_override"), t)
                for t, h in zip(types, headers)
            ]

        # Delete existing datastore resource
        if existing:
            context.logger.info(
                f'Deleting existing resource "{context.resource_id}" from datastore.'
            )
            dsu.delete_datastore_resource(context.resource_id)

        # Build headers_dicts
        headers_dicts, datetimecols_list = self._build_headers_dicts(
            context, headers, types, headers_min, headers_max, original_header_dict
        )

        context.logger.info(f"Determined headers and types: {headers_dicts}...")

        return headers_dicts, datetimecols_list, resource_fields_stats

    def _build_headers_dicts(
        self,
        context: ProcessingContext,
        headers: List[str],
        types: List[str],
        headers_min: List[str],
        headers_max: List[str],
        original_header_dict: Dict[int, str],
    ) -> tuple[List[Dict[str, Any]], List[str]]:
        """
        Build headers dictionaries with proper types.

        Args:
            context: Processing context
            headers: List of header names
            types: List of inferred types
            headers_min: List of minimum values
            headers_max: List of maximum values
            original_header_dict: Mapping of column index to original header

        Returns:
            Tuple of (headers_dicts, datetimecols_list)
        """
        default_type = "String"
        temp_headers_dicts = [
            dict(
                id=field[0],
                type=conf.TYPE_MAPPING.get(
                    str(field[1]) if field[1] else default_type, "text"
                ),
            )
            for field in zip(headers, types)
        ]

        # Build final headers_dicts with smartint resolution
        datetimecols_list = []
        headers_dicts = []

        for idx, header in enumerate(temp_headers_dicts):
            if header["type"] == "smartint":
                # Select best integer type based on min/max
                if (
                    int(headers_max[idx]) <= conf.POSTGRES_INT_MAX
                    and int(headers_min[idx]) >= conf.POSTGRES_INT_MIN
                ):
                    header_type = "integer"
                elif (
                    int(headers_max[idx]) <= conf.POSTGRES_BIGINT_MAX
                    and int(headers_min[idx]) >= conf.POSTGRES_BIGINT_MIN
                ):
                    header_type = "bigint"
                else:
                    header_type = "numeric"
            else:
                header_type = header["type"]

            if header_type == "timestamp":
                datetimecols_list.append(header["id"])

            info_dict = dict(label=original_header_dict.get(idx, "Unnamed Column"))
            headers_dicts.append(
                dict(id=header["id"], type=header_type, info=info_dict)
            )

        # Preserve data dictionary from existing resource
        if context.existing_info:
            for h in headers_dicts:
                if h["id"] in context.existing_info:
                    h["info"] = context.existing_info[h["id"]]
                    # Apply type overrides
                    type_override = context.existing_info[h["id"]].get("type_override")
                    if type_override in list(conf.TYPE_MAPPING.values()):
                        h["type"] = type_override

        return headers_dicts, datetimecols_list

    def _generate_frequency_tables(
        self, context: ProcessingContext
    ) -> Dict[str, List[Dict[str, str]]]:
        """
        Generate frequency tables for each column.

        Args:
            context: Processing context

        Returns:
            Dictionary mapping field names to frequency data

        Raises:
            utils.JobError: If frequency table generation fails
        """
        qsv_freq_csv = os.path.join(context.temp_dir, "qsv_freq.csv")

        try:
            context.qsv.frequency(
                context.tmp, limit=conf.QSV_FREQ_LIMIT, output_file=qsv_freq_csv
            )
        except utils.JobError as e:
            raise utils.JobError(f"Cannot create a frequency table: {e}")

        resource_fields_freqs = {}
        try:
            with open(qsv_freq_csv, "r") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    field = row["field"]
                    if field not in resource_fields_freqs:
                        resource_fields_freqs[field] = []

                    resource_fields_freqs[field].append(
                        {
                            "value": row["value"],
                            "count": row["count"],
                            "percentage": row["percentage"],
                        }
                    )
            context.logger.log(5, f"Resource fields freqs: {resource_fields_freqs}")
        except IOError as e:
            raise utils.JobError(f"Could not open frequency CSV file: {e}")

        return resource_fields_freqs

    def _generate_preview(self, context: ProcessingContext, record_count: int) -> int:
        """
        Generate a preview slice of the data.

        Args:
            context: Processing context
            record_count: Total number of records

        Returns:
            Number of rows in preview

        Raises:
            utils.JobError: If preview generation fails
        """
        qsv_slice_csv = os.path.join(context.temp_dir, "qsv_slice.csv")

        if conf.PREVIEW_ROWS > 0:
            # Positive: slice from beginning
            context.logger.info(f"Preparing {conf.PREVIEW_ROWS}-row preview...")
            try:
                context.qsv.slice(
                    context.tmp, length=conf.PREVIEW_ROWS, output_file=qsv_slice_csv
                )
            except utils.JobError as e:
                raise utils.JobError(f"Cannot create a preview slice: {e}")
            rows_to_copy = conf.PREVIEW_ROWS
        else:
            # Negative: slice from end
            slice_len = abs(conf.PREVIEW_ROWS)
            context.logger.info(f"Preparing {slice_len}-row preview from the end...")
            try:
                context.qsv.slice(
                    context.tmp, start=-1, length=slice_len, output_file=qsv_slice_csv
                )
            except utils.JobError as e:
                raise utils.JobError(f"Cannot create a preview slice from the end: {e}")
            rows_to_copy = slice_len

        context.update_tmp(qsv_slice_csv)
        context.add_stat("PREVIEW_FILE_SIZE", os.path.getsize(qsv_slice_csv))
        context.add_stat("PREVIEW_RECORD_COUNT", rows_to_copy)

        return rows_to_copy

    def _normalize_dates(
        self, context: ProcessingContext, datetimecols_list: List[str]
    ) -> None:
        """
        Normalize date columns to RFC3339 format.

        Args:
            context: Processing context
            datetimecols_list: List of datetime column names

        Raises:
            utils.JobError: If date normalization fails
        """
        qsv_applydp_csv = os.path.join(context.temp_dir, "qsv_applydp.csv")
        datecols = ",".join(datetimecols_list)

        context.logger.info(
            f'Formatting dates "{datecols}" to ISO 8601/RFC 3339 format '
            f"with PREFER_DMY: {conf.PREFER_DMY}..."
        )

        try:
            context.qsv.datefmt(
                datecols,
                context.tmp,
                prefer_dmy=conf.PREFER_DMY,
                output_file=qsv_applydp_csv,
            )
        except utils.JobError as e:
            raise utils.JobError(f"Applydp error: {e}")

        context.update_tmp(qsv_applydp_csv)

    def _screen_pii(self, context: ProcessingContext) -> None:
        """
        Screen for Personally Identifiable Information.

        Args:
            context: Processing context
        """
        if conf.PII_SCREENING:
            piiscreening_start = time.perf_counter()
            context.pii_found = screen_for_pii(
                context.tmp,
                context.resource,
                context.qsv,
                context.temp_dir,
                context.logger,
            )
            piiscreening_elapsed = time.perf_counter() - piiscreening_start
            context.logger.info(
                f"PII screening completed in {piiscreening_elapsed:,.2f} seconds"
            )

        context.add_stat("PII_SCREENING", conf.PII_SCREENING)
        context.add_stat("PII_FOUND", context.pii_found)
