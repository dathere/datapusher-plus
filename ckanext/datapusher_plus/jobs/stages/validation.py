# -*- coding: utf-8 -*-
"""
Validation stage for the DataPusher Plus pipeline.

Handles CSV validation and deduplication.
"""

import os
import json
import subprocess
from typing import Dict, Any, Union

import ckanext.datapusher_plus.utils as utils
import ckanext.datapusher_plus.config as conf
from ckanext.datapusher_plus.jobs.stages.base import BaseStage
from ckanext.datapusher_plus.jobs.context import ProcessingContext


class ValidationStage(BaseStage):
    """
    Validates CSV file and performs deduplication.

    Responsibilities:
    - Validate CSV against RFC4180 standard
    - Check if CSV is sorted
    - Count duplicates
    - Deduplicate if needed
    """

    def __init__(self):
        super().__init__(name="Validation")

    def process(self, context: ProcessingContext) -> ProcessingContext:
        """
        Validate CSV and deduplicate if needed.

        Args:
            context: Processing context

        Returns:
            Updated context

        Raises:
            utils.JobError: If validation fails
        """
        # Validate CSV
        self._validate_csv(context)

        # Check for duplicates and sort order
        dupe_count = 0
        if conf.SORT_AND_DUPE_CHECK or conf.DEDUP:
            dupe_count = self._check_duplicates(context)

        # Deduplicate if needed
        if conf.DEDUP and dupe_count > 0:
            self._deduplicate(context, dupe_count)
        else:
            context.add_stat("DEDUPED", False)

        return context

    def _validate_csv(self, context: ProcessingContext) -> None:
        """
        Validate CSV against RFC4180 standard.

        Strict validation runs first. When it fails, the stage retries
        with quarantine capture — qsv writes the rejected rows to a
        sibling ``<input>.invalid.csv`` and the clean subset to
        ``<input>.valid.csv``. The clean file becomes the new ``tmp``
        for downstream tasks; the count and path of quarantined rows
        are recorded on the context for ``validate_task`` to enforce
        ``max_quarantine_pct`` and attach the Prefect artifact.

        Args:
            context: Processing context

        Raises:
            utils.JobError: If validation cannot complete even with
                quarantine capture enabled.
        """
        from pathlib import Path

        context.logger.info("Validating CSV...")
        try:
            context.qsv.validate(context.tmp)
            context.logger.info("Well-formed, valid CSV file confirmed...")
            return
        except utils.JobError as strict_err:
            context.logger.warning(
                f"Strict validation failed ({strict_err}); retrying with "
                "quarantine capture so the clean rows can still be ingested"
            )

        # Quarantine pass.
        try:
            context.qsv.validate(
                context.tmp, invalid_suffix="invalid", valid_suffix="valid"
            )
        except utils.JobError as e:
            raise utils.JobError(f"qsv validate failed (quarantine pass): {e}")

        valid_path = f"{context.tmp}.valid.csv"
        invalid_path = f"{context.tmp}.invalid.csv"
        if Path(invalid_path).is_file():
            with open(invalid_path) as f:
                # First line is the header; remaining lines are
                # quarantined rows.
                line_count = sum(1 for _ in f)
            context.quarantined_rows = max(0, line_count - 1)
            context.quarantine_csv_path = invalid_path
            context.logger.info(
                f"Quarantined {context.quarantined_rows} rejected rows to "
                f"{invalid_path}"
            )
        if Path(valid_path).is_file():
            context.update_tmp(valid_path)
        context.logger.info(
            "Validation complete (some rows quarantined)"
            if context.quarantined_rows
            else "Well-formed, valid CSV file confirmed..."
        )

    def _check_duplicates(self, context: ProcessingContext) -> int:
        """
        Check for duplicates and if CSV is sorted.

        Args:
            context: Processing context

        Returns:
            Number of duplicates found

        Raises:
            utils.JobError: If sortcheck fails
        """
        context.logger.info("Checking for duplicates and if the CSV is sorted...")

        try:
            qsv_sortcheck = context.qsv.sortcheck(
                context.tmp, json_output=True, uses_stdio=True
            )
        except utils.JobError as e:
            raise utils.JobError(
                f"Failed to check if CSV is sorted and has duplicates: {e}"
            )

        # Parse sortcheck output
        sortcheck_json = self._parse_sortcheck_output(qsv_sortcheck)

        # Extract and store statistics
        is_sorted = bool(sortcheck_json.get("sorted", False))
        unsorted_breaks = int(sortcheck_json.get("unsorted_breaks", 0))
        dupe_count = int(sortcheck_json.get("dupe_count", 0))

        context.add_stat("IS_SORTED", is_sorted)
        # NOTE: `qsv sortcheck`'s `record_count` field has historically counted
        # the header row inconsistently across qsv versions (qsv ≤ 9.1.0
        # included the header; qsv ≥ 10.0.0 does not). Don't populate
        # RECORD_COUNT from it — let AnalysisStage compute the canonical
        # count via `qsv count` (which is data-rows-only across all versions).
        context.add_stat("UNSORTED_BREAKS", unsorted_breaks)
        context.add_stat("DUPE_COUNT", dupe_count)

        # Format log message
        sortcheck_msg = f"Sorted: {is_sorted}; Unsorted breaks: {unsorted_breaks:,}"
        if is_sorted and dupe_count > 0:
            sortcheck_msg = f"{sortcheck_msg}; Duplicates: {dupe_count:,}"

        context.logger.info(sortcheck_msg)

        return dupe_count

    def _parse_sortcheck_output(
        self, qsv_sortcheck: Union[subprocess.CompletedProcess, Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Parse sortcheck JSON output.

        Args:
            qsv_sortcheck: Output from qsv sortcheck command

        Returns:
            Parsed JSON dictionary

        Raises:
            utils.JobError: If parsing fails
        """
        try:
            # Handle both subprocess.CompletedProcess and dict outputs
            stdout_content = (
                qsv_sortcheck.stdout
                if hasattr(qsv_sortcheck, "stdout")
                else qsv_sortcheck.get("stdout")
            )
            sortcheck_json = json.loads(str(stdout_content))
        except (json.JSONDecodeError, AttributeError) as e:
            raise utils.JobError(f"Failed to parse sortcheck JSON output: {e}")

        # Validate required fields
        try:
            # Ensure numeric values are valid
            int(sortcheck_json.get("record_count", 0))
            int(sortcheck_json.get("unsorted_breaks", 0))
            int(sortcheck_json.get("dupe_count", 0))
        except (ValueError, TypeError) as e:
            raise utils.JobError(f"Invalid numeric value in sortcheck output: {e}")

        return sortcheck_json

    def _deduplicate(self, context: ProcessingContext, dupe_count: int) -> None:
        """
        Deduplicate the CSV file.

        Args:
            context: Processing context
            dupe_count: Number of duplicates found

        Raises:
            utils.JobError: If deduplication fails
        """
        qsv_dedup_csv = os.path.join(context.temp_dir, "qsv_dedup.csv")
        context.logger.info(f"{dupe_count} duplicate rows found. Deduping...")

        try:
            context.qsv.extdedup(context.tmp, qsv_dedup_csv)
        except utils.JobError as e:
            raise utils.JobError(f"Check for duplicates error: {e}")

        context.add_stat("DEDUPED", True)
        context.update_tmp(qsv_dedup_csv)
        context.logger.info(f"Deduped CSV saved to {qsv_dedup_csv}")
