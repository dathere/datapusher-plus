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
        Validate CSV against RFC 4180.

        Strict validation runs first (via ``qsv validate``). When it
        fails, the stage attempts a Python-side quarantine pass: parse
        the file row-by-row with the standard library's ``csv`` module,
        route rows whose field count diverges from the header into a
        sibling ``<input>.invalid.csv``, write the clean subset to
        ``<input>.valid.csv``, and re-validate the clean subset with qsv.

        qsv's own ``--valid`` / ``--invalid`` flags only emit output
        files in JSON-Schema mode, not in RFC 4180 mode — hence the
        Python-side pass. It covers the most common quarantine case
        (field-count mismatch); other RFC 4180 violations (bad encoding,
        unbalanced quotes mid-record) still fail the run and the
        operator must fix the source.

        Sets ``context.quarantined_rows`` and ``context.quarantine_csv_path``
        for downstream consumption by ``validate_task``.

        Args:
            context: Processing context

        Raises:
            utils.JobError: If validation cannot complete even after the
                quarantine pass (e.g., the clean subset still violates
                RFC 4180 for non-row-count reasons).
        """
        import csv
        from pathlib import Path

        context.logger.info("Validating CSV...")
        try:
            context.qsv.validate(context.tmp)
            context.logger.info("Well-formed, valid CSV file confirmed...")
            return
        except utils.JobError as strict_err:
            context.logger.warning(
                f"Strict RFC 4180 validation failed ({strict_err}); attempting "
                "Python-side quarantine of malformed rows"
            )

        # Python-side quarantine pass.
        src = context.tmp
        valid_path = f"{src}.valid.csv"
        invalid_path = f"{src}.invalid.csv"
        quarantined = 0
        valid_count = 0

        try:
            with open(src, newline="", encoding="utf-8") as fh_in, \
                 open(valid_path, "w", newline="", encoding="utf-8") as fh_valid, \
                 open(invalid_path, "w", newline="", encoding="utf-8") as fh_invalid:

                reader = csv.reader(fh_in)
                valid_writer = csv.writer(fh_valid)
                invalid_writer = csv.writer(fh_invalid)

                try:
                    header = next(reader)
                except StopIteration:
                    raise utils.JobError("CSV is empty; cannot quarantine.")

                valid_writer.writerow(header)
                # The quarantine CSV carries the same header for context,
                # plus a synthetic ``_dpp_line`` column showing where in
                # the source the bad row originated.
                invalid_writer.writerow(["_dpp_line"] + header)

                expected_cols = len(header)
                # Iterate rows; csv.Error (e.g., unbalanced quotes) is
                # rare in well-formed files but if it triggers we abort —
                # the file is malformed beyond the row-count case the
                # Python pass can handle.
                for line_num, row in enumerate(reader, start=2):
                    if len(row) != expected_cols:
                        invalid_writer.writerow([line_num] + row)
                        quarantined += 1
                    else:
                        valid_writer.writerow(row)
                        valid_count += 1
        except csv.Error as e:
            # Clean up partial sibling files and re-raise.
            for p in (valid_path, invalid_path):
                Path(p).unlink(missing_ok=True)
            raise utils.JobError(
                f"qsv validate failed and Python quarantine cannot recover: {e}"
            )

        # If nothing was quarantined the strict check must have failed
        # for a non-row-count reason; surface the original error.
        if quarantined == 0:
            for p in (valid_path, invalid_path):
                Path(p).unlink(missing_ok=True)
            raise utils.JobError(
                "qsv validate failed but no malformed rows were detected "
                "during the quarantine pass; the CSV may have encoding or "
                "quoting issues that need manual repair"
            )

        # Confirm the clean subset is now well-formed.
        try:
            context.qsv.validate(valid_path)
        except utils.JobError as e:
            for p in (valid_path, invalid_path):
                Path(p).unlink(missing_ok=True)
            raise utils.JobError(
                f"Clean subset still failed validation after quarantine: {e}"
            )

        context.quarantined_rows = quarantined
        context.quarantine_csv_path = invalid_path
        # Stash the clean-row count so validate_task can compute
        # total_rows = valid + quarantined for the quarantine-threshold
        # check. Without this, rows_to_copy is still 0 at validate time
        # (AnalysisStage sets it later), which made the quarantine
        # percentage 100% on every run with even one quarantined row.
        # AnalysisStage overwrites rows_to_copy with the real record
        # count downstream.
        context.rows_to_copy = valid_count
        context.update_tmp(valid_path)
        context.logger.info(
            f"Quarantined {quarantined} malformed rows to {invalid_path}; "
            f"continuing with clean subset {valid_path}"
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
