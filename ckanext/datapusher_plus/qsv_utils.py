# -*- coding: utf-8 -*-
# flake8: noqa: E501

import subprocess
import logging
import json
import os
from pathlib import Path
import ckanext.datapusher_plus.config as conf
import ckanext.datapusher_plus.utils as utils

logger = logging.getLogger(__name__)


class QSVCommand:
    """
    A utility class for executing qsv commands.

    This class provides methods for executing various qsv commands with consistent
    error handling and logging.
    """

    def __init__(self, logger=None):
        """
        Initialize the QSVCommand class.

        Args:
            logger: Optional logger instance. If not provided, a default logger will be used.
        """
        self.logger = logger or logging.getLogger(__name__)
        self.qsv_bin = conf.QSV_BIN

        # Verify qsv binary exists
        if not Path(self.qsv_bin).is_file():
            raise utils.JobError(f"{self.qsv_bin} not found.")

    def _run_command(self, args, check=True, capture_output=True, text=True, env=None, uses_stdio=False):
        """
        Run a qsv command with the given arguments.

        Args:
            args: List of arguments for the qsv command
            check: Whether to raise an exception if the command fails
            capture_output: Whether to capture stdout and stderr
            text: Whether to return output as text
            env: Optional environment variables

        Returns:
            The result of subprocess.run

        Raises:
            utils.JobError: If the command fails and check is True
        """
        # Ensure the first argument is the qsv binary
        if args[0] != self.qsv_bin:
            args = [self.qsv_bin] + args

        # Convert all args to str to avoid TypeError with Path objects
        str_args = [str(arg) for arg in args]

        try:
            self.logger.debug(f"Running qsv command: {' '.join(str_args)}")
            result = subprocess.run(
                str_args, check=check, capture_output=capture_output, text=text, env=env
            )
            return result
        except subprocess.CalledProcessError as e:
            if uses_stdio:
                stdio = {}
                stdio["stdout"] = e.stdout
                stdio["stderr"] = e.stderr
                return stdio
            error_msg = f"qsv command failed: {e}"
            if hasattr(e, "stderr") and e.stderr:
                error_msg += f" - {e.stderr}"
                self.logger.error(error_msg)
                if check:
                    raise utils.JobError(error_msg)
            return e.stderr

    def version(self):
        """
        Get the qsv version.

        Returns:
            The qsv version string

        Raises:
            utils.JobError: If the version command fails
        """
        result = self._run_command(["--version"])
        version_info = result.stdout.strip()

        if not version_info:
            raise utils.JobError(
                f"We expect qsv version info to be returned. Command: {self.qsv_bin} --version. Response: {version_info}"
            )

        # Extract version number
        version_start = version_info.find(" ")
        version_end = version_info.find("-")
        if version_start > 0 and version_end > version_start:
            version = version_info[version_start:version_end].lstrip()
        else:
            version = version_info

        return version

    def check_version(self):
        """
        Check if the qsv version meets the minimum requirement.

        Returns:
            True if the version meets the minimum requirement

        Raises:
            utils.JobError: If the version check fails
        """
        try:
            import semver

            version = self.version()
            self.logger.info(f"qsv version found: {version}")

            if semver.compare(version, conf.MINIMUM_QSV_VERSION) < 0:
                raise utils.JobError(
                    f"At least qsv version {conf.MINIMUM_QSV_VERSION} required. Found {version}. "
                    f"You can get the latest release at https://github.com/jqnatividad/qsv/releases/latest"
                )
            return True
        except ValueError as e:
            raise utils.JobError(f"Cannot parse qsv version info: {e}")

    def excel(self, input_file, sheet=0, trim=True, output_file=None):
        """
        Convert an Excel file to CSV.

        Args:
            input_file: Path to the Excel file
            sheet: Sheet index to convert (default: 0)
            trim: Whether to trim column names and data
            output_file: Path to the output CSV file

        Returns:
            The result of the command

        Raises:
            utils.JobError: If the command fails
        """
        args = ["excel", input_file, "--sheet", str(sheet)]

        if trim:
            args.append("--trim")

        if output_file:
            args.extend(["--output", output_file])

        return self._run_command(args)

    def geoconvert(
        self, input_file, input_format, output_format, max_length=None, output_file=None
    ):
        """
        Convert a spatial file to another format.

        Args:
            input_file: Path to the input file
            input_format: Input format (e.g., "geojson")
            output_format: Output format (e.g., "csv")
            max_length: Maximum string length
            output_file: Path to the output file

        Returns:
            The result of the command

        Raises:
            utils.JobError: If the command fails
        """
        args = ["geoconvert", input_file, input_format, output_format]

        if max_length:
            args.extend(["--max-length", str(max_length)])

        if output_file:
            args.extend(["--output", output_file])

        return self._run_command(args)

    def input(self, input_file, trim_headers=True, output_file=None):
        """
        Normalize and transcode a CSV/TSV/TAB file to UTF-8.

        Args:
            input_file: Path to the input file
            trim_headers: Whether to trim headers
            output_file: Path to the output file

        Returns:
            The result of the command

        Raises:
            utils.JobError: If the command fails
        """
        args = ["input", input_file]

        if trim_headers:
            args.append("--trim-headers")

        if output_file:
            args.extend(["--output", output_file])

        return self._run_command(args)

    def validate(self, input_file):
        """
        Validate a CSV file against RFC4180.

        Args:
            input_file: Path to the CSV file

        Returns:
            The result of the command

        Raises:
            utils.JobError: If the command fails
        """
        return self._run_command(["validate", input_file])

    def sortcheck(self, input_file, json_output=False, capture_output=True, uses_stdio=False):
        """
        Check if a CSV file is sorted and has duplicates.

        Args:
            input_file: Path to the CSV file
            json_output: Whether to output JSON

        Returns:
            The result of the command

        Raises:
            utils.JobError: If the command fails
        """
        args = ["sortcheck", input_file]

        if json_output:
            args.append("--json")

        return self._run_command(args, capture_output=capture_output, uses_stdio=uses_stdio)

    def extdedup(self, input_file, output_file):
        """
        Remove duplicate rows from a CSV file.

        Args:
            input_file: Path to the input CSV file
            output_file: Path to the output CSV file

        Returns:
            The result of the command

        Raises:
            utils.JobError: If the command fails
        """
        return self._run_command(["extdedup", input_file, output_file])

    def headers(self, input_file, just_names=False):
        """
        Get the headers of a CSV file.

        Args:
            input_file: Path to the CSV file
            just_names: Whether to return just the header names

        Returns:
            The result of the command

        Raises:
            utils.JobError: If the command fails
        """
        args = ["headers", input_file]

        if just_names:
            args.append("--just-names")

        return self._run_command(args)

    def safenames(
        self, input_file, mode="json", reserved=None, prefix=None, output_file=None, uses_stdio=False
    ):
        """
        Check and sanitize column names.

        Args:
            input_file: Path to the CSV file
            mode: Output mode ("json" or "conditional")
            reserved: Path to a file with reserved column names
            prefix: Prefix for unsafe column names
            output_file: Path to the output file

        Returns:
            The result of the command

        Raises:
            utils.JobError: If the command fails
        """
        args = ["safenames", input_file, "--mode", mode]

        if reserved:
            args.extend(["--reserved", reserved])

        if prefix:
            args.extend(["--prefix", prefix])

        if output_file:
            args.extend(["--output", output_file])

        return self._run_command(args, uses_stdio=uses_stdio)

    def index(self, input_file):
        """
        Create an index for a CSV file.

        Args:
            input_file: Path to the CSV file

        Returns:
            The result of the command

        Raises:
            utils.JobError: If the command fails
        """
        return self._run_command(["index", input_file])

    def count(self, input_file):
        """
        Count the number of rows in a CSV file.

        Args:
            input_file: Path to the CSV file

        Returns:
            The result of the command

        Raises:
            utils.JobError: If the command fails
        """
        return self._run_command(["count", input_file])

    def stats(
        self,
        input_file,
        typesonly=False,
        infer_dates=True,
        dates_whitelist="all",
        stats_jsonl=False,
        prefer_dmy=False,
        cardinality=False,
        summary_stats_options=None,
        output_file=None,
    ):
        """
        Get statistics for a CSV file.

        Args:
            input_file: Path to the CSV file
            typesonly: Whether to output only types
            infer_dates: Whether to infer date types
            dates_whitelist: Whitelist of date formats
            stats_jsonl: Whether to output JSONL
            prefer_dmy: Whether to prefer DMY date format
            cardinality: Whether to calculate cardinality
            summary_stats_options: Additional summary statistics options
            output_file: Path to the output file

        Returns:
            The result of the command

        Raises:
            utils.JobError: If the command fails
        """
        args = ["stats", input_file]

        if typesonly:
            args.append("--typesonly")

        if infer_dates:
            args.extend(["--infer-dates", "--dates-whitelist", dates_whitelist])

        if stats_jsonl:
            args.append("--stats-jsonl")

        if prefer_dmy:
            args.append("--prefer-dmy")

        if cardinality:
            args.append("--cardinality")

        if summary_stats_options:
            args.append(summary_stats_options)

        if output_file:
            args.extend(["--output", output_file])

        return self._run_command(args)

    def frequency(self, input_file, limit=0, output_file=None):
        """
        Get frequency statistics for a CSV file.

        Args:
            input_file: Path to the CSV file
            limit: Maximum number of values to return per field
            output_file: Path to the output file

        Returns:
            The result of the command

        Raises:
            utils.JobError: If the command fails
        """
        args = ["frequency", "--limit", str(limit), input_file]

        if output_file:
            args.extend(["--output", output_file])

        return self._run_command(args)

    def slice(self, input_file, start=None, length=None, output_file=None):
        """
        Slice a CSV file.

        Args:
            input_file: Path to the CSV file
            start: Starting row (0-based)
            length: Number of rows to include
            output_file: Path to the output file

        Returns:
            The result of the command

        Raises:
            utils.JobError: If the command fails
        """
        args = ["slice", input_file]

        if start is not None:
            args.extend(["--start", str(start)])

        if length is not None:
            args.extend(["--len", str(length)])

        if output_file:
            args.extend(["--output", output_file])

        return self._run_command(args)

    def datefmt(self, datecols, input_file, prefer_dmy=False, output_file=None):
        """
        Format dates in a CSV file.

        Args:
            datecols: Comma-separated list of date columns
            input_file: Path to the CSV file
            prefer_dmy: Whether to prefer DMY date format
            output_file: Path to the output file

        Returns:
            The result of the command

        Raises:
            utils.JobError: If the command fails
        """
        args = ["datefmt", datecols, input_file]

        if prefer_dmy:
            args.append("--prefer-dmy")

        if output_file:
            args.extend(["--output", output_file])

        return self._run_command(args)

    def searchset(
        self,
        regex_file,
        input_file,
        ignore_case=False,
        quick=False,
        flag=None,
        flag_matches_only=False,
        json_output=False,
        output_file=None,
    ):
        """
        Search a CSV file for patterns defined in a regex file.

        Args:
            regex_file: Path to the regex file
            input_file: Path to the CSV file
            ignore_case: Whether to ignore case
            quick: Whether to use quick mode
            flag: Flag to add to matching rows
            flag_matches_only: Whether to only output matching rows
            json_output: Whether to output JSON
            output_file: Path to the output file

        Returns:
            The result of the command

        Raises:
            utils.JobError: If the command fails
        """
        args = ["searchset", regex_file, input_file]

        if ignore_case:
            args.append("--ignore-case")

        if quick:
            args.append("--quick")

        if flag:
            args.extend(["--flag", flag])

        if flag_matches_only:
            args.append("--flag-matches-only")

        if json_output:
            args.append("--json")

        if output_file:
            args.extend(["--output", output_file])

        return self._run_command(args)
