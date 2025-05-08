# -*- coding: utf-8 -*-
# flake8: noqa: E501

import csv
import psycopg2
from psycopg2 import sql
import shutil
import os
import subprocess
import logging
from pathlib import Path
import ckanext.datapusher_plus.config as conf
import ckanext.datapusher_plus.utils as utils
from typing import Optional, Dict, Any, List, Union

from ckanext.datapusher_plus.logging_utils import TRACE

logger = logging.getLogger(__name__)


class QSVCommand:
    """
    A utility class for executing qsv commands.

    This class provides methods for executing various qsv commands with consistent
    error handling and logging.
    """

    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
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

        # Check qsv version
        try:
            self.check_version()
        except utils.JobError as e:
            raise utils.JobError(f"qsv version check failed: {e}")

    def _run_command(
        self,
        args: List[Union[str, Path]],
        check: bool = True,
        capture_output: bool = True,
        text: bool = True,
        env: Optional[Dict[str, str]] = None,
        uses_stdio: bool = False,
    ) -> Union[subprocess.CompletedProcess, Dict[str, Any], str]:
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

        args = [self.qsv_bin] + args

        # Convert all args to str to avoid TypeError with Path objects
        str_args = [str(arg) for arg in args]

        try:
            self.logger.trace(f"Running qsv command: {' '.join(str_args)}")
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

    def version(self) -> str:
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

    def check_version(self) -> bool:
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

    def excel(
        self,
        input_file: str,
        sheet: int = 0,
        trim: bool = True,
        output_file: Optional[str] = None,
    ) -> subprocess.CompletedProcess:
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
        self,
        input_file: str,
        input_format: str,
        output_format: str,
        max_length: Optional[int] = None,
        output_file: Optional[str] = None,
    ) -> subprocess.CompletedProcess:
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

    def geocode(
        self,
        input_file: str,
        subcommand: str,
        column: str,
        new_column: Optional[str] = None,
        rename: Optional[str] = None,
        country: Optional[str] = None,
        min_score: Optional[float] = None,
        admin1: Optional[bool] = None,
        k_weight: Optional[float] = None,
        format_str: Optional[str] = None,
        output_file: Optional[str] = None,
    ) -> subprocess.CompletedProcess:
        """
        Geocode addresses in a CSV file.

        Args:
            input_file: Path to the input CSV file
            subcommand: One of: suggest, suggestnow, reverse, reversenow, countryinfo, countryinfonow
            column: Column containing addresses to geocode
            new_column: New column name for geocoded results
            rename: Rename the column containing addresses to geocode
            country: Country code to restrict geocoding to (e.g., "US")
            min_score: Minimum score threshold for geocoding matches (0.0-1.0)
            admin1: Whether to include administrative level 1 (state/province) information
            k_weight: Weight for the k-nearest neighbors algorithm (0.0-1.0)
            format_str: Custom format string for output
            output_file: Path to the output file

        Returns:
            The result of the command

        Raises:
            utils.JobError: If the command fails
        """
        valid_subcommands = [
            "suggest",
            "suggestnow",
            "reverse",
            "reversenow",
            "countryinfo",
            "countryinfonow",
        ]
        if subcommand not in valid_subcommands:
            raise utils.JobError(
                f"Invalid subcommand: {subcommand}. Must be one of: {', '.join(valid_subcommands)}"
            )

        args = ["geocode", subcommand, input_file, column]

        if new_column:
            args.extend(["--new-column", new_column])

        if rename:
            args.extend(["--rename", rename])

        if country:
            args.extend(["--country", country])

        if min_score is not None:
            if not 0.0 <= min_score <= 1.0:
                raise utils.JobError("min_score must be between 0.0 and 1.0")
            args.extend(["--min-score", str(min_score)])

        if admin1 is not None:
            args.append("--admin1")

        if k_weight is not None:
            if not 0.0 <= k_weight <= 1.0:
                raise utils.JobError("k_weight must be between 0.0 and 1.0")
            args.extend(["--k-weight", str(k_weight)])

        if format_str:
            args.extend(["--format", format_str])

        if output_file:
            args.extend(["--output", output_file])

        return self._run_command(args)

    def input(
        self,
        input_file: str,
        trim_headers: bool = True,
        output_file: Optional[str] = None,
    ) -> subprocess.CompletedProcess:
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

    def validate(self, input_file: str) -> subprocess.CompletedProcess:
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

    def sortcheck(
        self,
        input_file: str,
        json_output: bool = False,
        capture_output: bool = True,
        uses_stdio: bool = False,
    ) -> Union[subprocess.CompletedProcess, Dict[str, Any]]:
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

        return self._run_command(
            args, capture_output=capture_output, uses_stdio=uses_stdio
        )

    def extdedup(
        self, input_file: str, output_file: str
    ) -> subprocess.CompletedProcess:
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

    def headers(
        self, input_file: str, just_names: bool = False
    ) -> subprocess.CompletedProcess:
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
        self,
        input_file: str,
        mode: str = "json",
        reserved: Optional[str] = None,
        prefix: Optional[str] = None,
        output_file: Optional[str] = None,
        uses_stdio: bool = False,
    ) -> Union[subprocess.CompletedProcess, Dict[str, Any]]:
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

    def index(self, input_file: str) -> subprocess.CompletedProcess:
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

    def count(self, input_file: str) -> subprocess.CompletedProcess:
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
        input_file: str,
        typesonly: bool = False,
        infer_dates: bool = True,
        dates_whitelist: str = "all",
        stats_jsonl: bool = False,
        prefer_dmy: bool = False,
        cardinality: bool = False,
        summary_stats_options: Optional[str] = None,
        output_file: Optional[str] = None,
    ) -> subprocess.CompletedProcess:
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

    def frequency(
        self,
        input_file: str,
        limit: int = 0,
        output_file: Optional[str] = None,
    ) -> subprocess.CompletedProcess:
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

    def slice(
        self,
        input_file: str,
        start: Optional[int] = None,
        length: Optional[int] = None,
        output_file: Optional[str] = None,
    ) -> subprocess.CompletedProcess:
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

    def datefmt(
        self,
        datecols: str,
        input_file: str,
        prefer_dmy: bool = False,
        output_file: Optional[str] = None,
    ) -> subprocess.CompletedProcess:
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
        regex_file: str,
        input_file: str,
        ignore_case: bool = False,
        quick: bool = False,
        flag: Optional[str] = None,
        flag_matches_only: bool = False,
        json_output: bool = False,
        output_file: Optional[str] = None,
    ) -> subprocess.CompletedProcess:
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

    def save_stats_to_datastore(
        self,
        stats_csv_file: str,
        resource_id: str,
        datastore_write_url: str,
        logger: Optional[logging.Logger] = None,
    ) -> bool:
        """
        Save statistics from a CSV file to the datastore.

        Args:
            stats_csv_file: Path to the CSV file containing statistics
            resource_id: Resource ID to use for the stats table
            datastore_write_url: PostgreSQL connection URL for the datastore
            logger: Optional logger instance

        Returns:
            bool: True if successful

        Raises:
            utils.JobError: If saving stats to datastore fails
        """
        logger = logger or self.logger
        stats_table = sql.Identifier(resource_id + "-druf-stats")

        with psycopg2.connect(datastore_write_url) as raw_connection_statsfreq:
            with raw_connection_statsfreq.cursor() as cur_statsfreq:
                # Create stats table based on qsv stats CSV structure
                cur_statsfreq.execute(
                    sql.SQL(
                        """
                        DROP TABLE IF EXISTS {};
                        CREATE TABLE {} (
                            field TEXT,
                            type TEXT,
                            is_ascii BOOLEAN,
                            sum TEXT,
                            min TEXT,
                            max TEXT,
                            range TEXT,
                            sort_order TEXT,
                            sortiness FLOAT,
                            min_length INTEGER,
                            max_length INTEGER,
                            sum_length INTEGER,
                            avg_length FLOAT,
                            stddev_length FLOAT,
                            variance_length FLOAT,
                            cv_length FLOAT,
                            mean TEXT,
                            sem FLOAT,
                            geometric_mean FLOAT,
                            harmonic_mean FLOAT,
                            stddev FLOAT,
                            variance FLOAT,
                            cv FLOAT,
                            nullcount INTEGER,
                            max_precision INTEGER,
                            sparsity FLOAT,
                            cardinality INTEGER,
                            uniqueness_ratio FLOAT
                        )
                    """
                    ).format(stats_table, stats_table)
                )

                # Load stats CSV directly using COPY
                copy_sql = sql.SQL(
                    "COPY {} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
                ).format(stats_table)

                # Copy stats CSV to /tmp directory for debugging purposes
                if logger.isEnabledFor(TRACE):
                    try:
                        debug_stats_path = os.path.join(
                            "/tmp", os.path.basename(stats_csv_file)
                        )
                        shutil.copy2(stats_csv_file, debug_stats_path)
                        logger.trace(
                            f"Copied stats CSV to {debug_stats_path} for debugging"
                        )
                    except Exception as e:
                        logger.trace(
                            f"Failed to copy stats CSV to /tmp for debugging: {e}"
                        )

                try:
                    with open(stats_csv_file, "r") as f:
                        cur_statsfreq.copy_expert(copy_sql, f)
                except IOError as e:
                    raise utils.JobError("Could not open stats CSV file: {}".format(e))
                except psycopg2.Error as e:
                    raise utils.JobError(
                        "Could not copy stats data to database: {}".format(e)
                    )

            raw_connection_statsfreq.commit()

        return True

    def save_freq_to_datastore(
        self,
        freq_csv_file: str,
        resource_id: str,
        datastore_write_url: str,
        logger: Optional[logging.Logger] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Save frequency data from a CSV file to the datastore.

        Args:
            freq_csv_file: Path to the CSV file containing frequency data
            resource_id: Resource ID to use for the stats table
            datastore_write_url: PostgreSQL connection URL for the datastore
            logger: Optional logger instance

        Returns:
            dict: The resource fields frequencies dictionary

        Raises:
            utils.JobError: If saving frequency data to datastore fails
        """
        logger = logger or self.logger
        freq_table = sql.Identifier(resource_id + "-druf-freq")

        with psycopg2.connect(datastore_write_url) as raw_connection_freq:
            with raw_connection_freq.cursor() as cur_freq:
                # Create frequency table based on qsv frequency CSV structure
                cur_freq.execute(
                    sql.SQL(
                        """
                        DROP TABLE IF EXISTS {};
                        CREATE TABLE {} (
                            field TEXT,
                            value TEXT,
                            count INTEGER,
                            percentage FLOAT,
                            PRIMARY KEY (field, value, count)
                        )
                    """
                    ).format(freq_table, freq_table)
                )

                # Copy frequency CSV to /tmp directory for debugging purposes
                if logger.isEnabledFor(TRACE):
                    try:
                        debug_freq_path = os.path.join(
                            "/tmp", os.path.basename(freq_csv_file)
                        )
                        shutil.copy2(freq_csv_file, debug_freq_path)
                        logger.trace(
                            f"Copied frequency CSV to {debug_freq_path} for debugging"
                        )
                    except Exception as e:
                        logger.trace(
                            f"Failed to copy frequency CSV to /tmp for debugging: {e}"
                        )

                # load the frequency table using COPY
                copy_sql = sql.SQL(
                    "COPY {} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
                ).format(freq_table)

                resource_fields_freqs = {}
                try:
                    with open(freq_csv_file, "r") as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            field = row["field"]
                            value = row["value"]
                            count = row["count"]
                            percentage = row["percentage"]

                            # Initialize list for field if it doesn't exist
                            if field not in resource_fields_freqs:
                                resource_fields_freqs[field] = []

                            # Append the frequency data as a dict to the field's list
                            resource_fields_freqs[field].append(
                                {
                                    "value": value,
                                    "count": count,
                                    "percentage": percentage,
                                }
                            )

                        logger.trace(f"Resource fields freqs: {resource_fields_freqs}")

                        # Rewind file for COPY operation
                        f.seek(0)
                        cur_freq.copy_expert(copy_sql, f)
                except IOError as e:
                    raise utils.JobError(
                        "Could not open frequency CSV file: {}".format(e)
                    )
                except psycopg2.Error as e:
                    raise utils.JobError(
                        "Could not copy frequency data to database: {}".format(e)
                    )

                raw_connection_freq.commit()

        return resource_fields_freqs
