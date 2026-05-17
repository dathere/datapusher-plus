# -*- coding: utf-8 -*-
# flake8: noqa: E501

import csv
import json
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
        timeout: Optional[float] = None,
    ) -> Union[subprocess.CompletedProcess, Dict[str, Any], str]:
        """
        Run a qsv command with the given arguments.

        Args:
            args: List of arguments for the qsv command
            check: Whether to raise an exception if the command fails
            capture_output: Whether to capture stdout and stderr
            text: Whether to return output as text
            env: Optional environment variables
            timeout: Per-call override (seconds). Defaults to
                ``conf.QSV_COMMAND_TIMEOUT`` so a hung qsv invocation never
                pins an RQ worker indefinitely.

        Returns:
            The result of subprocess.run

        Raises:
            utils.JobError: If the command fails and check is True, or if the
                command times out.
        """

        args = [self.qsv_bin] + args

        # Convert all args to str to avoid TypeError with Path objects
        str_args = [str(arg) for arg in args]

        effective_timeout = timeout if timeout is not None else conf.QSV_COMMAND_TIMEOUT

        try:
            self.logger.trace(f"Running qsv command: {' '.join(str_args)}")
            result = subprocess.run(
                str_args,
                check=check,
                capture_output=capture_output,
                text=text,
                env=env,
                timeout=effective_timeout,
            )
            return result
        except subprocess.TimeoutExpired as e:
            error_msg = (
                f"qsv command timed out after {effective_timeout}s: "
                f"{' '.join(str_args)}"
            )
            self.logger.error(error_msg)
            raise utils.JobError(error_msg) from e
        except subprocess.CalledProcessError as e:
            if uses_stdio:
                # Callers that opt into stdio capture handle their own failure
                # logic and just want the raw stdout/stderr back.
                return {"stdout": e.stdout, "stderr": e.stderr}

            # Always log AND always raise when check=True — previously the
            # raise was nested inside `if e.stderr:`, so failures with empty
            # stderr silently returned None/"" and slipped past callers that
            # had asked for check=True. Build the message with stderr when we
            # have it, fall back to the exception's str otherwise.
            error_msg = f"qsv command failed: {e}"
            if getattr(e, "stderr", None):
                error_msg += f" - {e.stderr}"
            self.logger.error(error_msg)
            if check:
                raise utils.JobError(error_msg) from e
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
        delimiter: Optional[str] = None,
    ) -> subprocess.CompletedProcess:
        """
        Normalize and transcode a CSV/TSV/TAB file to UTF-8.

        Args:
            input_file: Path to the input file
            trim_headers: Whether to trim headers
            output_file: Path to the output file
            delimiter: Input delimiter override (e.g. ``";"`` or a tab).
                When ``None``, qsv assumes comma. The output is always
                comma-delimited regardless.

        Returns:
            The result of the command

        Raises:
            utils.JobError: If the command fails
        """
        args = ["input", input_file]

        if delimiter:
            args.extend(["--delimiter", delimiter])

        if trim_headers:
            args.append("--trim-headers")

        if output_file:
            args.extend(["--output", output_file])

        return self._run_command(args)

    def sniff(self, input_file: str) -> Optional[Dict[str, Any]]:
        """
        Sniff a CSV's metadata (delimiter, header row, ...) via ``qsv sniff``.

        Best-effort: returns the parsed ``qsv sniff --json`` dict, or
        ``None`` when sniffing fails or the output is not valid JSON.
        Callers should treat ``None`` as "could not determine" and fall
        back to qsv's defaults.

        Args:
            input_file: Path to the file to sniff

        Returns:
            The sniff result dict, or ``None`` on failure
        """
        result = self._run_command(["sniff", "--json", input_file], check=False)
        stdout = getattr(result, "stdout", None)
        if not stdout:
            return None
        try:
            return json.loads(stdout)
        except (ValueError, TypeError):
            return None

    def validate(self, input_file: str) -> subprocess.CompletedProcess:
        """
        Validate a CSV file against the RFC 4180 standard.

        Note: qsv's ``--valid`` and ``--invalid`` row-routing flags only
        emit output files in JSON-Schema mode, not in RFC 4180 mode that
        DP+ uses. Row-level quarantine is handled in
        ``ValidationStage._validate_csv`` via Python's ``csv`` module
        when this strict check fails.

        Args:
            input_file: Path to the CSV file

        Returns:
            The result of the command

        Raises:
            utils.JobError: If the command fails (i.e., RFC 4180
                violations were found).
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

    def describegpt(
        self,
        input_file: str,
        prompt_file: Optional[str] = None,
        description: bool = True,
        dictionary: bool = True,
        tags: bool = True,
        json_output: bool = True,
        output_file: Optional[str] = None,
        timeout: Optional[float] = None,
        env: Optional[Dict[str, str]] = None,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
    ) -> subprocess.CompletedProcess:
        """
        Run ``qsv describegpt`` against a CSV to get LLM-generated
        description / per-field dictionary / tags.

        Thin wrapper — qsv owns the actual LLM round-trip (endpoint,
        model, prompt template, API key all live in ``prompt_file`` or
        qsv's environment, NOT in DP+ config). The wrapper just stitches
        the flags together and invokes the subprocess.

        Output shape (with ``json_output=True``, validated against qsv
        20.0.0)::

            {
              "Dictionary": {
                "response": {
                  "fields": [
                    {"name": "...", "type": "...", "label": "...",
                     "description": "...", "min": "...", "max": "...",
                     "cardinality": N, "enumeration": ...,
                     "null_count": N, "examples": "..."}
                  ],
                  "enum_threshold": N, "num_examples": N,
                  "truncate_str": N, "attribution": "..."
                },
                "reasoning": "...", "token_usage": {...}
              },
              "Description": {
                "response": "<markdown string>",
                "reasoning": "...", "token_usage": {...}
              },
              "Tags": {
                "response": {"tags": [...], "attribution": "..."},
                "reasoning": "...", "token_usage": {...},
                "num_tags": N, "tag_vocab": ...
              }
            }

        ``AISuggestionsStage._reshape_for_ui`` consumes this and emits
        the per-field ``{value, source}`` map the scheming UI reads.

        Args:
            input_file: Path to the CSV (the file qsv describes).
            prompt_file: Optional path to qsv's describegpt prompt /
                config file (TOML). When omitted, qsv falls back to its
                own discovery (``~/.qsv/describegpt.toml`` etc.) and
                ``OPENAI_API_KEY`` from the environment. Maps to
                ``--prompt-file``.
            description: Include the overall dataset description.
                Maps to ``--description``. Default True.
            dictionary: Include the per-field dictionary.
                Maps to ``--dictionary``. Default True.
            tags: Include the tag list. Maps to ``--tags``. Default True.
            json_output: When True, request the structured JSON envelope
                described above (``--format JSON``). When False, qsv
                emits the default Markdown report. Default True — the
                AISuggestionsStage's JSON parse depends on this.

                Note: qsv 20.0.0 controls output format via
                ``--format <Markdown|TSV|JSON|TOON>``, NOT a ``--json``
                flag (an earlier draft of this wrapper assumed the
                latter and produced ``Unknown flag: '--json'`` at
                runtime).
            output_file: Optional path to write the result to. When
                omitted, the result lands in ``CompletedProcess.stdout``.
            timeout: Per-call subprocess timeout, in seconds. Defaults
                to ``conf.DESCRIBEGPT_TIMEOUT_SECONDS`` (120s) so a hung
                LLM endpoint doesn't pin a worker for the full
                ``QSV_COMMAND_TIMEOUT`` (1800s default).
            env: Optional environment overrides for the subprocess
                (e.g. ``{"OPENAI_API_KEY": "..."}`` if the caller has a
                key it wants to inject out-of-band).
            api_key: Optional value for ``--api-key``. Required when
                the LLM endpoint isn't on ``localhost`` (qsv treats
                non-localhost base URLs as "not a local LLM" and
                refuses to start without an API key). For unauthenticated
                local LLMs reached via container-host hostnames
                (``host.docker.internal``, ``host.local``, …) pass the
                literal string ``"NONE"`` — qsv accepts it as the
                "I know this is unauthenticated, get on with it" sentinel.
                When omitted, qsv reads ``QSV_LLM_APIKEY`` from the
                environment (or the prompt-file).
            base_url: Optional override for ``--base-url`` — lets
                callers point at a different LLM endpoint without
                rewriting the prompt-file. When omitted, qsv reads
                ``base_url`` from the prompt-file.

        Returns:
            The result of ``subprocess.run``. ``stdout`` is the
            describegpt output (JSON envelope when ``json_output=True``,
            Markdown otherwise).

        Raises:
            utils.JobError: If the subprocess fails (non-zero exit or
                timeout). Callers in the AISuggestionsStage catch
                ``JobError`` so describegpt failures don't bring down
                the pipeline.
        """
        args: List[Union[str, Path]] = ["describegpt"]

        if description:
            args.append("--description")
        if dictionary:
            args.append("--dictionary")
        if tags:
            args.append("--tags")
        if json_output:
            args.extend(["--format", "JSON"])
        if prompt_file:
            args.extend(["--prompt-file", prompt_file])
        if output_file:
            args.extend(["--output", output_file])
        if api_key:
            args.extend(["--api-key", api_key])
        if base_url:
            args.extend(["--base-url", base_url])

        args.append(input_file)

        effective_timeout = (
            timeout if timeout is not None else conf.DESCRIBEGPT_TIMEOUT_SECONDS
        )
        return self._run_command(args, timeout=effective_timeout, env=env)

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
