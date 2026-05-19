# -*- coding: utf-8 -*-
"""
Download stage for the DataPusher Plus pipeline.

Handles downloading resources, hash checking, and ZIP file extraction.
"""

import os
import time
import hashlib
import mimetypes
from typing import Dict, Any
from urllib.parse import urlsplit, urlparse

import requests
from datasize import DataSize
from dateutil.parser import parse as parsedate

import ckanext.datapusher_plus.utils as utils
import ckanext.datapusher_plus.helpers as dph
import ckanext.datapusher_plus.config as conf
from ckanext.datapusher_plus.job_exceptions import HTTPError
from ckanext.datapusher_plus.jobs.stages.base import BaseStage
from ckanext.datapusher_plus.jobs.context import ProcessingContext


# Issue #221: factory for the configured file-hash algorithm. Returns an
# object with the standard ``.update(bytes)`` / ``.hexdigest()`` shape
# so the streaming-download loop doesn't care which algorithm it's
# feeding. ``hashlib`` exposes sha256 and md5 directly; ``blake3`` ships
# the same protocol via a separate package (already in requirements.txt).
#
# Returning a fresh hasher per call (rather than caching) is intentional
# — hashlib objects are stateful and must not be reused across downloads.
def _get_file_hasher():
    """Return a new hasher instance for the configured algorithm.

    Reads ``ckanext.datapusher_plus.file_hash_algorithm`` from
    ``tk.config`` **live** at each call. Because the config key is
    declared ``editable: true`` in ``config_declaration.yaml``,
    operators expect a runtime change via the admin UI to take effect
    without a worker restart — caching the value at import would
    silently break that contract. The factory is called once per
    resource download, so the dict lookup overhead is negligible.

    Raises ``utils.JobError`` for an unknown algorithm name rather than
    silently falling back, so a typo in ``ckan.ini`` surfaces at the
    first download instead of producing inscrutable hash mismatches
    downstream.
    """
    # ``tk`` is the CKAN toolkit; ``tk.config`` is the live config dict
    # the admin UI mutates. Local import keeps the module-load time
    # path of this file unchanged and matches the rest of the codebase
    # idiom of pulling ``tk`` in where needed.
    import ckan.plugins.toolkit as tk

    algo = tk.config.get(
        "ckanext.datapusher_plus.file_hash_algorithm", "blake3"
    ).lower()
    if algo == "blake3":
        # ``blake3`` is a hard requirement in requirements.txt; if the
        # import fails the install is broken, not a config issue.
        from blake3 import blake3 as _blake3  # type: ignore[import-untyped]

        return _blake3()
    if algo == "sha256":
        return hashlib.sha256()
    if algo == "md5":
        # DevSkim flags md5 as DS126858; we keep it as a legacy
        # compatibility knob, not for security. Same suppression as the
        # original site this factory replaces.
        return hashlib.md5()  # DevSkim: ignore DS126858
    raise utils.JobError(
        f"Unknown ckanext.datapusher_plus.file_hash_algorithm={algo!r}. "
        f"Allowed values: 'blake3', 'sha256', 'md5'."
    )


def _get_download_always_whitelist():
    """Return the parsed DOWNLOAD_ALWAYS_WHITELIST from live config.

    Reads ``ckanext.datapusher_plus.download_always_whitelist`` from
    ``tk.config`` live (declared ``editable: true`` in
    ``config_declaration.yaml``) so admin-UI edits take effect without
    a worker restart. Mirrors the ``_get_file_hasher`` pattern from
    issue #221. Parses on each call; the cost is negligible since the
    hash-skip check fires at most once per download.

    Returns:
        ``frozenset`` of lowercased hostnames. Empty set if the
        config is unset, empty, or the value is malformed.
    """
    import ckan.plugins.toolkit as tk

    raw = tk.config.get(
        "ckanext.datapusher_plus.download_always_whitelist", ""
    )
    if isinstance(raw, str):
        raw = raw.split()
    try:
        return frozenset(h.lower() for h in raw if h)
    except (TypeError, AttributeError):
        # Defensive: malformed config (e.g., a non-iterable) should
        # degrade to "no host bypass" rather than crash the download.
        return frozenset()


def _host_in_always_whitelist(url):
    """Return the matched hostname if ``url``'s host is whitelisted, else ``None``.

    Used by the download stage to bypass the hash-skip optimization
    in ``_should_skip_upload`` for trusted/peered hosts or hosts
    that update content in place without changing the published
    byte hash. See issue #61 for the operator-side rationale.

    Returning the matched host (rather than a bare ``bool``) lets
    the caller log the host without a second ``urlparse`` call.

    Args:
        url: The resource URL as stored in ``resource["url"]``.

    Returns:
        The lowercased hostname (with port stripped) when matched;
        ``None`` when the whitelist is empty, the URL is missing or
        unparseable, the URL has no hostname (relative URLs,
        ``file://`` with empty host, etc.), or the host is not in
        the whitelist. Matching is exact: ``data.gov`` in the
        whitelist does NOT match ``subdomain.data.gov``.
    """
    whitelist = _get_download_always_whitelist()
    if not whitelist:
        return None
    if not url:
        return None
    try:
        host = urlparse(url).hostname
    except (ValueError, AttributeError):
        return None
    if not host:
        return None
    host = host.lower()
    return host if host in whitelist else None


class DownloadStage(BaseStage):
    """
    Downloads the resource file, validates it, and handles ZIP extraction.

    Responsibilities:
    - Validate resource URL scheme
    - Download file with authentication if needed
    - Calculate file hash for deduplication
    - Check if file has changed since last upload
    - Extract ZIP files if applicable
    """

    def __init__(self):
        super().__init__(name="Download")

    def process(self, context: ProcessingContext) -> ProcessingContext:
        """
        Download and validate the resource file.

        Args:
            context: Processing context

        Returns:
            Updated context with downloaded file information

        Raises:
            utils.JobError: If download fails or file is invalid
        """
        # Validate resource URL scheme
        self._validate_url_scheme(context)

        # Start timing
        context.timer_start = time.perf_counter()

        # Download the file
        file_hash, length, resource_format, response_headers = self._download_file(context)

        # Store file information
        context.file_hash = file_hash
        context.content_length = length
        context.add_stat("ORIGINAL_FILE_SIZE", length)

        # Check for file deduplication
        if self._should_skip_upload(context, file_hash, response_headers):
            context.logger.warning(
                f"Upload skipped as the file hash hasn't changed: {file_hash}."
            )
            return None  # Signal to skip further processing

        # Update resource hash
        context.resource["hash"] = file_hash

        # Log download completion
        fetch_elapsed = time.perf_counter() - context.timer_start
        context.logger.info(
            f"Fetched {DataSize(length):.2MB} file in {fetch_elapsed:,.2f} seconds."
        )

        # Handle ZIP file extraction
        self._handle_zip_file(context, resource_format)

        return context

    def _validate_url_scheme(self, context: ProcessingContext) -> None:
        """
        Validate that the resource URL uses an allowed scheme.

        Args:
            context: Processing context

        Raises:
            utils.JobError: If URL scheme is not allowed
        """
        context.resource_url = context.resource.get("url")
        scheme = urlsplit(context.resource_url).scheme
        if scheme not in ("http", "https", "ftp"):
            raise utils.JobError("Only http, https, and ftp resources may be fetched.")

    def _download_file(
        self, context: ProcessingContext
    ) -> tuple[str, int, str, Dict[str, Any]]:
        """
        Download the resource file and calculate its hash.

        Args:
            context: Processing context

        Returns:
            Tuple of (file_hash, file_length, resource_format, response_headers)

        Raises:
            HTTPError: If download fails
            utils.JobError: If file is too large or format cannot be determined
        """
        resource_url = context.resource_url
        context.logger.info(f"Fetching from: {resource_url}...")

        # Prepare request headers
        headers: Dict[str, str] = {}
        if context.resource.get("url_type") == "upload":
            # Authenticate for uploaded files
            api_token = utils.get_dp_plus_user_apitoken()
            headers["Authorization"] = api_token

            # Rewrite URL if needed (for firewalls)
            resource_url = self._rewrite_url_if_needed(
                context, resource_url, context.ckan_url
            )

        # Configure request
        kwargs: Dict[str, Any] = {
            "headers": headers,
            "timeout": conf.TIMEOUT,
            "verify": conf.SSL_VERIFY,
            "stream": True,
        }
        if conf.USE_PROXY:
            kwargs["proxies"] = {
                "http": conf.DOWNLOAD_PROXY,
                "https": conf.DOWNLOAD_PROXY,
            }

        # Download file
        try:
            with requests.get(resource_url, **kwargs) as response:
                response.raise_for_status()

                # Get content info
                cl = response.headers.get("content-length")
                max_content_length = conf.MAX_CONTENT_LENGTH
                ct = response.headers.get("content-type")

                # Check size before download
                if cl:
                    try:
                        if int(cl) > max_content_length and conf.PREVIEW_ROWS > 0:
                            raise utils.JobError(
                                f"Resource too large to download: {DataSize(int(cl)):.2MB} "
                                f"> max ({DataSize(int(max_content_length)):.2MB})."
                            )
                    except ValueError:
                        pass

                # Determine file format
                resource_format = self._determine_format(
                    context, ct, response.headers
                )

                # Download and hash the file
                file_hash, length = self._stream_download(
                    context, resource_format, response, max_content_length
                )

                return file_hash, length, resource_format, dict(response.headers)

        except requests.HTTPError as e:
            raise HTTPError(
                f"DataPusher+ received a bad HTTP response when trying to download "
                f"the data file from {resource_url}. Status code: {e.response.status_code}, "
                f"Response content: {e.response.content}",
                status_code=e.response.status_code,
                request_url=resource_url,
                response=e.response.content,
            )
        except requests.RequestException as e:
            raise HTTPError(
                message=str(e),
                status_code=None,
                request_url=resource_url,
                response=None,
            )

    def _rewrite_url_if_needed(
        self, context: ProcessingContext, resource_url: str, ckan_url: str
    ) -> str:
        """
        Rewrite URL if CKAN is behind a firewall.

        Args:
            context: Processing context
            resource_url: Original resource URL
            ckan_url: CKAN base URL

        Returns:
            Potentially rewritten URL
        """
        if not resource_url.startswith(ckan_url):
            new_url = urlparse(resource_url)
            rewrite_url = urlparse(ckan_url)
            new_url = new_url._replace(
                scheme=rewrite_url.scheme, netloc=rewrite_url.netloc
            )
            resource_url = new_url.geturl()
            context.logger.info(f"Rewritten resource url to: {resource_url}")
        return resource_url

    def _determine_format(
        self, context: ProcessingContext, content_type: str, headers: Dict[str, Any]
    ) -> str:
        """
        Determine the file format from resource metadata or content type.

        Args:
            context: Processing context
            content_type: HTTP content-type header
            headers: Response headers

        Returns:
            File format string (uppercase)

        Raises:
            utils.JobError: If format cannot be determined
        """
        resource_format = context.resource.get("format", "").upper()

        if not resource_format:
            context.logger.info("File format: NOT SPECIFIED")
            if content_type:
                extension = mimetypes.guess_extension(content_type.split(";")[0])
                if extension is None:
                    raise utils.JobError(
                        "Cannot determine format from mime type. Please specify format."
                    )
                resource_format = extension.lstrip(".").upper()
                context.logger.info(f"Inferred file format: {resource_format}")
            else:
                raise utils.JobError(
                    "Server did not return content-type. Please specify format."
                )
        else:
            context.logger.info(f"File format: {resource_format}")

        return resource_format

    def _stream_download(
        self,
        context: ProcessingContext,
        resource_format: str,
        response: requests.Response,
        max_content_length: int,
    ) -> tuple[str, int]:
        """
        Stream download the file and calculate its hash.

        Args:
            context: Processing context
            resource_format: File format extension
            response: HTTP response object
            max_content_length: Maximum allowed file size

        Returns:
            Tuple of (file_hash, file_length)

        Raises:
            utils.JobError: If file exceeds maximum size
        """
        tmp = os.path.join(context.temp_dir, "tmp." + resource_format)
        context.update_tmp(tmp)

        length = 0
        # Issue #221: algorithm is selected from
        # ``ckanext.datapusher_plus.file_hash_algorithm`` (default
        # ``blake3``). See ``_get_file_hasher`` for the contract. The
        # hash is used for upload-skip / cache-key / resource ``hash``
        # field — not for cryptographic integrity.
        m = _get_file_hasher()

        # Log download start
        cl = response.headers.get("content-length")
        if cl:
            context.logger.info(f"Downloading {DataSize(int(cl)):.2MB} file...")
        else:
            context.logger.info("Downloading file of unknown size...")

        # Stream download
        with open(tmp, "wb") as tmp_file:
            for chunk in response.iter_content(conf.CHUNK_SIZE):
                length += len(chunk)
                if length > max_content_length and not conf.PREVIEW_ROWS:
                    raise utils.JobError(
                        f"Resource too large to process: {length} > max ({max_content_length})."
                    )
                tmp_file.write(chunk)
                m.update(chunk)

        return m.hexdigest(), length

    def _should_skip_upload(
        self,
        context: ProcessingContext,
        file_hash: str,
        response_headers: Dict[str, Any],
    ) -> bool:
        """
        Check if upload should be skipped due to unchanged file.

        Args:
            context: Processing context
            file_hash: Hash of the downloaded file (algorithm per
                ``ckanext.datapusher_plus.file_hash_algorithm`` —
                blake3 by default, can be ``sha256`` or ``md5``;
                see #221).
            response_headers: HTTP response headers

        Returns:
            True if upload should be skipped, False otherwise
        """
        # Issue #61: forced re-processing for operator-whitelisted hosts.
        # Bypasses the hash-skip path entirely so resources from these
        # hosts get re-downloaded + re-analyzed on every push. This is
        # intended for hosts that update content in place without
        # changing the byte hash (e.g., a daily report that overwrites
        # the same URL with the same template but new underlying data),
        # or for local/peered hosts where re-download is essentially
        # free and operators want forced re-analysis.
        url = context.resource.get("url", "") if context.resource else ""
        matched_host = _host_in_always_whitelist(url)
        if matched_host:
            context.logger.info(
                "Host %r is in DOWNLOAD_ALWAYS_WHITELIST; forcing "
                "re-processing (bypassing hash-skip).",
                matched_host,
            )
            return False

        # Check if resource metadata was updated
        resource_updated = False
        resource_last_modified = context.resource.get("last_modified")
        if resource_last_modified:
            resource_last_modified = parsedate(resource_last_modified)
            file_last_modified = response_headers.get("last-modified")
            if file_last_modified:
                file_last_modified = parsedate(file_last_modified).replace(tzinfo=None)
                if file_last_modified < resource_last_modified:
                    resource_updated = True

        # Skip if hash matches and not forced
        metadata = context.metadata
        return (
            context.resource.get("hash") == file_hash
            and not metadata.get("ignore_hash")
            and not conf.IGNORE_FILE_HASH
            and not resource_updated
        )

    def _handle_zip_file(self, context: ProcessingContext, resource_format: str) -> None:
        """
        Extract ZIP file if applicable.

        Args:
            context: Processing context
            resource_format: File format

        Returns:
            None, but updates context.tmp if ZIP is extracted
        """
        if resource_format.upper() == "ZIP":
            context.logger.info("Processing ZIP file...")

            file_count, extracted_path, unzipped_format = dph.extract_zip_or_metadata(
                context.tmp, context.temp_dir, context.logger
            )

            if not file_count:
                context.logger.error("ZIP file invalid or no files found in ZIP file.")
                return None

            if file_count > 1:
                context.logger.info(
                    f"More than one file in the ZIP file ({file_count} files), "
                    f"saving metadata..."
                )
            else:
                context.logger.info(
                    f"Extracted {unzipped_format} file: {extracted_path}"
                )

            context.update_tmp(extracted_path)
