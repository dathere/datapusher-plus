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
        # Using MD5 for file deduplication only (not for security)
        m = hashlib.md5()  # DevSkim: ignore DS126858

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
            file_hash: MD5 hash of downloaded file
            response_headers: HTTP response headers

        Returns:
            True if upload should be skipped, False otherwise
        """
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
