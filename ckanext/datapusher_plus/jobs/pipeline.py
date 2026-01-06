# -*- coding: utf-8 -*-
"""
DataPusher Plus Pipeline

Main orchestration logic for the refactored jobs module.
"""

import sys
import time
import logging
import tempfile
import traceback
import sqlalchemy as sa
from pathlib import Path
from typing import Dict, Any, Optional, List
from rq import get_current_job

import ckanext.datapusher_plus.utils as utils
import ckanext.datapusher_plus.helpers as dph
import ckanext.datapusher_plus.config as conf
import ckanext.datapusher_plus.datastore_utils as dsu
from ckanext.datapusher_plus.logging_utils import TRACE
from ckanext.datapusher_plus.qsv_utils import QSVCommand
from ckanext.datapusher_plus.jobs.context import ProcessingContext
from ckanext.datapusher_plus.jobs.stages.download import DownloadStage
from ckanext.datapusher_plus.jobs.stages.format_converter import FormatConverterStage
from ckanext.datapusher_plus.jobs.stages.validation import ValidationStage
from ckanext.datapusher_plus.jobs.stages.analysis import AnalysisStage
from ckanext.datapusher_plus.jobs.stages.database import DatabaseStage
from ckanext.datapusher_plus.jobs.stages.indexing import IndexingStage
from ckanext.datapusher_plus.jobs.stages.formula import FormulaStage
from ckanext.datapusher_plus.jobs.stages.metadata import MetadataStage


# Re-export validation functions for backward compatibility
def validate_input(input: Dict[str, Any]) -> None:
    """
    Validates input dictionary contains required metadata and resource_id.

    Args:
        input: Input dictionary

    Raises:
        utils.JobError: If validation fails
    """
    if "metadata" not in input:
        raise utils.JobError("Metadata missing")

    data = input["metadata"]

    if "resource_id" not in data:
        raise utils.JobError("No id provided.")


def callback_datapusher_hook(result_url: str, job_dict: Dict[str, Any]) -> bool:
    """
    Sends callback to CKAN with job status updates.

    Args:
        result_url: URL to send callback to
        job_dict: Job status dictionary

    Returns:
        True if callback successful, False otherwise
    """
    import json
    import requests

    api_token = utils.get_dp_plus_user_apitoken()
    headers: Dict[str, str] = {
        "Content-Type": "application/json",
        "Authorization": api_token,
    }

    try:
        result = requests.post(
            result_url,
            data=json.dumps(job_dict, cls=utils.DatetimeJsonEncoder),
            verify=conf.SSL_VERIFY,
            headers=headers,
        )
    except requests.ConnectionError:
        return False

    return result.status_code == requests.codes.ok


def datapusher_plus_to_datastore(input: Dict[str, Any]) -> Optional[str]:
    """
    Main function called by the datapusher_plus worker.

    Errors are caught and logged in the database.

    Args:
        input: Dictionary containing metadata and other job information

    Returns:
        Optional[str]: Returns "error" if there was an error, None otherwise
    """
    job_dict: Dict[str, Any] = dict(metadata=input["metadata"], status="running")
    callback_datapusher_hook(result_url=input["result_url"], job_dict=job_dict)

    job_id = get_current_job().id
    errored = False
    try:
        push_to_datastore(input, job_id)
        job_dict["status"] = "complete"
        dph.mark_job_as_completed(job_id, job_dict)
    except utils.JobError as e:
        dph.mark_job_as_errored(job_id, str(e))
        job_dict["status"] = "error"
        job_dict["error"] = str(e)
        log = logging.getLogger(__name__)
        log.error(f"Datapusher Plus error: {e}, {traceback.format_exc()}")
        errored = True
    except Exception as e:
        dph.mark_job_as_errored(
            job_id, traceback.format_tb(sys.exc_info()[2])[-1] + repr(e)
        )
        job_dict["status"] = "error"
        job_dict["error"] = str(e)
        log = logging.getLogger(__name__)
        log.error(f"Datapusher Plus error: {e}, {traceback.format_exc()}")
        errored = True
    finally:
        is_saved_ok = callback_datapusher_hook(
            result_url=input["result_url"], job_dict=job_dict
        )
        errored = errored or not is_saved_ok
    return "error" if errored else None


def push_to_datastore(
    input: Dict[str, Any], task_id: str, dry_run: bool = False
) -> Optional[List[Dict[str, Any]]]:
    """
    Download and parse a resource push its data into CKAN's DataStore.

    An asynchronous job that gets a resource from CKAN, downloads the
    resource's data file and, if the data file has changed since last time,
    parses the data and posts it into CKAN's DataStore.

    Args:
        input: Dictionary containing metadata and other job information
        task_id: Unique identifier for the task
        dry_run: If True, fetch and parse the data file but don't actually post the
            data to the DataStore, instead return the data headers and rows that
            would have been posted.

    Returns:
        Optional[List[Dict[str, Any]]]: If dry_run is True, returns the headers and rows
            that would have been posted. Otherwise returns None.
    """
    # Ensure temporary files are removed after run
    with tempfile.TemporaryDirectory() as temp_dir:
        return _push_to_datastore(task_id, input, dry_run=dry_run, temp_dir=temp_dir)


def _push_to_datastore(
    task_id: str,
    input: Dict[str, Any],
    dry_run: bool = False,
    temp_dir: Optional[str] = None,
) -> Optional[List[Dict[str, Any]]]:
    """
    Internal function that processes the resource through the pipeline.

    Args:
        task_id: Unique task identifier
        input: Input dictionary with metadata
        dry_run: If True, don't actually push to datastore
        temp_dir: Temporary directory path

    Returns:
        Optional list of headers dicts if dry_run is True
    """
    # Register job
    try:
        dph.add_pending_job(task_id, **input)
    except sa.exc.IntegrityError:
        raise utils.JobError("Job already exists.")

    # Setup logging
    handler = utils.StoringHandler(task_id, input)
    logger = logging.getLogger(task_id)
    logger.addHandler(handler)
    logger.addHandler(logging.StreamHandler())

    # Set log level
    try:
        log_level = getattr(logging, conf.UPLOAD_LOG_LEVEL.upper())
    except AttributeError:
        log_level = TRACE

    logger.setLevel(logging.INFO)
    logger.info(f"Setting log level to {logging.getLevelName(int(log_level))}")
    logger.setLevel(log_level)

    # Validate QSV binary exists
    if not Path(conf.QSV_BIN).is_file():
        raise utils.JobError(f"{conf.QSV_BIN} not found.")

    # Initialize QSV
    qsv = QSVCommand(logger=logger)

    # Validate input
    validate_input(input)

    # Extract metadata
    data = input["metadata"]
    ckan_url = data["ckan_url"]
    resource_id = data["resource_id"]

    # Fetch resource
    try:
        resource = dsu.get_resource(resource_id)
    except utils.JobError:
        # Retry once after 5 seconds
        time.sleep(5)
        resource = dsu.get_resource(resource_id)

    # Check if resource is datastore type
    if resource.get("url_type") == "datastore":
        logger.info("Dump files are managed with the Datastore API")
        return

    # Create processing context
    context = ProcessingContext(
        task_id=task_id,
        input=input,
        dry_run=dry_run,
        temp_dir=temp_dir,
        logger=logger,
        qsv=qsv,
        resource=resource,
        resource_id=resource_id,
        ckan_url=ckan_url,
    )

    # Create and run pipeline
    pipeline = DataProcessingPipeline()
    result_context = pipeline.execute(context)

    # Return headers if dry run
    if dry_run and result_context:
        return result_context.headers_dicts

    return None


class DataProcessingPipeline:
    """
    Orchestrates the data processing pipeline through sequential stages.

    Each stage processes the context and returns it (possibly modified).
    If a stage returns None, the pipeline stops execution.
    """

    def __init__(self):
        """Initialize the pipeline with all processing stages."""
        self.stages = [
            DownloadStage(),
            FormatConverterStage(),
            ValidationStage(),
            AnalysisStage(),
            DatabaseStage(),
            IndexingStage(),
            FormulaStage(),
            MetadataStage(),
        ]

    def execute(self, context: ProcessingContext) -> Optional[ProcessingContext]:
        """
        Execute all pipeline stages sequentially.

        Args:
            context: Initial processing context

        Returns:
            Final processing context, or None if pipeline was aborted

        Raises:
            utils.JobError: If any stage fails
        """
        for stage in self.stages:
            try:
                context = stage(context)

                # If stage returns None, stop pipeline
                if context is None:
                    context.logger.info(f"Pipeline stopped after stage: {stage.name}")
                    return None

            except utils.JobError:
                # Re-raise JobErrors as-is
                raise
            except Exception as e:
                # Wrap other exceptions
                raise utils.JobError(
                    f"Stage {stage.name} failed with error: {str(e)}"
                ) from e

        context.logger.info("Pipeline completed successfully!")
        return context
