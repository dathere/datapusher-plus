# -*- coding: utf-8 -*-
"""
ProcessingContext for the DataPusher Plus pipeline.

This class holds all state that is passed between pipeline stages.
"""

import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field

from ckanext.datapusher_plus.qsv_utils import QSVCommand


@dataclass
class ProcessingContext:
    """
    Context object that holds all state for the data processing pipeline.

    This object is passed through each stage of the pipeline and is modified
    by each stage to track progress and intermediate results.
    """

    # Task/Job identification
    task_id: str
    input: Dict[str, Any]
    dry_run: bool = False

    # Directories and file paths
    temp_dir: str = ""
    tmp: str = ""  # Current working CSV file (changes throughout pipeline)

    # Logging and utilities
    logger: Optional[logging.Logger] = None
    qsv: Optional[QSVCommand] = None

    # Resource information (from CKAN)
    resource: Dict[str, Any] = field(default_factory=dict)
    resource_id: str = ""
    resource_url: str = ""
    ckan_url: str = ""

    # Headers and schema
    headers_dicts: List[Dict[str, Any]] = field(default_factory=list)
    headers: List[str] = field(default_factory=list)
    original_header_dict: Dict[int, str] = field(default_factory=dict)

    # Statistics and metadata
    dataset_stats: Dict[str, Any] = field(default_factory=dict)
    resource_fields_stats: Dict[str, Any] = field(default_factory=dict)
    resource_fields_freqs: Dict[str, Any] = field(default_factory=dict)

    # Datastore information
    existing_info: Optional[Dict[str, Any]] = None
    rows_to_copy: int = 0
    copied_count: int = 0

    # Timing information
    timer_start: float = 0.0

    # Processing flags and results
    pii_found: bool = False
    file_hash: str = ""
    content_length: int = 0

    # Intermediate files (for tracking)
    qsv_index_file: str = ""

    @property
    def metadata(self) -> Dict[str, Any]:
        """Convenience property to access input metadata."""
        return self.input.get("metadata", {})

    def update_tmp(self, new_tmp: str) -> None:
        """
        Update the current working CSV file path.

        Args:
            new_tmp: Path to the new temporary CSV file
        """
        self.tmp = new_tmp
        self.logger.log(5, f"Updated tmp file to: {new_tmp}")  # TRACE level

    def add_stat(self, key: str, value: Any) -> None:
        """
        Add a statistic to the dataset stats.

        Args:
            key: Statistics key
            value: Statistics value
        """
        self.dataset_stats[key] = value
