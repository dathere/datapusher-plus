# -*- coding: utf-8 -*-
"""
DataPusher Plus Jobs Module - Backward Compatibility Wrapper

This file provides backward compatibility for code importing from the original
jobs.py module. The actual implementation has been refactored into a modular
pipeline architecture located in the jobs/ subdirectory.

For the refactored implementation, see:
- jobs/pipeline.py - Main orchestration logic
- jobs/context.py - Processing context state
- jobs/stages/ - Individual pipeline stages

Original implementation preserved in jobs_legacy.py for reference.
"""

# Import and re-export main entry points from the refactored pipeline
from ckanext.datapusher_plus.jobs.pipeline import (
    datapusher_plus_to_datastore,
    push_to_datastore,
    validate_input,
    callback_datapusher_hook,
)

# Export all public functions
__all__ = [
    "datapusher_plus_to_datastore",
    "push_to_datastore",
    "validate_input",
    "callback_datapusher_hook",
]
