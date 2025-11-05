# -*- coding: utf-8 -*-
"""
DataPusher Plus Jobs Module

This module contains the refactored job processing pipeline for DataPusher Plus.
The monolithic jobs.py has been refactored into a clean pipeline architecture.
"""

# Re-export main entry points for backward compatibility
from ckanext.datapusher_plus.jobs.pipeline import (
    datapusher_plus_to_datastore,
    push_to_datastore,
)

__all__ = [
    "datapusher_plus_to_datastore",
    "push_to_datastore",
]
