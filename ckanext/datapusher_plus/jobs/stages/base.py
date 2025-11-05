# -*- coding: utf-8 -*-
"""
Base stage class for the DataPusher Plus pipeline.

All pipeline stages inherit from this base class.
"""

from abc import ABC, abstractmethod
from typing import Optional

from ckanext.datapusher_plus.jobs.context import ProcessingContext


class BaseStage(ABC):
    """
    Abstract base class for all pipeline stages.

    Each stage processes the context and returns it (possibly modified).
    Stages can skip processing by returning None.
    """

    def __init__(self, name: Optional[str] = None):
        """
        Initialize the stage.

        Args:
            name: Optional name for the stage (defaults to class name)
        """
        self.name = name or self.__class__.__name__

    @abstractmethod
    def process(self, context: ProcessingContext) -> Optional[ProcessingContext]:
        """
        Process the context through this stage.

        Args:
            context: The processing context containing all state

        Returns:
            The modified context, or None to skip this stage

        Raises:
            utils.JobError: If processing fails
        """
        pass

    def should_skip(self, context: ProcessingContext) -> bool:
        """
        Determine if this stage should be skipped.

        Override this method to add conditional stage execution.

        Args:
            context: The processing context

        Returns:
            True if the stage should be skipped, False otherwise
        """
        return False

    def __call__(self, context: ProcessingContext) -> Optional[ProcessingContext]:
        """
        Make the stage callable.

        This allows stages to be used as: stage(context)

        Args:
            context: The processing context

        Returns:
            The modified context, or None to skip
        """
        if self.should_skip(context):
            context.logger.info(f"Skipping stage: {self.name}")
            return context

        context.logger.info(f"Starting stage: {self.name}")
        result = self.process(context)
        context.logger.info(f"Completed stage: {self.name}")
        return result

    def __repr__(self) -> str:
        """String representation of the stage."""
        return f"<{self.name}>"
