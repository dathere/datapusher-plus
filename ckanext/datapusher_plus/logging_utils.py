# -*- coding: utf-8 -*-
# flake8: noqa: E501

import logging

# Define custom TRACE level (5 is below DEBUG's 10)
TRACE = 5
logging.addLevelName(TRACE, "TRACE")


# Add a trace method to the logger class
def trace(self, message, *args, **kwargs):
    if self.isEnabledFor(TRACE):
        self._log(TRACE, message, args, **kwargs)


# Add the method to the Logger class
logging.Logger.trace = trace
