# -*- coding: utf-8 -*-
"""Shim setup.py — kept only for babel ``message_extractors``.

All other project metadata lives in pyproject.toml (PEP 621). Babel does not
yet read ``message_extractors`` from pyproject.toml, so this minimal setup.py
remains. Do not add other config here.
"""

from setuptools import setup

setup(
    message_extractors={
        "ckanext": [
            ("**.py", "python", None),
            ("**.js", "javascript", None),
            ("**/templates/**.html", "ckan", None),
        ],
    },
)
