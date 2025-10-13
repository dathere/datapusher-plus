# -*- coding: utf-8 -*-
# flake8: noqa: E501

# Always prefer setuptools over distutils
from setuptools import setup, find_packages
from codecs import open  # To use a consistent encoding
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the relevant file
with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="datapusher-plus",
    # Versions should comply with PEP440.  For a discussion on single-sourcing
    # the version across setup.py and the project code, see
    # http://packaging.python.org/en/latest/tutorial.html#version
    version="2.0.0",
    description="Next-generation, extensible Data Ingestion framework for CKAN. Accelerated by qsv.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    # The project's main homepage.
    url="https://github.com/dathere/datapusher-plus",
    # Choose your license
    license="AGPL",
    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        "Development Status :: 5 - Production/Stable",
        # Pick your license as you wish (should match "license" above)
        "License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)",
        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
    ],
    # What does your project relate to?
    keywords="ckan csv tsv xls ods excel qsv",
    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages().
    packages=find_packages(exclude=["tests*"]),
    # setup_requires=['wheel'],
    # List run-time dependencies here.  These will be installed by pip when your
    # project is installed. For an analysis of "install_requires" vs pip's
    # requirements files see:
    # https://packaging.python.org/en/latest/discussions/install-requires-vs-requirements/
    install_requires=[
        # CKAN extensions should not list dependencies here, but in a separate
        # ``requirements.txt`` file.
        #
        # http://docs.ckan.org/en/latest/extensions/best-practices.html#add-third-party-libraries-to-requirements-txt
        "jinja2>=3.1.4",
        "fiona==1.10.1",
        "pandas>=2.2.3",
        "semver==3.0.4",
        "shapely>=2.1.0",
        "pyproj>=3.7.1",
    ],
    # If there are data files included in your packages that need to be
    # installed, specify them here.  If using Python 2.6 or less, then these
    # have to be included in MANIFEST.in as well.
    package_data={},
    # Although 'package_data' is the preferred approach, in some case you may
    # need to place data files outside of your packages.
    # see http://docs.python.org/3.8/distutils/setupscript.html#installing-additional-files
    # In this case, 'data_file' will be installed into '<sys.prefix>/my_data'
    data_files=[],
    # To provide executable scripts, use entry points in preference to the
    # "scripts" keyword. Entry points provide cross-platform support and allow
    # pip to create the appropriate form of executable for the target platform.
    entry_points="""
        [ckan.plugins]
        datapusher_plus=ckanext.datapusher_plus.plugin:DatapusherPlusPlugin

        [babel.extractors]
        ckan = ckan.lib.extract:extract_ckan

    """,
    # message_extractors={
    #     "ckanext": [
    #         ("**.py", "python", None),
    #         ("**.js", "javascript", None),
    #         ("**/templates/**.html", "ckan", None),
    #     ],
    # },
)
