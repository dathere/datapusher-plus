# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.0.0] - 2025-04-25

## Highlights
Data Resource Upload First (DRUF) Workflow is here!
A workflow that flips the old CKAN traditional data ingestion on its head.
 * Instead of filling out the metadata first and then uploading the data, users upload data resources first 
 * In a few seconds, even for very large datasets, analysis and validation is done while precompiling statistical metadata
 * This precompiled metadata are then used by formulas defined in the scheming yaml files to either precompute other metadata fields and/or to offer metadata suggestions
 * Formulas use the same powerful Jinja2 template engine that powers CKAN's templating system.
 * It comes with an extensible library of Jinja2 filters/functions that can be used in formulas ala Excel.

The DRUF reinvents CKAN data ingestion - making it easier for Data Publishers to ensure their data catalog has high-quality, high-resolution metadata that actually reflects the and describes the data in the catalog.

---

### Added
* Data Resource Upload First (DRUF) Workflow
  * Enhanced resource validation for DRUF workflow
  * Formulas for precomputing metadata/metadata sugggestions
  * Spatial file support - supports GeoJSON and Shapefiles
* Support for CKAN 2.9 compatibility in CLI operations
* Enhanced error handling and logging for resource uploads

### Changed
* Updated CLI interface to work with CKAN 2.9
* Refactored resource upload process to support DRUF workflow
* Improved error messages and user feedback
* Enhanced configuration handling

### Fixed
* Various bug fixes and improvements for CKAN 2.9 compatibility
* Resource upload process reliability improvements

### Contributors
* @tino097
* @minhajuddin2510
* @jqnatividad

**Full Changelog**: https://github.com/dathere/datapusher-plus/compare/1.0.4...2.0.0

## [1.0.4] - 2025-01-15

## [1.0.3] - 2024-10-30

### Changed
* Ensure we are always using the same token setting for datapusher
* Fix iconv
* Fix the api_token config variable and fix for default views creation
* Migration added

### Contributors
* @tino097
* @avdata99

## [1.0.2] - 2024-09-16

### Changed
* Update README file for DP+ as extension
* Fix MANIFEST.in
* Migrate cli commands
* Fix init db command
* Config part
* Database migrations
* Update readme
* Fix yaml extension in MANIFEST.in
* Fix datefmt compatability with qsv in dev-v1.0
* Remove obsolete assets

### Contributors
* @Zharktas
* @tino097
* @pdelboca

## [1.0.1] - 2024-05-22

### Changed
* Replace http requests with actions
* Fix calling package action for resource

### Contributors
* @tino097

## [1.0.0] - 2024-05-06

### Added
* Convert the datapusher to work as plugin
* Feature db models
* Add migration script
* Rewrite resource URL if it differs from the defined ckan_url

### Changed
* Code cleanup
* Rewrite resource url
* Sync with master

### Contributors
* @jhbruhn
* @tino097
* @TomeCirun

## [0.16.4] - 2024-01-23

### Changed
* sync read buffer with buffer size of copyexpert

### Contributors
* @jqnatividad

## [0.16.3] - 2024-01-23

### Changed
* make COPY_READBUFFER_SIZE a configurable parameter

### Contributors
* @jqnatividad

## [0.16.2] - 2024-01-23

### Changed
* explicitly create a large read buffer when reading CSV when COPYing files to the datastore

## [0.16.1] - 2024-01-15

### Fixed
* fix utf8 encoding check, replacing NamedTemporaryFile approach, with Temporary Directory approach

### Note
* Requires `uchardet` for the encoding check (`apt-get install uchardet`)

## [0.16.0] - 2024-01-11

### Added
* Update README.md
* Use a temporary directory to manage temporary files
* Utf8 conversion
* import syntax and ckanserviceprovider version
* upgrade container qsv to 0.118.0
* Fixed init of index_elapsed in case auto_index is off

### Changed
* caught a missing variable

### Contributors
* @bzar
* @categulario
* @hjhornbeck
* @EricSoroos
* @minhajuddin2510

## [0.15.0] - 2023-06-26

### Fixed
* removed DOWNLOAD_PREVIEW_ONLY as its unreliable with CSVs and corrupted Excel files
* removed SUMMARY_STATS_WITH_PREVIEW, which doesn't make sense without DOWNLOAD_PREVIEW_ONLY

## [0.14.1] - 2023-06-26

### Changed
* made a mistake publishing 0.14.0, neglecting to bump setup version
* add a note about using glibc-2.31 version of qsv if the Linux distro running it has an older version of GNU C library (e.g. Ubuntu 18.04 and Debian 11)

## [0.14.0] - 2023-06-26

### Added
* More robust file format detection, also now prompts the user to specify the file format if it cannot infer it from the server's content-type header or the file's extension

### Changed
* Minimum QuickSilver version is now 0.108.0, which features a more robust and faster `input` command DP+ uses for transcoding and normalization of CSVs

### Fixed
* Fixed file format detection
* Removed SNIFF_DELIMITER setting which was causing DP+ to periodically sniff non-comma delimiters even if the file was using comma delimiters

## [0.13.2] - 2023-06-22

### Fixed
* Added `tzdata` dependency and missing `logging` import

### Contributors
* @minhajuddin2510

## [0.13.1] - 2023-06-22

### Changed
* Reordered imports for clarity
* Minor Download improvements to make streaming download more robust as we're doing streaming downloads when using preview rows by doing request in a with clause
* added vscode setting to use black formatter

### Fixed
* added missing dependencies for `pytz` and `python-dateutil`. These new dependencies are required because of the fix in 0.13.0 that checked if a resource's metadata has been modified, allowing a DP+ job even if the file hash has not changed (e.g. when the Data Dictionary data types are changed and the user wants the resource file re-pushed to use the new data types)

## [0.13.0] - 2023-06-16

### Added
* Add unsafe headers configuration settings. This allows DP+ to use an alternate unsafe prefix when sanitizing column names
* Add SNIFF_DELIMITER setting. This allows DP+ to automatically infer the delimiter used by a CSV file if its not a comma
* The inferred Data Dictionary now also has a "Unit" column. Note that you'll still need to modify your CKAN theme to expose the Unit field in the Data Dictionary tab

### Changed
* set minimum qsv version to 0.107.0

### Fixed
* Allow url parameters. This allows DP+ to process links with URL parameters. Just be sure to specify the resource format to one of the supported DP+ formats so it will be processed
* Properly handle when there is no timezone info when checking if a resource is updated

## [0.12.0] - 2023-05-19

### Changed
* Use single source of configuration
* Containerfile dependencies

### Fixed
* Don't crash when not given content-length header
* Use `--prefer-dmy` with `qsv` instead of `--prefer_dmy`
* Don't crash on missing original column name
* Allow reupload of file if resource metadata has changed
* Reset resource.preview_rows to False if existing resource falls below preview_rows threshold

### Contributors
* @bluepython508

## [0.11.0] - 2023-04-10

### Added
* Added link to datapusher-plus docker
* Added uninstallation procedure
* Added more comments in the main jobs.py process where all the main work is done
* Added details about what qsv analysis enables

### Changed
* Revamped documentation to streamline installation
* set config.py to more conservative defaults
* set minimum QSV version to 0.99.0

### Fixed
* Container packaging fixes
* Fix error handling in validate
* pinned ckanserviceprovider to 1.1.0 and APScheduler to 3.9.1.post1

### Contributors
* @Zharktas
* @EricSoroos
* @minhajuddin2510

## [0.10.1] - 2023-02-03

### Changed
* add separate AUTO_UNIQUE_INDEX setting
* improved Development Installation procedure
* improved Datapusher+ Configuration section, with heavily commented dot-env.template
* bumped qsv from 0.87.0 to 0.87.1, with improved safenames sanitizing
* added qsv version checks

## [0.9.0] - 2023-01-30

### Added
* Updated the readme to include locale installation
* Initial implementation of PII screening

### Contributors
* @jqnatividad
* @minhajuddin2510

## [0.8.0] - 2023-01-18

**More detailed release notes forthcoming...**

## [0.7.0] - 2023-01-17

### Fixed
* fix import of MutableMapping from collections.abc

### Contributors
* @ctrepka

## [0.6.0] - 2023-01-06

### Added
* validate excel file exported CSVs as well, as they can potentially be invalid CSVs (e.g. differing column counts per row)
* support negative values for PREVIEW_ROWS to start previewing from the end of a file (e.g. -1000 = last 1000 rows)
* if an Excel file is invalid or password-protected, show additional file metadata by using the `file` command
* add PREFER_DMY setting for parsing dates and doing column date inferencing (otherwise, the default is YMD)
* add logic to DROP VIEWS if ALIAS_UNIQUE is false, and show warning on datastore log
* implement smart auto-indexing which is controlled by AUTO_INDEX_THRESHOLD (default: 3) and AUTO_INDEX_DATES (default: true)
* improved log messages (comma-separated formatting for numbers, context-sensitive normalizing/transcoding messages, etc.)
* applied Black formatter to jobs.py

### Removed
* remove obsolete CHUNK_INSERT_ROWS setting as we now do Postgres COPY

## [0.5.1] - 2023-01-05

### Fixed
* Fixed "no data rows" bug

### Added
* added more implementation comments and TODOS

## [0.5.0] - 2023-01-04

### Added
* new AUTO_ALIAS_UNIQUE setting with a default of false. This ensure the alias is stable if the resource is updated
* two-stage normalization/validation of incoming files, ensuring that we can gracefully handle corrupt files
* ensure column names are "safe" (e.g. valid postgresql column identifiers), modifying them as required - while still retaining the original "unsafe" name in the data dictionary

### Changed
* updated deployment instructions

## [0.4.0] - 2022-12-13

### Added
* smart data dictionary
* "safe" column names handling
* uwsgi deployment fixed
* send the env file explicitly

### Contributors
* @TomeCirun

## [0.3.1] - 2022-12-09

### Changed
* refactored log message right before qsv preprocessing starts

## [0.3.0] - 2022-12-09

### Changed
* spreadsheet files that are added as a link are parsed properly so long as the resource format is set
* header names are sanitized so they are valid Postgres column identifiers

### Fixed
* wsgi deployment fixed

## [0.2.0] - 2022-12-07

### Changed
* fix UnboundLocalError
* Add datapusherplus config
* fix resource download
* delete settings.py

### Contributors
* @TomeCirun

## [0.1.0] - 2022-09-09

### Added
* available smarter data type mapping to Postgres data types. By looking at the min/max values of a column,
  we can infer the best postgres data type - integer, bigint or numeric, instead of using the numeric Postgres type
  for all integers.
  This is done by changing TYPE_MAPPING of `Integer` from `numeric` to `smartint`.
* Add resource preview metadata fields:
    * `preview` - if the resource is a preview, and not the entire file, containing only the first PREVIEW_ROWS of the file (boolean)
    * `preview_rows` - the number of rows of the preview
    * `total_record_count` - the actual number of rows of the file

### Changed
* change mapping of inferred Date fields to the Postgres `date` data type, instead of using Postgres `timestamp` data type for
  both Date (YYYY-MM-DD) and Datetime (YYYY-MM-DD HH:MM:SS TZ) columns.
* warn when duplicates are found, instead of info
* decreased default preview to 1,000 rows
* better error handling when calling qsv binary
* update instructions to use the latest qsv binary - qsv 0.67.0

### Fixed
* trimmed header and column values when processing spreadsheets. As spreadsheets are more often than not, manually curated,
  there are often invisible whitespaces that "look" right that may cause invalid CSVs - e.g. column names with leading/trailing whitespaces
  that cause Postgres errors when columns are created using the Excel column name.

## [0.0.23] - 2022-05-09
 ### Changed
* use `psycopg2-binary` instead of `psycopg2` to ease installation and eliminate need to have postgres dev files
* made logging messages auto-dedup aware if dupes are detected, by adding "unique" qualifier to record count
* pointed to the latest qsv version (0.46.1) with the excel off by 1 fix
* added note about nightly builds of qsv for maximum performance
* added note about additional DP+ supported Excel and TSV subformats
* use JOB_CONFIG consistently for setting DP+ settings
* made qsvdp the default QSV_BIN
* added note about how to install python 3.7 and above in DP+ virtual environment

### Removed
* removed Hitchiker's guide quote from setup.py epilog
* removed `six` as DP+ requires at least python 3.7
* removed `pytest` step in Development installation until the tests are adapted to DP+

### Fixed
* fixed development installation procedure, so no assumptions are made
* fixed production deployment procedure and made it more detailed
* fixed off by 1 error in `excel` export message in qsv

## [0.0.21] - 2022-05-04
### Added
* additional analysis & preparation steps enabled by qsv
  * blazing fast, guaranteed data type inferences with comprehensive descriptive statistics
  * configurable Excel/ODS exports (supports XLS, XLSX, ODS, XLSM, and XLSB formats)
  * automatic deduplication (https://github.com/dathere/datapusher-plus/pull/25)
  * [RFC 4180](https://datatracker.ietf.org/doc/html/rfc4180) validation and UTF-8 transcoding of CSV files
  * smart date inferencing (https://github.com/dathere/datapusher-plus/pull/28)
  * automatic preview subset creation
  * DP+ optimized qsv binary (qsvdp) - which is 6x smaller than regular qsv, 3x smaller than qsvlite, and with 
    the self-update engine removed.
* added an init_db command for initializing jobs_store db by [@categulario](https://github.com/categulario) (https://github.com/dathere/datapusher-plus/pull/3)
* automatic resource alias (aka Postgres view) creation (https://github.com/dathere/datapusher-plus/pull/26)
* added JOB_CONFIG environment variable by [@categulario](https://github.com/categulario) (https://github.com/dathere/datapusher-plus/pull/6)
* added package install by [@categulario](https://github.com/categulario) (https://github.com/dathere/datapusher-plus/pull/16)
* Postgres COPY to datastore
* additional data types inferred and changed postgres type mapping
  (String -> text; Float -> numeric; Integer -> numeric; Date -> timestamp;
  DateTime -> timestamp; NULL -> text)
* added requirement to create a Postgres application role/user with SUPERUSER privs
  to do native Postgres operations through psycopg2
* added more verbose logging with elapsed time per phase (Analysis & Preparation Phase,
  Copy Phase, Total elapsed time)
* added Containerfile reference by [@categulario](https://github.com/categulario) (https://github.com/dathere/datapusher-plus/pull/22)
* added `datasize` dependency for human-readable file sizes in messages
* added a Changelog using the Keep a Changelog template.
* published `datapusher-plus` on pypi
* added a GitHub action to publish a pypi package on release
* added a Roadmap Tracking EPIC, that will always be updated as DP+ evolves https://github.com/dathere/datapusher-plus/issue/5

### Changed
* DP+ requires at least Python 3.7, as it needs `CAPTURE_OUTPUT` option in `subprocess.run`.
  It should still continue to work with CKAN<=2.8 as it runs in its own virtualenv.
* replaced messytables with qsv
* removed old "chunked" datastore inserts
* removed requirements.txt, requirements/dependencies are now in setup.py
* expanded documentation - rationale for DP+; addl config vars; modified installation procedure; scnshots; etc.
* improvements to install instructions by [@categulario](https://github.com/categulario) (https://github.com/dathere/datapusher-plus/pull/20)
* made DP+ a detached Datapusher fork to maintain own issue tracker, discussions, releases, etc.
