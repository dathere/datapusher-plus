# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.21] - 2022-05-04
### Added
* additional analysis & preparation steps enabled by qsv
  * automatic deduplication #25
  * validation/UTF-8 transcoding of CSV files
  * automatic resource alias (aka Postgres view) creation #26
  * smart date inferencing #28
* added an init_db command for initializing jobs_store db by @categulario (#3)
* added JOB_CONFIG environment variable by @categulario (#6)
* added package install by @categulario (#16)
* Postgres COPY to datastore
* additional data types inferred and changed postgres type mapping
  (String -> text; Float -> numeric; Integer -> numeric; Date -> timestamp;
  DateTime -> timestamp; NULL -> text)
* added requirement to create a Postgres application role/user with SUPERUSER privs
  to do native Postgres operations through psycopg2
* added more verbose logging with elapsed time per phase (Analysis & Preparation Phase,
  Copy Phase, Total elapsed time)
* added Containerfile reference by @categulario
* added a Changelog using the Keep a Changelog template.
* published `datapusher-plus` on pypi
* added a GitHub action to publish a pypi package on release
* added a Roadmap Tracking EPIC, that will always be updated as DP+ evolves #5

### Changed
* DP+ requires at least Python 3.7, as it needs `CAPTURE_OUTPUT` option in `subprocess.run`.
  It should still continue to work with CKAN<=2.8 as it runs in its own virtualenv.
* replaced messytables with qsv
* removed old "chunked" datastore inserts
* removed requirements.txt, requirements/dependencies are now in setup.py
* expanded documentation - rationale for DP+; addl config vars; modified installation procedure
* made DP+ a detached Datapusher fork to maintain own issue tracker, discussions, releases, etc.
