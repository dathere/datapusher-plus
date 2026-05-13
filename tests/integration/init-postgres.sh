#!/bin/bash
# Postgres init script for the integration stack. Runs once on first
# container start (Docker entrypoint convention) to create the three
# databases DP+ needs: prefect (Prefect server), ckan (CKAN app db),
# datastore (CKAN datastore).
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE prefect;

    CREATE USER ckan WITH PASSWORD 'ckan';
    CREATE DATABASE ckan OWNER ckan;

    CREATE USER datastore_write WITH PASSWORD 'datastore';
    CREATE USER datastore_read WITH PASSWORD 'datastore';
    CREATE DATABASE datastore OWNER datastore_write;
EOSQL
