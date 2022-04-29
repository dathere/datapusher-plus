#!/bin/sh
set -e

UWSGI_FILE=${DATAPUSHER_CONFIG}/uwsgi.ini

abort () {
  echo "$@" >&2
  exit 1
}

# Fail if postgresql is not running
if ! pg_isready -h "${POSTGRES_HOST}" -U "${POSTGRES_USER}"; then
    abort "Postgresql not running"
fi

if [ ! -e $UWSGI_FILE ]; then
    cp $DATAPUSHER_CODE/container/uwsgi.ini $UWSGI_FILE
fi

datapusher_initdb $DATAPUSHER_CODE/datapusher/settings.py

# run ckan with uwsgi
exec $DATAPUSHER_VENV/bin/uwsgi -i $UWSGI_FILE
