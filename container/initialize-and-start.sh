#!/bin/sh
set -e

UWSGI_FILE=${DATAPUSHER_CONFIG}/uwsgi.ini

abort () {
  echo "$@" >&2
  exit 1
}

# Fail if postgresql is not running, if that's in the sqlalchemy connection string
if echo "${DATAPUSHER_SQLALCHEMY_DATABASE_URI}" | grep -q 'postgres' \
       && ! psql -l ${DATAPUSHER_SQLALCHEMY_DATABASE_URI} > /dev/null; then
   abort "Postgresql not running"
fi

if [ ! -e $UWSGI_FILE ]; then
    cp $DATAPUSHER_CODE/container/uwsgi.ini $UWSGI_FILE
fi

datapusher_initdb $DATAPUSHER_CODE/datapusher/settings.py

# run ckan with uwsgi
exec $DATAPUSHER_VENV/bin/uwsgi -i $UWSGI_FILE
