import os
import uuid

DEBUG = False
TESTING = False
SECRET_KEY = str(uuid.uuid4())
USERNAME = str(uuid.uuid4())
PASSWORD = str(uuid.uuid4())

NAME = 'datapusher'

# Webserver host and port

HOST = os.environ.get('DATAPUSHER_HOST', '0.0.0.0')
PORT = os.environ.get('DATAPUSHER_PORT', 8800)

# Database
SQLALCHEMY_DATABASE_URI = os.environ.get('DATAPUSHER_SQLALCHEMY_DATABASE_URI', 'sqlite:////tmp/job_store.db')

# PostgreSQL COPY settings
# set this to the same value as your ckan.datastore.write_url
WRITE_ENGINE_URL = os.environ.get('DATAPUSHER_WRITE_ENGINE_URL', 'postgresql://datapusher:THEPASSWORD@localhost/datastore_default')

AUTO_ALIAS = bool(int(os.environ.get('DATAPUSHER_AUTO_ALIAS', '1')))

# qsv settings
QSV_BIN = os.environ.get('DATAPUSHER_QSV_BIN', '/usr/local/bin/qsvlite')
QSV_DEDUP = bool(int(os.environ.get('DATAPUSHER_QSV_DEDUP', '1')))
PREVIEW_ROWS = int(os.environ.get('DATAPUSHER_PREVIEW_ROWS', '10000'))
DEFAULT_EXCEL_SHEET = int(os.environ.get('DATAPUSHER_DEFAULT_EXCEL_SHEET', '0'))

# Download and streaming settings
MAX_CONTENT_LENGTH = int(os.environ.get('DATAPUSHER_MAX_CONTENT_LENGTH', '1024000'))
CHUNK_SIZE = int(os.environ.get('DATAPUSHER_CHUNK_SIZE', '16384'))
CHUNK_INSERT_ROWS = int(os.environ.get('DATAPUSHER_CHUNK_INSERT_ROWS', '250'))
DOWNLOAD_TIMEOUT = int(os.environ.get('DATAPUSHER_DOWNLOAD_TIMEOUT', '30'))

# Verify SSL
SSL_VERIFY = bool(int(os.environ.get('DATAPUSHER_SSL_VERIFY', '1')))

# logging
LOG_FILE = os.environ.get('DATAPUSHER_LOG_FILE', '/tmp/ckan_service.log')
STDERR = bool(int(os.environ.get('DATAPUSHER_STDERR', '1')))

# other config values that can be overriden here if you don't want to modify jobs.py
# see jobs.py for their current default values
# DATELIKE_FIELDNAMES
# TYPE_MAPPING
# TYPES 
