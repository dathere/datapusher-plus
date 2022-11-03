import os


# job_store database
# make sure to set this up properly by
# 1) creating the job_store and datapusher_jobs user in PostgreSQL FIRST
# 2) adjust the settings here, and
# 3) run the INIT_DB command
SQLALCHEMY_DATABASE_URI = os.environ.get('DATAPUSHER_SQLALCHEMY_DATABASE_URI',
                                         'postgresql://datapusher_jobs:YOURPASSWORD@localhost/datapusher_jobs')

# PostgreSQL COPY settings
# set this to the same value as your ckan.datastore.write_url
WRITE_ENGINE_URL = os.environ.get(
    'DATAPUSHER_WRITE_ENGINE_URL', 'postgresql://datapusher:THEPASSWORD@localhost/datastore_default')
