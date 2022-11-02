# Use this file for development, on a production setup (eg a CKAN production
# install) use deployment/datapusher.wsgi

import ckanserviceprovider.web as web

from datapusher.config import config

web.init()

import datapusher.jobs as jobs
# check whether jobs have been imported properly
assert(jobs.push_to_datastore)

web.app.run(config.get('HOST'), config.get('PORT'))
