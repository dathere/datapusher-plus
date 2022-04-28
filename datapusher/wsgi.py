import os
import ckanserviceprovider.web as web

if 'JOB_CONFIG' not in os.environ:
    os.environ['JOB_CONFIG'] = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), 'settings.py'
    )

web.init()

import datapusher.jobs as jobs

application = web.app
