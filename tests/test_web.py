# -*- coding: utf-8 -*-
'''
Test whether the service can be started properly and whether the
configuration and the jobs are loaded.
'''

import os
import json

import datapusher.main as main

os.environ['JOB_CONFIG'] = os.path.join(os.path.dirname(__file__),
                                        'settings_test.py')

app = main.serve_test()


class TestWeb():

    def test_status(self):
        rv = app.get('/status')
        result_dict = json.loads(rv.data)
        assert result_dict['job_types'] == ['push_to_datastore']
        assert result_dict['name'] == 'datapusher'
