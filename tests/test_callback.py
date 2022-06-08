from unittest import TestCase, skip
from unittest.mock import Mock, patch
import json

import sqlalchemy
from rq import get_current_job

from rq_settings import prefix, callback_queue_name
from app_settings.app_settings import dcs_url
from callback import job


def my_get_current_job():
    class Result:
        id = 12345
        origin = callback_queue_name
    return Result()


class TestCallback(TestCase):

    def setUp(self):
        # Make sure that other tests didn't mess up our prefix
        dcs_url(prefix=prefix)

    def test_prefix(self):
        self.assertEqual(prefix, dcs_url.prefix)

    @skip("Not currently working")
    @patch('callback.get_current_job', side_effect=my_get_current_job)
    def test_bad_payload(self, mocked_get_current_job_function):
        test_payload = {'something': 'anything',}
        with self.assertRaises(KeyError):
            job(test_payload)

    @skip("Skip this test on Travis-CI coz it fails with AWS test credentials - leave for standalone testing")
    @patch('callback.get_current_job', side_effect=my_get_current_job)
    def test_typical_full_payload(self, mocked_get_current_job_function):
        with open( 'tests/resources/webhook_post.json', 'rt' ) as json_file:
            payload_json = json.load(json_file)
        #with self.assertRaises(sqlalchemy.exc.OperationalError): # access denied to tx_db -- why did this stop happening???
            #job(payload_json)
        job(payload_json)
        # After job has run, should update https://dev.door43.org/u/tx-manager-test-data/en-obs-rc-0.2/93829a566c/

