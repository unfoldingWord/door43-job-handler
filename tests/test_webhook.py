from unittest import TestCase
from unittest.mock import Mock, patch

from webhook import job

from rq import get_current_job

def my_get_current_job():
    print("HERE")
    return {'id':99}

class TestWebhook(TestCase):

    @patch('rq.get_current_job', side_effect=my_get_current_job)
    def test_success(self, mocked_get_current_job_function):
        test_payload = {'something': 'anything',}
        job(test_payload)
        #output = sample_service(input)
        #expected = {
            #'hello': input
        #}
        #self.assertEqual(output, expected)

