from tornado import testing

import tredis

from . import base


class PipelineTests(base.AsyncTestCase):

    @testing.gen_test
    def test_empty_pipeline_raises(self):
        self.client.pipeline_start()
        with self.assertRaises(ValueError):
            yield self.client.pipeline_execute()

    def test_pipeline_responses(self):
        resp = tredis._PipelineResponses()
        self.assertEqual(len(resp), 0)
        resp.append('foo')
        self.assertEqual(len(resp), 1)
