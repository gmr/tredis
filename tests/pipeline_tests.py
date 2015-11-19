from tornado import testing

from . import base


class PipelineTests(base.AsyncTestCase):

    @testing.gen_test
    def test_empty_pipeline_raises(self):
        self.client.pipeline_start()
        with self.assertRaises(ValueError):
            yield self.client.pipeline_execute()
