from tornado import testing

from tredis import exceptions

from . import base


class PipelineTests(base.AsyncTestCase):

    @testing.gen_test
    def test_empty_pipeline_raises(self):
        yield self.client.connect()
        self.client.pipeline_start()
        with self.assertRaises(ValueError):
            yield self.client.pipeline_execute()

    @testing.gen_test
    def test_execute_with_pipeline_raises(self):
        yield self.client.connect()
        self.client.pipeline_start()
        with self.assertRaises(exceptions.PipelineError):
            yield self.client._execute(['foo', 'bar'])

