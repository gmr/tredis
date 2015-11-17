import os

from tornado import concurrent
from tornado import testing

import tredis

# os.environ['ASYNC_TEST_TIMEOUT'] = '10'


class AsyncTestCase(testing.AsyncTestCase):

    def setUp(self):
        super(AsyncTestCase, self).setUp()
        self.client = tredis.RedisClient(os.getenv('REDIS_HOST', 'localhost'),
                                         int(os.getenv('REDIS_PORT', '6379')),
                                         int(os.getenv('REDIS_DB', '0')))
        self._execute_result = None

    def execute(self, parts, callback):
        future = concurrent.Future()
        future.add_done_callback(callback)
        if isinstance(self._execute_result, Exception):
            future.set_exception(self._execute_result)
        else:
            future.set_result(self._execute_result)
