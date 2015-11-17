import os
import uuid

from tornado import concurrent
from tornado import gen
from tornado import testing

import tredis

# os.environ['ASYNC_TEST_TIMEOUT'] = '10'


class AsyncTestCase(testing.AsyncTestCase):

    DEFAULT_EXPIRATION = 5

    def setUp(self):
        super(AsyncTestCase, self).setUp()
        self.client = tredis.RedisClient(os.getenv('REDIS_HOST', 'localhost'),
                                         int(os.getenv('REDIS_PORT', '6379')),
                                         int(os.getenv('REDIS_DB', '0')))
        self._execute_result = None

    @gen.coroutine
    def expiring_set(self, key, value, expiration=None, nx=None, xx=None):
        result = yield self.client.set(key, value,
                                       expiration or self.DEFAULT_EXPIRATION,
                                       nx=nx, xx=xx)
        raise gen.Return(result)

    def _execute(self, parts, callback):
        future = concurrent.Future()
        future.add_done_callback(callback)
        if isinstance(self._execute_result, Exception):
            future.set_exception(self._execute_result)
        else:
            future.set_result(self._execute_result)

    def uuid4(self, qty=1):
        if qty == 1:
            return str(uuid.uuid4()).encode('ascii')
        else:
            return tuple([str(uuid.uuid4()).encode('ascii')
                          for i in range(0, qty)])
