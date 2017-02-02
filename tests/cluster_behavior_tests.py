import logging
import os
import uuid

import mock
from tornado import gen, testing

from . import base

# os.environ['ASYNC_TEST_TIMEOUT'] = '10'


class AsyncTestCase(base.AsyncTestCase):

    AUTO_CONNECT = False
    CLUSTERING = True

    @property
    def redis_port(self):
        return int(os.environ['NODE1_PORT'])


class ClusterBehaviorTests(AsyncTestCase):

    @testing.gen_test
    def test_connection_move(self):
        yield self.client.connect()
        for offset in range(0, 20):
            key = str(uuid.uuid4())
            yield self.client.set(key, b'1', ex=10)
            value = yield self.client.get(key)
            self.assertEqual(value, b'1')
