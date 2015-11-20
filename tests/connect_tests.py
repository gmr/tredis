"""

"""
import os
import re
import mock
import uuid

from tornado import testing
from tornado import gen

import tredis
from tredis import exceptions

from . import base

ADDR_PATTERN = re.compile(r'(addr=([\.\d:]+))')


class ConnectTests(base.AsyncTestCase):

    @testing.gen_test
    def test_bad_connect_raises_exception(self):
        client = tredis.RedisClient(str(uuid.uuid4()))
        with self.assertRaises(exceptions.ConnectError):
            yield client.get('foo')

    @testing.gen_test
    def test_bad_db_raises_exception(self):
        client = tredis.RedisClient(os.getenv('REDIS_HOST', 'localhost'),
                                    int(os.getenv('REDIS_PORT', '6379')),
                                    db=255)
        with self.assertRaises(exceptions.RedisError):
            yield client.get('foo')

    @testing.gen_test
    def test_close_invokes_iostream_close(self):
        r = yield self.client.set('foo', 'bar', 1)  # Establish the connection
        with mock.patch.object(self.client._stream, 'close') as close:
            self.client.close()
            close.assert_called_once_with()

    @testing.gen_test
    def test_on_close_callback_invoked(self):
        on_close = mock.Mock()
        client = tredis.RedisClient(os.getenv('REDIS_HOST', 'localhost'),
                                    int(os.getenv('REDIS_PORT', '6379')), 0,
                                    on_close)
        result = yield client.set('foo', 'bar', 10)
        self.assertTrue(result)
        results = yield client._execute([b'CLIENT', b'LIST'])
        matches = ADDR_PATTERN.findall(results.decode('ascii'))
        print(len(matches))
        value = None
        for match, addr in matches:
            value = addr
        self.assertIsNotNone(value, 'Could not find client')
        yield client._execute([b'CLIENT', b'KILL', value.encode('ascii')])
        yield gen.sleep(0.1)  # IOLoop needs ot process for assertion
        on_close.assert_called_once_with()
