"""

"""
import os
import mock
import uuid

from tornado import testing

import tredis
from tredis import exceptions

from . import base


class ConnectTests(base.AsyncTestCase):

    @testing.gen_test
    def test_bad_connect_raises_exception(self):
        client = tredis.RedisClient(str(uuid.uuid4()))
        with self.assertRaises(exceptions.ConnectError):
            yield client.connect()

    @testing.gen_test
    def test_bad_db_raises_exception(self):
        client = tredis.RedisClient(os.getenv('REDIS_HOST', 'localhost'),
                                    int(os.getenv('REDIS_PORT', '6379')),
                                    db=255)
        with self.assertRaises(exceptions.RedisError):
            yield client.connect()

    @testing.gen_test
    def test_close_invokes_iostream_close(self):
        client = tredis.RedisClient(os.getenv('REDIS_HOST', 'localhost'),
                                    int(os.getenv('REDIS_PORT', '6379')), 0)
        yield client.connect()
        with mock.patch.object(client._stream, 'close') as close:
            client.close()
            close.assert_called_once_with()

    @testing.gen_test
    def test_on_close_callback_invoked(self):
        callback_method = mock.Mock()
        client = tredis.RedisClient(os.getenv('REDIS_HOST', 'localhost'),
                                    int(os.getenv('REDIS_PORT', '6379')), 0,
                                    callback_method)
        yield client.connect()
        sock_name = client._stream.socket.getsockname()
        results = yield client.execute([b'CLIENT', b'LIST'])
        host = '[%s]' % sock_name[0] if '::' in sock_name[0] else sock_name[0]
        yield client.execute([b'CLIENT', b'KILL',
                              '%s:%s' % (host, sock_name[1])])
        callback_method.assert_called_once_with()
