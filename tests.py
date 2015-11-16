import os
import time
import uuid

import mock
from tornado import gen
from tornado import ioloop
from tornado import testing

import tredis

# os.environ['ASYNC_TEST_TIMEOUT'] = '10'


class BaseTestCase(testing.AsyncTestCase):

    def setUp(self):
        super(BaseTestCase, self).setUp()
        self.client = tredis.RedisClient(os.getenv('REDIS_HOST'),
                                         os.getenv('REDIS_PORT'),
                                         os.getenv('REDIS_DB'))
        self._execute_return = None

    @gen.coroutine
    def _execute(self, parts):
        raise gen.Return(self._execute_return)


class ConnectTests(testing.AsyncTestCase):

    @testing.gen_test
    def test_bad_connect_raises_exception(self):
        client = tredis.RedisClient(str(uuid.uuid4()))
        with self.assertRaises(tredis.ConnectError):
            yield client.connect()

    @testing.gen_test
    def test_bad_db_raises_exception(self):
        client = tredis.RedisClient(os.getenv('REDIS_HOST'),
                                    os.getenv('REDIS_PORT'),
                                    db=255)
        with self.assertRaises(tredis.RedisError):
            yield client.connect()


class ServerCommandTests(BaseTestCase):

    @testing.gen_test
    def test_auth_raises_exception(self):
        yield self.client.connect()
        with self.assertRaises(tredis.AuthError):
            yield self.client.auth('boom-goes-the-silver-nitrate')

    @testing.gen_test
    def test_auth_returns_true(self):
        yield self.client.connect()
        with mock.patch.object(self.client, '_execute') as execute:
            self._execute_return = b'OK'
            execute.side_effect = self._execute
            result = yield self.client.auth('password')
            self.assertTrue(result)

    @testing.gen_test
    def test_echo_response(self):
        yield self.client.connect()
        value = b'echo-test'
        result = yield self.client.echo(value)
        self.assertEqual(result, value)

    @testing.gen_test
    def test_ping_response(self):
        yield self.client.connect()
        result = yield self.client.ping()
        self.assertEqual(result, b'PONG')

    @testing.gen_test
    def test_quit_response(self):
        yield self.client.connect()
        result = yield self.client.quit()
        self.assertEqual(result, b'OK')

    @testing.gen_test
    def test_select_response(self):
        yield self.client.connect()
        result = yield self.client.select(1)
        self.assertEqual(result, b'OK')


class StringAndKeyCommandTests(BaseTestCase):

    @testing.gen_test
    def test_simple_set_and_get(self):
        yield self.client.connect()
        key = str(uuid.uuid4()).encode('ascii')
        value = str(uuid.uuid4()).encode('ascii')
        result = yield self.client.set(key, value)
        self.assertTrue(result)
        result = yield self.client.get(key)
        self.assertEqual(result, value)

    @testing.gen_test
    def test_simple_set_ex(self):
        yield self.client.connect()
        key = str(uuid.uuid4()).encode('ascii')
        value = str(uuid.uuid4()).encode('ascii')
        result = yield self.client.set(key, value, ex=1)
        self.assertTrue(result)
        result = yield self.client.get(key)
        self.assertEqual(result, value)
        yield gen.sleep(1.0)
        result = yield self.client.get(key)
        self.assertIsNone(result)

    @testing.gen_test
    def test_simple_set_px(self):
        yield self.client.connect()
        key = str(uuid.uuid4()).encode('ascii')
        value = str(uuid.uuid4()).encode('ascii')
        result = yield self.client.set(key, value, px=100)
        self.assertTrue(result)
        result = yield self.client.get(key)
        self.assertEqual(result, value)
        yield gen.sleep(0.300)
        result = yield self.client.get(key)
        self.assertIsNone(result)


    @testing.gen_test
    def test_simple_set_expire_and_ttl(self):
        yield self.client.connect()
        key = str(uuid.uuid4()).encode('ascii')
        value = str(uuid.uuid4()).encode('ascii')
        ttl = 5
        result = yield self.client.set(key, value)
        self.assertTrue(result)
        result = yield self.client.expire(key, ttl)
        self.assertTrue(result)
        result = yield self.client.ttl(key)
        self.assertAlmostEqual(result, ttl)

