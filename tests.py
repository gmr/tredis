import os
import uuid

import mock
from tornado import concurrent
from tornado import gen
from tornado import testing

import tredis

# os.environ['ASYNC_TEST_TIMEOUT'] = '10'


class BaseTestCase(testing.AsyncTestCase):

    def setUp(self):
        super(BaseTestCase, self).setUp()
        self.client = tredis.RedisClient(os.getenv('REDIS_HOST', 'localhost'),
                                         int(os.getenv('REDIS_PORT', '6379')),
                                         int(os.getenv('REDIS_DB', '0')))
        self._execute_result = None

    def _execute(self, parts, callback):
        future = concurrent.Future()
        future.add_done_callback(callback)
        if isinstance(self._execute_result, Exception):
            future.set_exception(self._execute_result)
        else:
            future.set_result(self._execute_result)


class ConnectTests(testing.AsyncTestCase):

    @testing.gen_test
    def test_bad_connect_raises_exception(self):
        client = tredis.RedisClient(str(uuid.uuid4()))
        with self.assertRaises(tredis.ConnectError):
            yield client.connect()

    @testing.gen_test
    def test_bad_db_raises_exception(self):
        client = tredis.RedisClient(os.getenv('REDIS_HOST', 'localhost'),
                                    int(os.getenv('REDIS_PORT', '6379')),
                                    db=255)
        with self.assertRaises(tredis.RedisError):
            yield client.connect()

    @testing.gen_test
    def test_close_invokes_iostream_close(self):
        client = tredis.RedisClient(os.getenv('REDIS_HOST', 'localhost'),
                                    int(os.getenv('REDIS_PORT', '6379')), 0)
        yield client.connect()
        with mock.patch.object(client._stream, 'close') as close:
            client.close()
            close.assert_called_once_with()


class ServerCommandTests(BaseTestCase):

    @testing.gen_test
    def test_auth_raises_redis_error(self):
        yield self.client.connect()
        with self.assertRaises(tredis.RedisError):
            yield self.client.auth('boom-goes-the-silver-nitrate')

    @testing.gen_test
    def test_auth_raises_auth_error(self):
        yield self.client.connect()
        self._execute_result = tredis.RedisError(b'invalid password')
        with mock.patch.object(self.client, '_execute', self._execute):
            with self.assertRaises(tredis.AuthError):
                yield self.client.auth('boom-goes-the-silver-nitrate')

    @testing.gen_test
    def test_auth_returns_true(self):
        yield self.client.connect()
        self._execute_result = b'OK'
        with mock.patch.object(self.client, '_execute', self._execute):
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
        self.assertTrue(result)

    @testing.gen_test
    def test_select_response(self):
        yield self.client.connect()
        result = yield self.client.select(1)
        self.assertTrue(result)

class KeyCommandTests(BaseTestCase):

    @testing.gen_test
    def test_delete(self):
        yield self.client.connect()
        key = str(uuid.uuid4()).encode('ascii')
        value = str(uuid.uuid4()).encode('ascii')
        result = yield self.client.set(key, value)
        self.assertTrue(result)
        result = yield self.client.delete(key)
        self.assertTrue(result)

    @testing.gen_test
    def test_delete_multi(self):
        yield self.client.connect()
        key1 = str(uuid.uuid4()).encode('ascii')
        key2 = str(uuid.uuid4()).encode('ascii')
        value = str(uuid.uuid4()).encode('ascii')
        result = yield self.client.set(key1, value)
        self.assertTrue(result)
        result = yield self.client.set(key2, value)
        self.assertTrue(result)
        result = yield self.client.delete(key1, key2)
        self.assertTrue(result)

    @testing.gen_test
    def test_delete_missing_key(self):
        yield self.client.connect()
        key1 = str(uuid.uuid4()).encode('ascii')
        key2 = str(uuid.uuid4()).encode('ascii')
        value = str(uuid.uuid4()).encode('ascii')
        result = yield self.client.set(key1, value)
        self.assertTrue(result)
        result = yield self.client.set(key2, value)
        self.assertTrue(result)
        result = yield self.client.delete(key1, key2,
                                          str(uuid.uuid4()).encode('ascii'))
        self.assertEqual(result, 2)

    @testing.gen_test
    def test_delete_with_error(self):
        yield self.client.connect()
        key = str(uuid.uuid4()).encode('ascii')
        self._execute_result = tredis.RedisError('Test Exception')
        with mock.patch.object(self.client, '_execute', self._execute):
            with self.assertRaises(tredis.RedisError):
                yield self.client.delete(key)

    @testing.gen_test
    def test_dump(self):
        yield self.client.connect()
        key = str(uuid.uuid4()).encode('ascii')
        value = str(uuid.uuid4()).encode('ascii')
        result = yield self.client.set(key, value)
        self.assertTrue(result)
        result = yield self.client.dump(key)
        self.assertIn(value, result)

    @testing.gen_test
    def test_dump_with_invalid_key(self):
        yield self.client.connect()
        key = str(uuid.uuid4()).encode('ascii')
        result = yield self.client.dump(key)
        self.assertIsNone(result)

    @testing.gen_test
    def test_expire_and_ttl(self):
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

    @testing.gen_test
    def test_expire_with_error(self):
        yield self.client.connect()
        key = str(uuid.uuid4()).encode('ascii')
        value = str(uuid.uuid4()).encode('ascii')
        result = yield self.client.set(key, value)
        with self.assertRaises(tredis.RedisError):
            yield self.client.expire(key, 'abc')


class StringCommandTests(BaseTestCase):

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
    def test_set_ex(self):
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
    def test_set_px(self):
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
    def test_set_nx(self):
        yield self.client.connect()
        key = str(uuid.uuid4()).encode('ascii')
        value = str(uuid.uuid4()).encode('ascii')
        result = yield self.client.set(key, value, nx=True)
        self.assertTrue(result)
        result = yield self.client.get(key)
        self.assertEqual(result, value)

    @testing.gen_test
    def test_set_nx_with_value(self):
        yield self.client.connect()
        key = str(uuid.uuid4()).encode('ascii')
        value = str(uuid.uuid4()).encode('ascii')
        result = yield self.client.set(key, value, nx=True)
        self.assertTrue(result)
        result = yield self.client.get(key)
        self.assertEqual(result, value)
        result = yield self.client.set(key, value, nx=True)
        self.assertFalse(result)

    @testing.gen_test
    def test_set_xx_with_value(self):
        yield self.client.connect()
        key = str(uuid.uuid4()).encode('ascii')
        value = str(uuid.uuid4()).encode('ascii')
        result = yield self.client.set(key, value)
        self.assertTrue(result)
        result = yield self.client.get(key)
        self.assertEqual(result, value)
        result = yield self.client.set(key, value, xx=True)
        self.assertTrue(result)

    @testing.gen_test
    def test_set_xx_without_value(self):
        yield self.client.connect()
        key = str(uuid.uuid4()).encode('ascii')
        value = str(uuid.uuid4()).encode('ascii')
        result = yield self.client.set(key, value, xx=True)
        self.assertFalse(result)
