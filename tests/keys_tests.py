"""

"""
import mock
import time
import uuid

from tornado import testing

from tredis import exceptions

from . import base


class KeyCommandTests(base.AsyncTestCase):

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
        self._execute_result = exceptions.RedisError('Test Exception')
        with mock.patch.object(self.client, 'execute', self.execute):
            with self.assertRaises(exceptions.RedisError):
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
        with self.assertRaises(exceptions.RedisError):
            yield self.client.expire(str(uuid.uuid4()).encode('ascii'), 'abc')

    @testing.gen_test
    def test_expireat_and_ttl(self):
        yield self.client.connect()
        key = str(uuid.uuid4()).encode('ascii')
        value = str(uuid.uuid4()).encode('ascii')
        result = yield self.client.set(key, value)
        self.assertTrue(result)

        timestamp = int(time.time()) + 5
        result = yield self.client.expireat(key, timestamp)
        self.assertTrue(result)
        result = yield self.client.ttl(key)
        self.assertLessEqual(result, 5)
        self.assertGreater(result, 0)

    @testing.gen_test
    def test_expireat_with_error(self):
        yield self.client.connect()
        with self.assertRaises(exceptions.RedisError):
            yield self.client.expireat(str(uuid.uuid4()).encode('ascii'),
                                       'abc')

    @testing.gen_test
    def test_exists_single(self):
        yield self.client.connect()
        key = str(uuid.uuid4()).encode('ascii')
        value = str(uuid.uuid4()).encode('ascii')
        result = yield self.client.set(key, value)
        self.assertTrue(result)
        result = yield self.client.exists(key)
        self.assertTrue(result)

    @testing.gen_test
    def test_exists_none(self):
        yield self.client.connect()
        key = str(uuid.uuid4()).encode('ascii')
        result = yield self.client.exists(key)
        self.assertFalse(result)

    @testing.gen_test
    def test_exists_error(self):
        yield self.client.connect()
        self._execute_result = exceptions.RedisError('Test Exception')
        with mock.patch.object(self.client, 'execute', self.execute):
            with self.assertRaises(exceptions.RedisError):
                result = yield self.client.exists('foo')
                self.assertFalse(result)

    @testing.gen_test
    def test_keys(self):
        yield self.client.connect()
        prefix = 'keys-test'
        keys = ['{}-{}'.format(prefix, str(uuid.uuid4())).encode('ascii')
                for i in range(0, 10)]
        for key in keys:
            yield self.client.set(key, str(uuid.uuid4()), 10)
        result = yield self.client.keys('{}*'.format(prefix))
        self.assertListEqual(sorted(result), sorted(keys))
