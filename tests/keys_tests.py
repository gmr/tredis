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
        key, value = self.uuid4(2)
        result = yield self.expiring_set(key, value)
        self.assertTrue(result)
        result = yield self.client.delete(key)
        self.assertTrue(result)

    @testing.gen_test
    def test_delete_multi(self):
        yield self.client.connect()
        key1, key2, value = self.uuid4(3)
        result = yield self.expiring_set(key1, value)
        self.assertTrue(result)
        result = yield self.expiring_set(key2, value)
        self.assertTrue(result)
        result = yield self.client.delete(key1, key2)
        self.assertTrue(result)

    @testing.gen_test
    def test_delete_missing_key(self):
        yield self.client.connect()
        key1, key2, value = self.uuid4(3)
        result = yield self.expiring_set(key1, value)
        self.assertTrue(result)
        result = yield self.expiring_set(key2, value)
        self.assertTrue(result)
        result = yield self.client.delete(key1, key2,
                                          self.uuid4())
        self.assertEqual(result, 2)

    @testing.gen_test
    def test_delete_with_error(self):
        yield self.client.connect()
        key = self.uuid4()
        self._execute_result = exceptions.RedisError('Test Exception')
        with mock.patch.object(self.client, '_execute', self._execute):
            with self.assertRaises(exceptions.RedisError):
                yield self.client.delete(key)

    @testing.gen_test
    def test_dump_and_restore(self):
        yield self.client.connect()
        key, value = self.uuid4(2)
        result = yield self.expiring_set(key, value)
        self.assertTrue(result)
        dump_value = yield self.client.dump(key)
        self.assertIn(value, dump_value)
        result = yield self.client.delete(key)
        self.assertTrue(result)
        result = yield self.client.restore(key, 10, dump_value)
        self.assertTrue(result)
        result = yield self.client.get(key)
        self.assertEqual(result, value)

    @testing.gen_test
    def test_dump_and_restore_with_replace(self):
        yield self.client.connect()
        key, value1, value2 = self.uuid4(3)
        result = yield self.expiring_set(key, value1)
        self.assertTrue(result)
        dump_value = yield self.client.dump(key)
        self.assertIn(value1, dump_value)
        result = yield self.client.delete(key)
        self.assertTrue(result)
        result = yield self.expiring_set(key, value2)
        self.assertTrue(result)
        result = yield self.client.get(key)
        self.assertEqual(result, value2)
        result = yield self.client.restore(key, 10, dump_value, True)
        self.assertTrue(result)
        result = yield self.client.get(key)
        self.assertEqual(result, value1)

    @testing.gen_test
    def test_dump_and_restore_without_replace(self):
        yield self.client.connect()
        key, value1, value2 = self.uuid4(3)
        result = yield self.expiring_set(key, value1)
        self.assertTrue(result)
        dump_value = yield self.client.dump(key)
        self.assertIn(value1, dump_value)
        result = yield self.client.delete(key)
        self.assertTrue(result)
        result = yield self.expiring_set(key, value2)
        self.assertTrue(result)
        result = yield self.client.get(key)
        self.assertEqual(result, value2)
        with self.assertRaises(exceptions.RedisError):
            yield self.client.restore(key, 10, dump_value)

    @testing.gen_test
    def test_dump_with_invalid_key(self):
        yield self.client.connect()
        key = self.uuid4()
        result = yield self.client.dump(key)
        self.assertIsNone(result)

    @testing.gen_test
    def test_expire_and_ttl(self):
        yield self.client.connect()
        key, value = self.uuid4(2)
        ttl = 5
        result = yield self.expiring_set(key, value)
        self.assertTrue(result)
        result = yield self.client.expire(key, ttl)
        self.assertTrue(result)
        result = yield self.client.ttl(key)
        self.assertAlmostEqual(result, ttl)

    @testing.gen_test
    def test_expire_with_error(self):
        yield self.client.connect()
        with self.assertRaises(exceptions.RedisError):
            yield self.client.expire(self.uuid4(), 'abc')

    @testing.gen_test
    def test_expireat_and_ttl(self):
        yield self.client.connect()
        key, value = self.uuid4(2)
        result = yield self.expiring_set(key, value)
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
            yield self.client.expireat(self.uuid4(), 'abc')

    @testing.gen_test
    def test_exists_single(self):
        yield self.client.connect()
        key, value = self.uuid4(2)
        result = yield self.expiring_set(key, value)
        self.assertTrue(result)
        result = yield self.client.exists(key)
        self.assertTrue(result)

    @testing.gen_test
    def test_exists_none(self):
        yield self.client.connect()
        key = self.uuid4()
        result = yield self.client.exists(key)
        self.assertFalse(result)

    @testing.gen_test
    def test_exists_error(self):
        yield self.client.connect()
        self._execute_result = exceptions.RedisError('Test Exception')
        with mock.patch.object(self.client, '_execute', self._execute):
            with self.assertRaises(exceptions.RedisError):
                result = yield self.client.exists('foo')
                self.assertFalse(result)

    @testing.gen_test
    def test_keys(self):
        yield self.client.connect()
        yield self.client.select(3)
        prefix = 'keys-test'
        keys = ['{}-{}'.format(prefix, str(uuid.uuid4())).encode('ascii')
                for i in range(0, 10)]
        for key in keys:
            yield self.expiring_set(key, str(uuid.uuid4()))
        result = yield self.client.keys('{}*'.format(prefix))
        self.assertListEqual(sorted(result), sorted(keys))

    @testing.gen_test
    def test_move(self):
        yield self.client.connect()
        key, value = self.uuid4(2)
        response = yield self.client.select(2)
        self.assertTrue(response)
        response = yield self.expiring_set(key, value)
        self.assertTrue(response)
        response = yield self.client.move(key, 1)
        self.assertTrue(response)
        response = yield self.client.select(1)
        self.assertTrue(response)
        response = yield self.client.get(key)
        self.assertEqual(response, value)

    @testing.gen_test
    def test_expire_and_persist(self):
        yield self.client.connect()
        key, value = self.uuid4(2)
        result = yield self.expiring_set(key, value)
        self.assertTrue(result)
        result = yield self.client.expire(key, 10)
        self.assertTrue(result)
        result = yield self.client.persist(key)
        self.assertTrue(result)
        result = yield self.client.ttl(key)
        self.assertEqual(result, -1)

    @testing.gen_test
    def test_pexpire_and_ttl(self):
        yield self.client.connect()
        key, value = self.uuid4(2)
        result = yield self.expiring_set(key, value)
        self.assertTrue(result)
        result = yield self.client.pexpire(key, 5000)
        self.assertTrue(result)
        result = yield self.client.ttl(key)
        self.assertAlmostEqual(result, 5)

    @testing.gen_test
    def test_pexpire_with_error(self):
        yield self.client.connect()
        with self.assertRaises(exceptions.RedisError):
            yield self.client.pexpire(self.uuid4(), 'abc')

    @testing.gen_test
    def test_pexpireat_and_ttl(self):
        yield self.client.connect()
        key, value = self.uuid4(2)
        result = yield self.expiring_set(key, value)
        self.assertTrue(result)
        timestamp = (int(time.time()) + 5) * 1000
        result = yield self.client.pexpireat(key, timestamp)
        self.assertTrue(result)
        result = yield self.client.ttl(key)
        self.assertLessEqual(result, 5)
        self.assertGreater(result, 0)

    @testing.gen_test
    def test_pexpireat_with_error(self):
        yield self.client.connect()
        with self.assertRaises(exceptions.RedisError):
            yield self.client.pexpireat(self.uuid4(), 'abc')

    @testing.gen_test
    def test_pttl(self):
        yield self.client.connect()
        key, value = self.uuid4(2)
        result = yield self.expiring_set(key, value)
        self.assertTrue(result)
        result = yield self.client.pexpire(key, 5000)
        self.assertTrue(result)
        result = yield self.client.pttl(key)
        self.assertGreater(result, 1000)
        self.assertLessEqual(result, 5000)

    @testing.gen_test
    def test_randomkey(self):
        yield self.client.connect()
        yield self.client.select(4)
        keys = self.uuid4(10)
        for key in list(keys):
            yield self.expiring_set(key, str(uuid.uuid4()))
        result = yield self.client.randomkey()
        self.assertIn(result, keys)

    @testing.gen_test
    def test_rename(self):
        yield self.client.connect()
        key1, key2, value = self.uuid4(3)
        result = yield self.expiring_set(key1, value)
        self.assertTrue(result)
        result = yield self.client.rename(key1, key2)
        self.assertTrue(result)
        result = yield self.client.get(key2)
        self.assertEqual(result, value)

    @testing.gen_test
    def test_renamenx(self):
        yield self.client.connect()
        key1, key2, value = self.uuid4(3)
        result = yield self.expiring_set(key1, value)
        self.assertTrue(result)

        result = yield self.client.get(key2)
        self.assertIsNone(result)

        result = yield self.client.renamenx(key1, key2)
        self.assertTrue(result)
        result = yield self.client.get(key2)
        self.assertEqual(result, value)

    @testing.gen_test
    def test_renamenx_failure(self):
        yield self.client.connect()
        key1, key2, value = self.uuid4(3)
        result = yield self.expiring_set(key1, value)
        self.assertTrue(result)
        result = yield self.expiring_set(key2, value)
        self.assertTrue(result)
        result = yield self.client.renamenx(key1, key2)
        self.assertFalse(result)

    @testing.gen_test
    def test_scan(self):
        yield self.client.connect()
        yield self.client.select(5)
        key1, key2, key3, value = self.uuid4(4)
        keys = [key1, key2, key3]
        for key in keys:
            result = yield self.expiring_set(key, value)
            self.assertTrue(result)
        cursor, result = yield self.client.scan(0)
        self.assertListEqual(sorted(result), sorted(keys))
        self.assertEqual(cursor, 0)
        for key in keys:
            result = yield self.client.delete(key)
            self.assertTrue(result)

    @testing.gen_test
    def test_scan_with_pattern(self):
        yield self.client.connect()
        yield self.client.select(5)
        key1, key2, key3, value = self.uuid4(4)
        keys = [key1, key2, key3]
        for key in keys:
            result = yield self.expiring_set(key, value)
            self.assertTrue(result)
        cursor, result = yield self.client.scan(0, '*')
        self.assertListEqual(sorted(result), sorted(keys))
        self.assertEqual(cursor, 0)
        for key in keys:
            result = yield self.client.delete(key)
            self.assertTrue(result)

    @testing.gen_test
    def test_scan_with_pattern_and_count(self):
        yield self.client.connect()
        yield self.client.select(5)
        key1, key2, key3, value = self.uuid4(4)
        keys = [key1, key2, key3]
        for key in keys:
            result = yield self.expiring_set(key, value)
            self.assertTrue(result)
        cursor, result = yield self.client.scan(0, '*', 10)
        self.assertListEqual(sorted(result), sorted(keys))
        self.assertEqual(cursor, 0)
        for key in keys:
            result = yield self.client.delete(key)
            self.assertTrue(result)

    @testing.gen_test
    def test_scan_with_error(self):
        yield self.client.connect()
        key = self.uuid4()
        self._execute_result = exceptions.RedisError('Test Exception')
        with mock.patch.object(self.client, '_execute', self._execute):
            with self.assertRaises(exceptions.RedisError):
                yield self.client.scan(key, 0)

    @testing.gen_test
    def test_type_string(self):
        yield self.client.connect()
        key, value = self.uuid4(2)
        result = yield self.expiring_set(key, value)
        self.assertTrue(result)
        result = yield self.client.type(key)
        self.assertEqual(result, b'string')

    @testing.gen_test
    def test_type_set(self):
        yield self.client.connect()
        key, value = self.uuid4(2)
        result = yield self.client.sadd(key, value)
        self.assertTrue(result)
        result = yield self.client.type(key)
        self.assertEqual(result, b'set')
        result = yield self.client.delete(key)
        self.assertTrue(result)
