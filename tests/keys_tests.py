import mock
import os
import time
import uuid

from tornado import testing

import tredis
from tredis import exceptions

from . import base


class KeyCommandTests(base.AsyncTestCase):

    @testing.gen_test
    def test_delete(self):
        key, value = self.uuid4(2)
        result = yield self.expiring_set(key, value)
        self.assertTrue(result)
        result = yield self.client.delete(key)
        self.assertTrue(result)

    @testing.gen_test
    def test_delete_multi(self):
        key1, key2, value = self.uuid4(3)
        result = yield self.expiring_set(key1, value)
        self.assertTrue(result)
        result = yield self.expiring_set(key2, value)
        self.assertTrue(result)
        result = yield self.client.delete(key1, key2)
        self.assertTrue(result)

    @testing.gen_test
    def test_delete_missing_key(self):
        key1, key2, value = self.uuid4(3)
        result = yield self.expiring_set(key1, value)
        self.assertTrue(result)
        result = yield self.expiring_set(key2, value)
        self.assertTrue(result)
        result = yield self.client.delete(key1, key2,
                                          self.uuid4())
        self.assertEqual(result, 2)

    @testing.gen_test
    def test_dump_and_restore(self):
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
        key = self.uuid4()
        result = yield self.client.dump(key)
        self.assertIsNone(result)

    @testing.gen_test
    def test_expire_and_ttl(self):
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
        with self.assertRaises(exceptions.RedisError):
            yield self.client.expire(self.uuid4(), 'abc')

    @testing.gen_test
    def test_expireat_and_ttl(self):
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
        with self.assertRaises(exceptions.RedisError):
            yield self.client.expireat(self.uuid4(), 'abc')

    @testing.gen_test
    def test_exists_single(self):
        key, value = self.uuid4(2)
        result = yield self.expiring_set(key, value)
        self.assertTrue(result)
        result = yield self.client.exists(key)
        self.assertTrue(result)

    @testing.gen_test
    def test_exists_none(self):
        key = self.uuid4()
        result = yield self.client.exists(key)
        self.assertFalse(result)

    @testing.gen_test
    def test_keys(self):
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
    def test_object_encoding(self):
        key, value1, value2 = self.uuid4(3)
        result = yield self.client.sadd(key, value1, value2)
        self.assertTrue(result)
        result = yield self.client.object_encoding(key)
        self.assertEqual(result, b'hashtable')
        result = yield self.client.delete(key)
        self.assertTrue(result)

    @testing.gen_test
    def test_object_idle_time(self):
        key, value1, value2 = self.uuid4(3)
        result = yield self.client.sadd(key, value1, value2)
        self.assertTrue(result)
        result = yield self.client.object_idle_time(key)
        self.assertEqual(result, 0)
        result = yield self.client.delete(key)
        self.assertTrue(result)

    @testing.gen_test
    def test_object_refcount(self):
        key = self.uuid4()
        for value in self.uuid4(3):
            result = yield self.client._execute([b'ZADD', key, b'1', value])
            self.assertTrue(result)
        result = yield self.client.object_refcount(key)
        self.assertEqual(result, 1)
        result = yield self.client.delete(key)
        self.assertTrue(result)

    @testing.gen_test
    def test_persist(self):
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
        key, value = self.uuid4(2)
        result = yield self.expiring_set(key, value)
        self.assertTrue(result)
        result = yield self.client.pexpire(key, 5000)
        self.assertTrue(result)
        result = yield self.client.ttl(key)
        self.assertAlmostEqual(result, 5)

    @testing.gen_test
    def test_pexpire_with_error(self):
        with self.assertRaises(exceptions.RedisError):
            yield self.client.pexpire(self.uuid4(), 'abc')

    @testing.gen_test
    def test_pexpireat_and_ttl(self):
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
        with self.assertRaises(exceptions.RedisError):
            yield self.client.pexpireat(self.uuid4(), 'abc')

    @testing.gen_test
    def test_pttl(self):
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
        yield self.client.select(4)
        keys = self.uuid4(10)
        for key in list(keys):
            yield self.expiring_set(key, str(uuid.uuid4()))
        result = yield self.client.randomkey()
        self.assertIn(result, keys)

    @testing.gen_test
    def test_rename(self):
        key1, key2, value = self.uuid4(3)
        result = yield self.expiring_set(key1, value)
        self.assertTrue(result)
        result = yield self.client.rename(key1, key2)
        self.assertTrue(result)
        result = yield self.client.get(key2)
        self.assertEqual(result, value)

    @testing.gen_test
    def test_renamenx(self):
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
        key1, key2, value = self.uuid4(3)
        result = yield self.expiring_set(key1, value)
        self.assertTrue(result)
        result = yield self.expiring_set(key2, value)
        self.assertTrue(result)
        result = yield self.client.renamenx(key1, key2)
        self.assertFalse(result)

    @testing.gen_test
    def test_scan(self):
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
        key = self.uuid4()
        self._execute_result = exceptions.RedisError('Test Exception')
        with mock.patch.object(self.client, '_execute', self._execute):
            with self.assertRaises(exceptions.RedisError):
                yield self.client.scan(key, 0)

    @testing.gen_test
    def test_sort_invalid_order(self):
        key = self.uuid4()
        with self.assertRaises(ValueError):
            yield self.client.sort(key, alpha=True, order='DOWN')

    @testing.gen_test
    def test_sort_numeric(self):
        key = self.uuid4()
        result = yield self.client.sadd(key, 100, 300, 200)
        self.assertTrue(result)
        result = yield self.client.sort(key)
        self.assertListEqual(result, [b'100', b'200', b'300'])
        result = yield self.client.delete(key)
        self.assertTrue(result)

    @testing.gen_test
    def test_sort_alpha_asc(self):
        key, value1, value2, value3 = self.uuid4(4)
        result = yield self.client.sadd(key, value1, value2, value3)
        self.assertTrue(result)
        result = yield self.client.sort(key, alpha=True)
        self.assertListEqual(result, sorted([value1, value2, value3]))
        result = yield self.client.delete(key)
        self.assertTrue(result)

    @testing.gen_test
    def test_sort_alpha_desc(self):
        key, value1, value2, value3 = self.uuid4(4)
        result = yield self.client.sadd(key, value1, value2, value3)
        self.assertTrue(result)
        result = yield self.client.sort(key, alpha=True, order='DESC')
        self.assertListEqual(result, sorted([value1, value2, value3],
                                            reverse=True))
        result = yield self.client.delete(key)
        self.assertTrue(result)

    @testing.gen_test
    def test_sort_alpha_limit_offset(self):
        key, value1, value2, value3 = self.uuid4(4)
        result = yield self.client.sadd(key, value1, value2, value3)
        self.assertTrue(result)
        result = yield self.client.sort(key, limit=2, offset=1, alpha=True)
        self.assertListEqual(result, sorted([value1, value2, value3])[1:])
        result = yield self.client.delete(key)
        self.assertTrue(result)

    @testing.gen_test
    def test_sort_alpha_asc_and_store(self):
        key1, key2, value1, value2, value3 = self.uuid4(5)
        result = yield self.client.sadd(key1, value1, value2, value3)
        self.assertTrue(result)
        result = yield self.client.sort(key1, alpha=True, store_as=key2)
        self.assertEqual(result, 3)
        result = yield self.client.type(key2)
        result = yield self.client._execute([b'LRANGE', key2, 0, 3])
        self.assertListEqual(result, sorted([value1, value2, value3]))
        result = yield self.client.delete(key1)
        self.assertTrue(result)
        result = yield self.client.delete(key2)
        self.assertTrue(result)

    @testing.gen_test
    def test_sort_by(self):
        key, value1, value2, value3 = self.uuid4(4)
        values = [value1, value2, value3]
        result = yield self.client.sadd(key, value1, value2, value3)
        self.assertTrue(result)
        expectation = []
        for index, value in enumerate(values):
            weight_key = 'weight1_{}'.format(value.decode('utf-8'))
            result = yield self.expiring_set(weight_key, index)
            self.assertTrue(result)
            expectation.append(value)

        result = yield self.client.sort(key, by='weight1_*')
        self.assertListEqual(result, expectation)
        result = yield self.client.delete(key)
        self.assertTrue(result)

    @testing.gen_test
    def test_sort_by_with_external(self):
        key, value1, value2, value3 = self.uuid4(4)
        values = [value1, value2, value3]
        result = yield self.client.sadd(key, value1, value2, value3)
        self.assertTrue(result)
        expectation = []
        for index, value in enumerate(values):
            weight_key = 'weight2_{}'.format(value.decode('utf-8'))
            result = yield self.expiring_set(weight_key, index)
            self.assertTrue(result)

            ext_key = 'obj2_{}'.format(value.decode('utf-8'))
            ext_val = 'value: {}'.format(index).encode('utf-8')
            result = yield self.expiring_set(ext_key, ext_val)
            self.assertTrue(result)
            expectation.append(ext_val)

        result = yield self.client.sort(key, by='weight2_*',
                                        external='obj2_*')
        self.assertListEqual(result, expectation)
        result = yield self.client.delete(key)
        self.assertTrue(result)

    @testing.gen_test
    def test_sort_by_with_externals(self):
        key, value1, value2, value3 = self.uuid4(4)
        values = [value1, value2, value3]
        result = yield self.client.sadd(key, value1, value2, value3)
        self.assertTrue(result)
        expectation = []
        for index, value in enumerate(values):
            weight_key = 'weight2_{}'.format(value.decode('utf-8'))
            result = yield self.expiring_set(weight_key, index)
            self.assertTrue(result)

            ext_key = 'obj2a_{}'.format(value.decode('utf-8'))
            ext_val = 'value1: {}'.format(index).encode('utf-8')
            result = yield self.expiring_set(ext_key, ext_val)
            self.assertTrue(result)
            expectation.append(ext_val)

            ext_key = 'obj2b_{}'.format(value.decode('utf-8'))
            ext_val = 'value2: {}'.format(index).encode('utf-8')
            result = yield self.expiring_set(ext_key, ext_val)
            self.assertTrue(result)
            expectation.append(ext_val)

        result = yield self.client.sort(key, by='weight2_*',
                                        external=['obj2a_*', 'obj2b_*'])
        self.assertListEqual(result, expectation)
        result = yield self.client.delete(key)
        self.assertTrue(result)

    @testing.gen_test
    def test_type_string(self):
        key, value = self.uuid4(2)
        result = yield self.expiring_set(key, value)
        self.assertTrue(result)
        result = yield self.client.type(key)
        self.assertEqual(result, b'string')

    @testing.gen_test
    def test_type_set(self):
        key, value = self.uuid4(2)
        result = yield self.client.sadd(key, value)
        self.assertTrue(result)
        result = yield self.client.type(key)
        self.assertEqual(result, b'set')
        result = yield self.client.delete(key)
        self.assertTrue(result)

    @testing.gen_test
    def test_wait(self):
        key, value = self.uuid4(2)
        result = yield self.expiring_set(key, value)
        self.assertTrue(result)
        result = yield self.client.wait(0, 500)
        self.assertEqual(result, 0)


class MigrationTests(base.AsyncTestCase):

    def setUp(self):
        super(MigrationTests, self).setUp()
        self.redis_host = os.getenv('REDIS_HOST', 'localhost')
        self.redis2_host = os.getenv('REDIS2_HOST', 'localhost')
        self.redis2_port = int(os.getenv('REDIS2_PORT', '6379'))

    @testing.gen_test
    def test_migrate(self):
        key, value = self.uuid4(2)
        result = yield self.expiring_set(key, value)
        self.assertTrue(result)
        result = yield self.client.migrate(self.redis2_host, 6379, key, 10,
                                           5000)
        self.assertTrue(result)
        client = tredis.RedisClient(self.redis_host, self.redis2_port, 10)
        response = yield client.get(key)
        self.assertEqual(response, value)
        result = yield self.client.get(key)
        self.assertIsNone(result)

    @testing.gen_test
    def test_migrate_copy(self):
        key, value = self.uuid4(2)
        result = yield self.expiring_set(key, value)
        self.assertTrue(result)
        result = yield self.client.migrate(self.redis2_host, 6379, key, 10,
                                           5000, copy=True)
        self.assertTrue(result)
        client = tredis.RedisClient(self.redis_host, self.redis2_port, 10)
        result = yield client.get(key)
        self.assertEqual(result, value)
        result = yield self.client.get(key)
        self.assertEqual(result, value)

    @testing.gen_test
    def test_migrate_exists(self):
        key, value = self.uuid4(2)
        result = yield self.expiring_set(key, value)
        self.assertTrue(result)
        client = tredis.RedisClient(self.redis_host, self.redis2_port, 10)
        result = yield client.set(key, value, 10)
        self.assertTrue(result)
        with self.assertRaises(exceptions.RedisError):
            yield self.client.migrate(self.redis2_host, 6379, key, 10, 5000)

    @testing.gen_test
    def test_migrate_replace(self):
        key, value = self.uuid4(2)
        result = yield self.expiring_set(key, value)
        self.assertTrue(result)
        client = tredis.RedisClient(self.redis_host, self.redis2_port, 10)
        result = yield client.set(key, value, 10)
        self.assertTrue(result)
        result = yield self.client.migrate(self.redis2_host, 6379,
                                           key, 10, 5000, replace=True)
        self.assertTrue(result)
        result = yield client.get(key)
        self.assertEqual(result, value)
        result = yield self.client.get(key)
        self.assertIsNone(result)
