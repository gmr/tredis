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
    def test_object_encoding(self):
        yield self.client.connect()
        key, value1, value2 = self.uuid4(3)
        result = yield self.client.sadd(key, value1, value2)
        self.assertTrue(result)
        result = yield self.client.object_encoding(key)
        self.assertEqual(result, b'hashtable')
        result = yield self.client.delete(key)
        self.assertTrue(result)

    @testing.gen_test
    def test_object_idle_time(self):
        yield self.client.connect()
        key, value1, value2 = self.uuid4(3)
        result = yield self.client.sadd(key, value1, value2)
        self.assertTrue(result)
        result = yield self.client.object_idle_time(key)
        self.assertEqual(result, 0)
        result = yield self.client.delete(key)
        self.assertTrue(result)

    @testing.gen_test
    def test_object_refcount(self):
        yield self.client.connect()
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
    def test_sort_invalid_order(self):
        yield self.client.connect()
        key = self.uuid4()
        with self.assertRaises(ValueError):
            yield self.client.sort(key, alpha=True, order='DOWN')

    @testing.gen_test
    def test_sort_numeric(self):
        yield self.client.connect()
        key = self.uuid4()
        result = yield self.client.sadd(key, 100, 300, 200)
        self.assertTrue(result)
        result = yield self.client.sort(key)
        self.assertListEqual(result, [b'100', b'200', b'300'])
        result = yield self.client.delete(key)
        self.assertTrue(result)

    @testing.gen_test
    def test_sort_alpha_asc(self):
        yield self.client.connect()
        key, value1, value2, value3 = self.uuid4(4)
        result = yield self.client.sadd(key, value1, value2, value3)
        self.assertTrue(result)
        result = yield self.client.sort(key, alpha=True)
        self.assertListEqual(result, sorted([value1, value2, value3]))
        result = yield self.client.delete(key)
        self.assertTrue(result)

    @testing.gen_test
    def test_sort_alpha_desc(self):
        yield self.client.connect()
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
        yield self.client.connect()
        key, value1, value2, value3 = self.uuid4(4)
        result = yield self.client.sadd(key, value1, value2, value3)
        self.assertTrue(result)
        result = yield self.client.sort(key, limit=2, offset=1, alpha=True)
        self.assertListEqual(result, sorted([value1, value2, value3])[1:])
        result = yield self.client.delete(key)
        self.assertTrue(result)

    @testing.gen_test
    def test_sort_alpha_asc_and_store(self):
        yield self.client.connect()
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
        yield self.client.connect()
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
        yield self.client.connect()
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

        result = yield self.client.sort(key, by='weight2_*', external='obj2_*')
        self.assertListEqual(result, expectation)
        result = yield self.client.delete(key)
        self.assertTrue(result)

    @testing.gen_test
    def test_sort_by_with_externals(self):
        yield self.client.connect()
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

    @testing.gen_test
    def test_wait(self):
        yield self.client.connect()
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
        yield self.client.connect()
        key, value = self.uuid4(2)
        result = yield self.expiring_set(key, value)
        self.assertTrue(result)
        result = yield self.client.migrate(self.redis2_host, 6379, key, 10,
                                           5000)
        self.assertTrue(result)
        client = tredis.RedisClient(self.redis_host, self.redis2_port, 10)
        yield client.connect()
        result = yield client.get(key)
        self.assertEqual(result, value)
        result = yield self.client.get(key)
        self.assertIsNone(result)

    @testing.gen_test
    def test_migrate_copy(self):
        yield self.client.connect()
        key, value = self.uuid4(2)
        result = yield self.expiring_set(key, value)
        self.assertTrue(result)
        result = yield self.client.migrate(self.redis2_host, 6379, key, 10,
                                           5000, copy=True)
        self.assertTrue(result)
        client = tredis.RedisClient(self.redis_host, self.redis2_port, 10)
        yield client.connect()
        result = yield client.get(key)
        self.assertEqual(result, value)
        result = yield self.client.get(key)
        self.assertEqual(result, value)

    @testing.gen_test
    def test_migrate_exists(self):
        yield self.client.connect()
        key, value = self.uuid4(2)
        result = yield self.expiring_set(key, value)
        self.assertTrue(result)
        client = tredis.RedisClient(self.redis_host, self.redis2_port, 10)
        yield client.connect()
        result = yield client.set(key, value, 10)
        self.assertTrue(result)
        with self.assertRaises(exceptions.RedisError):
            yield self.client.migrate(self.redis2_host, 6379, key, 10, 5000)

    @testing.gen_test
    def test_migrate_replace(self):
        yield self.client.connect()
        key, value = self.uuid4(2)
        result = yield self.expiring_set(key, value)
        self.assertTrue(result)
        client = tredis.RedisClient(self.redis_host, self.redis2_port, 10)
        yield client.connect()
        result = yield client.set(key, value, 10)
        self.assertTrue(result)
        result = yield self.client.migrate(self.redis2_host, 6379,
                                           key, 10, 5000, replace=True)
        self.assertTrue(result)
        result = yield client.get(key)
        self.assertEqual(result, value)
        result = yield self.client.get(key)
        self.assertIsNone(result)


class PipelineTests(base.AsyncTestCase):

    def setUp(self):
        super(PipelineTests, self).setUp()
        self.redis_host = os.getenv('REDIS_HOST', 'localhost')
        self.redis2_host = os.getenv('REDIS2_HOST', 'localhost')
        self.redis2_port = int(os.getenv('REDIS2_PORT', '6379'))

    @testing.gen_test
    def test_command_pipeline(self):
        yield self.client.connect()
        yield self.client.select(9)
        self.client.pipeline_start()

        expectation = []

        key1, value = self.uuid4(2)
        self.client.set(key1, value, 10)
        expectation.append(True)  # 0
        self.client.exists(key1)
        expectation.append(True)  # 1
        self.client.delete(key1)
        expectation.append(True)  # 2
        self.client.exists(key1)
        expectation.append(False)  # 3
        self.client.set(key1, value, 10)
        expectation.append(True)  # 4
        self.client.keys('*')
        expectation.append([key1])  # 5
        self.client.type(key1)
        expectation.append(b'string')  # 6

        key2, value1, value2 = self.uuid4(3)

        self.client.sadd(key2, value1, value2)
        expectation.append(True)  # 7
        self.client.object_encoding(key2)
        expectation.append(b'hashtable')  # 8
        self.client.object_idle_time(key2)
        expectation.append(0)  # 9
        self.client.delete(key2)
        expectation.append(True)  # 10

        for value in self.uuid4(3):
            self.client._pipeline_add([b'ZADD', key2, b'1', value])
            expectation.append(True)  # 11-13

        self.client.object_refcount(key2)
        expectation.append(1)  # 14

        self.client.delete(key2)
        expectation.append(True)  # 15

        key3, new1 = self.uuid4(2)
        self.client.set(key3, value, 10)
        expectation.append(True)  # 16
        self.client.rename(key3, new1)
        expectation.append(True)  # 17
        self.client.set(key3, value, 10)
        expectation.append(True)  # 18
        self.client.renamenx(key3, new1)
        expectation.append(False)  # 19

        self.client.scan(0, '*')
        expectation.append((0, sorted([key1, key3, new1])))  # 20

        key4 = self.uuid4()
        self.client.sadd(key4, 100, 300, 200)
        expectation.append(True)  # 21
        self.client.sort(key4)
        expectation.append([b'100', b'200', b'300'])  # 22
        self.client.delete(key4)
        expectation.append(True)  # 23

        key5 = self.uuid4()
        self.client.set(key5, value, 10)
        expectation.append(True)  # 24
        self.client.wait(0, 500)
        expectation.append(0)  # 25

        self.client.expire(key5, 'abc')
        expectation.append(exceptions.RedisError)  # 26

        result = yield self.client.pipeline_execute()
        for index, value in enumerate(result):
            if isinstance(value, list):
                result[index] = sorted(value)
            elif isinstance(value, tuple) and isinstance(value[1],list):
                result[index] = value[0], sorted(value[1])
            elif isinstance(value, exceptions.RedisError):
                result[index] = exceptions.RedisError

        self.assertListEqual(result, expectation)

    @testing.gen_test
    def test_dump_and_restore(self):
        yield self.client.connect()
        self.client.pipeline_start()
        key, value = self.uuid4(2)
        self.client.set(key, value, 10)
        self.client.dump(key)
        self.client.delete(key)
        result = yield self.client.pipeline_execute()
        self.assertTrue(result[0])
        self.assertIn(value, result[1])
        self.assertTrue(result[2])

        self.client.pipeline_start()
        self.client.restore(key, 10, result[1])
        self.client.get(key)
        result = yield self.client.pipeline_execute()
        self.assertTrue(result[0])
        self.assertEqual(result[1], value)

    @testing.gen_test
    def test_expire_and_ttl(self):
        yield self.client.connect()
        self.client.pipeline_start()
        key, value = self.uuid4(2)
        ttl = 5
        self.client.set(key, value, 10)
        self.client.expire(key, ttl)
        self.client.ttl(key)
        result = yield self.client.pipeline_execute()
        self.assertTrue([result[0], result[1]])
        self.assertAlmostEqual(result[2], ttl)

    @testing.gen_test
    def test_pttl(self):
        yield self.client.connect()
        self.client.pipeline_start()
        key, value = self.uuid4(2)
        self.client.set(key, value, 10)
        self.client.pexpire(key, 5000)
        self.client.pttl(key)
        result = yield self.client.pipeline_execute()
        self.assertTrue(result[0] and result[1])
        self.assertGreater(result[2], 1000)
        self.assertLessEqual(result[2], 5000)

    @testing.gen_test
    def test_migrate(self):
        yield self.client.connect()
        self.client.pipeline_start()
        key, value = self.uuid4(2)
        self.client.set(key, value, 10)
        self.client.migrate(self.redis2_host, 6379, key, 10, 5000)
        result = yield self.client.pipeline_execute()
        self.assertListEqual(result, [True, True])

        client = tredis.RedisClient(self.redis_host, self.redis2_port, 10)
        yield client.connect()
        result = yield client.get(key)
        self.assertEqual(result, value)

        result = yield self.client.get(key)
        self.assertIsNone(result)

    @testing.gen_test
    def test_randomkey(self):
        yield self.client.connect()
        yield self.client.select(8)
        self.client.pipeline_start()
        keys = self.uuid4(10)
        for key in list(keys):
            self.client.set(key, str(uuid.uuid4()), 10)
        self.client.randomkey()
        result = yield self.client.pipeline_execute()
        self.assertTrue(all(result[0:10]))
        self.assertIn(result[-1], keys)
