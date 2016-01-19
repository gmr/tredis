import mock

from tornado import testing

from tredis import exceptions

from . import base


class SetTests(base.AsyncTestCase):

    @testing.gen_test
    def test_sadd_single(self):
        key, value = self.uuid4(2)
        result = yield self.client.sadd(key, value)
        self.assertEqual(result, 1)

    @testing.gen_test
    def test_sadd_multiple(self):
        key, value1, value2, value3 = self.uuid4(4)
        result = yield self.client.sadd(key, value1, value2, value3)
        self.assertTrue(result)

    @testing.gen_test
    def test_sadd_multiple_dupe(self):
        key, value1, value2, value3 = self.uuid4(4)
        result = yield self.client.sadd(key, value1, value2, value3, value3)
        self.assertEqual(result, 3)

    @testing.gen_test
    def test_sadd_with_error(self):
        key, value = self.uuid4(2)
        self._execute_result = exceptions.RedisError('Test Exception')
        with mock.patch.object(self.client, '_execute', self._execute):
            with self.assertRaises(exceptions.RedisError):
                yield self.client.sadd(key, value)

    @testing.gen_test
    def test_sdiff(self):
        key1, key2, value1, value2, value3 = self.uuid4(5)
        result = yield self.client.sadd(key1, value1, value2)
        self.assertTrue(result)
        result = yield self.client.sadd(key2, value1, value3)
        self.assertTrue(result)
        result = yield self.client.sdiff(key1, key2)
        self.assertListEqual(result, [value2])

    @testing.gen_test
    def test_sdiffstore(self):
        key1, key2, key3, value1, value2, value3 = self.uuid4(6)
        result = yield self.client.sadd(key1, value1, value2)
        self.assertTrue(result)
        result = yield self.client.sadd(key2, value1, value3)
        self.assertTrue(result)
        result = yield self.client.sdiffstore(key3, key1, key2)
        self.assertEqual(result, 1)
        result = yield self.client.sismember(key3, value2)
        self.assertTrue(result)

    @testing.gen_test
    def test_sinter(self):
        key1, key2, value1, value2, value3 = self.uuid4(5)
        result = yield self.client.sadd(key1, value1, value2)
        self.assertTrue(result)
        result = yield self.client.sadd(key2, value2, value3)
        self.assertTrue(result)
        result = yield self.client.sinter(key1, key2)
        self.assertListEqual(result, [value2])

    @testing.gen_test
    def test_sinterstore(self):
        key1, key2, key3, value1, value2, value3 = self.uuid4(6)
        result = yield self.client.sadd(key1, value1, value2)
        self.assertTrue(result)
        result = yield self.client.sadd(key2, value2, value3)
        self.assertTrue(result)
        result = yield self.client.sinterstore(key3, key1, key2)
        self.assertEqual(result, 1)
        result = yield self.client.sismember(key3, value2)
        self.assertTrue(result)

    @testing.gen_test
    def test_sadd_sismember_true(self):
        key, value = self.uuid4(2)
        result = yield self.client.sadd(key, value)
        self.assertTrue(result)
        result = yield self.client.sismember(key, value)
        self.assertTrue(result)

    @testing.gen_test
    def test_sadd_sismember_false(self):
        key, value1, value2 = self.uuid4(3)
        result = yield self.client.sadd(key, value1)
        self.assertTrue(result)
        result = yield self.client.sismember(key, value2)
        self.assertFalse(result)

    @testing.gen_test
    def test_scard(self):
        key, value1, value2, value3 = self.uuid4(4)
        result = yield self.client.sadd(key, value1, value2, value3)
        self.assertTrue(result)
        result = yield self.client.scard(key)
        self.assertEqual(result, 3)

    @testing.gen_test
    def test_smembers(self):
        key, value1, value2, value3 = self.uuid4(4)
        result = yield self.client.sadd(key, value1, value2, value3)
        self.assertTrue(result)
        result = yield self.client.smembers(key)
        self.assertListEqual(sorted(result), sorted([value1, value2, value3]))

    @testing.gen_test
    def test_smove(self):
        key1, key2, value1 = self.uuid4(3)
        result = yield self.client.sadd(key1, value1)
        self.assertTrue(result)
        result = yield self.client.smove(key1, key2, value1)
        self.assertTrue(result)
        result = yield self.client.sismember(key1, value1)
        self.assertFalse(result)
        result = yield self.client.sismember(key2, value1)
        self.assertTrue(result)

    @testing.gen_test
    def test_spop(self):
        key, value1, value2, value3 = self.uuid4(4)
        values = [value1, value2, value3]
        result = yield self.client.sadd(key, *values)
        self.assertTrue(result)
        member = yield self.client.spop(key)
        self.assertIn(member, values)
        members = yield self.client.smembers(key)
        self.assertNotIn(member, members)

    @testing.gen_test
    def test_srandmember(self):
        key, value1, value2, value3 = self.uuid4(4)
        values = [value1, value2, value3]
        result = yield self.client.sadd(key, *values)
        self.assertTrue(result)
        member = yield self.client.srandmember(key)
        self.assertIn(member, values)
        members = yield self.client.smembers(key)
        self.assertIn(member, members)

    @testing.gen_test
    def test_srandmember_multi(self):
        key, value1, value2, value3 = self.uuid4(4)
        values = [value1, value2, value3]
        result = yield self.client.sadd(key, *values)
        self.assertTrue(result)
        members = yield self.client.srandmember(key, 2)
        for member in members:
            self.assertIn(member, values)
        self.assertEqual(len(members), 2)

    @testing.gen_test
    def test_srem(self):
        key, value1, value2, value3 = self.uuid4(4)
        values = [value1, value2, value3]
        result = yield self.client.sadd(key, *values)
        self.assertTrue(result)
        result = yield self.client.srem(key, value2, value3)
        self.assertTrue(result)
        members = yield self.client.smembers(key)
        self.assertNotIn(value2, members)
        self.assertNotIn(value3, members)

    @testing.gen_test
    def test_srem_dupe(self):
        key = self.uuid4()
        key, value1, value2, value3 = self.uuid4(4)
        values = [value1, value2, value3]
        result = yield self.client.sadd(key, *values)
        self.assertTrue(result)
        result = yield self.client.srem(key, value2, value3, value3)
        self.assertEqual(result, 2)
        members = yield self.client.smembers(key)
        self.assertNotIn(value2, members)
        self.assertNotIn(value3, members)

    @testing.gen_test
    def test_srem_with_error(self):
        key, value = self.uuid4(2)
        self._execute_result = exceptions.RedisError('Test Exception')
        with mock.patch.object(self.client, '_execute', self._execute):
            with self.assertRaises(exceptions.RedisError):
                yield self.client.srem(key, value)

    @testing.gen_test
    def test_sscan(self):
        key, value1, value2, value3 = self.uuid4(4)
        values = [value1, value2, value3]
        result = yield self.client.sadd(key, *values)
        self.assertTrue(result)
        cursor, result = yield self.client.sscan(key, 0)
        self.assertListEqual(sorted(result), sorted(values))
        self.assertEqual(cursor, 0)

    @testing.gen_test
    def test_sscan_with_pattern(self):
        key, value1, value2, value3 = self.uuid4(4)
        values = [value1, value2, value3]
        result = yield self.client.sadd(key, *values)
        self.assertTrue(result)
        cursor, result = yield self.client.sscan(key, 0, '*')
        self.assertListEqual(sorted(result), sorted(values))
        self.assertEqual(cursor, 0)

    @testing.gen_test
    def test_sscan_with_pattern_and_count(self):
        key, value1, value2, value3 = self.uuid4(4)
        values = [value1, value2, value3]
        result = yield self.client.sadd(key, *values)
        self.assertTrue(result)
        cursor, result = yield self.client.sscan(key, 0, '*', 10)
        self.assertListEqual(sorted(result), sorted(values))
        self.assertEqual(cursor, 0)

    @testing.gen_test
    def test_sscan_with_error(self):
        key = self.uuid4()
        self._execute_result = exceptions.RedisError('Test Exception')
        with mock.patch.object(self.client, '_execute', self._execute):
            with self.assertRaises(exceptions.RedisError):
                yield self.client.sscan(key, 0)

    @testing.gen_test
    def test_sunion(self):
        key1, key2, key3, value1, value2, value3 = self.uuid4(6)
        result = yield self.client.sadd(key1, value1, value2)
        self.assertTrue(result)
        result = yield self.client.sadd(key2, value2, value3)
        self.assertTrue(result)
        result = yield self.client.sunion(key1, key2)
        self.assertListEqual(sorted(result), sorted([value1, value2, value3]))

    @testing.gen_test
    def test_suinionstore(self):
        key1, key2, key3, value1, value2, value3 = self.uuid4(6)
        result = yield self.client.sadd(key1, value1, value2)
        self.assertTrue(result)
        result = yield self.client.sadd(key2, value2, value3)
        self.assertTrue(result)
        result = yield self.client.sunionstore(key3, key1, key2)
        self.assertEqual(result, 3)
        result = yield self.client.sismember(key3, value1)
        self.assertTrue(result)
        result = yield self.client.sismember(key3, value2)
        self.assertTrue(result)
        result = yield self.client.sismember(key3, value3)
        self.assertTrue(result)
