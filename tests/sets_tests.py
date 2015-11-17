import mock

from tornado import testing

from tredis import exceptions

from . import base


class SetTests(base.AsyncTestCase):

    @testing.gen_test
    def test_sadd_single(self):
        yield self.client.connect()
        key = self.uuid4()
        value = self.uuid4()
        result = yield self.client.sadd(key, value)
        self.assertEqual(result, 1)

    @testing.gen_test
    def test_sadd_multiple(self):
        yield self.client.connect()
        key = self.uuid4()
        value1 = self.uuid4()
        value2 = self.uuid4()
        value3 = self.uuid4()
        result = yield self.client.sadd(key, value1, value2, value3)
        self.assertTrue(result)

    @testing.gen_test
    def test_sadd_multiple_dupe(self):
        yield self.client.connect()
        key = self.uuid4()
        value1 = self.uuid4()
        value2 = self.uuid4()
        value3 = self.uuid4()
        result = yield self.client.sadd(key, value1, value2, value3, value3)
        self.assertEqual(result, 3)

    @testing.gen_test
    def test_sadd_with_error(self):
        yield self.client.connect()
        key = self.uuid4()
        value = self.uuid4()
        self._execute_result = exceptions.RedisError('Test Exception')
        with mock.patch.object(self.client, '_execute', self._execute):
            with self.assertRaises(exceptions.RedisError):
                yield self.client.sadd(key, value)

    @testing.gen_test
    def test_sdiff(self):
        yield self.client.connect()
        key1 = self.uuid4()
        key2 = self.uuid4()
        value1 = self.uuid4()
        value2 = self.uuid4()
        value3 = self.uuid4()
        result = yield self.client.sadd(key1, value1, value2)
        self.assertTrue(result)
        result = yield self.client.sadd(key2, value1, value3)
        self.assertTrue(result)
        result = yield self.client.sdiff(key1, key2)
        self.assertListEqual(result, [value2])

    @testing.gen_test
    def test_sdiffstore(self):
        yield self.client.connect()
        key1 = self.uuid4()
        key2 = self.uuid4()
        key3 = self.uuid4()
        value1 = self.uuid4()
        value2 = self.uuid4()
        value3 = self.uuid4()
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
        yield self.client.connect()
        key1 = self.uuid4()
        key2 = self.uuid4()
        value1 = self.uuid4()
        value2 = self.uuid4()
        value3 = self.uuid4()
        result = yield self.client.sadd(key1, value1, value2)
        self.assertTrue(result)
        result = yield self.client.sadd(key2, value2, value3)
        self.assertTrue(result)
        result = yield self.client.sinter(key1, key2)
        self.assertListEqual(result, [value2])

    @testing.gen_test
    def test_sinterstore(self):
        yield self.client.connect()
        key1 = self.uuid4()
        key2 = self.uuid4()
        key3 = self.uuid4()
        value1 = self.uuid4()
        value2 = self.uuid4()
        value3 = self.uuid4()
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
        yield self.client.connect()
        key = self.uuid4()
        value = self.uuid4()
        result = yield self.client.sadd(key, value)
        self.assertTrue(result)
        result = yield self.client.sismember(key, value)
        self.assertTrue(result)

    @testing.gen_test
    def test_sadd_sismember_false(self):
        yield self.client.connect()
        key = self.uuid4()
        value1 = self.uuid4()
        value2 = self.uuid4()
        result = yield self.client.sadd(key, value1)
        self.assertTrue(result)
        result = yield self.client.sismember(key, value2)
        self.assertFalse(result)

    @testing.gen_test
    def test_scard(self):
        yield self.client.connect()
        key = self.uuid4()
        value1 = self.uuid4()
        value2 = self.uuid4()
        value3 = self.uuid4()
        result = yield self.client.sadd(key, value1, value2, value3)
        self.assertTrue(result)
        result = yield self.client.scard(key)
        self.assertEqual(result, 3)

    @testing.gen_test
    def test_smembers(self):
        yield self.client.connect()
        key = self.uuid4()
        value1 = self.uuid4()
        value2 = self.uuid4()
        value3 = self.uuid4()
        result = yield self.client.sadd(key, value1, value2, value3)
        self.assertTrue(result)
        result = yield self.client.smembers(key)
        self.assertListEqual(sorted(result), sorted([value1, value2, value3]))

    @testing.gen_test
    def test_smove(self):
        yield self.client.connect()
        key1 = self.uuid4()
        key2 = self.uuid4()
        value1 = self.uuid4()
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
        yield self.client.connect()
        key = self.uuid4()
        value1 = self.uuid4()
        value2 = self.uuid4()
        value3 = self.uuid4()
        values = [value1, value2, value3]
        result = yield self.client.sadd(key, *values)
        self.assertTrue(result)
        member = yield self.client.spop(key)
        self.assertIn(member, values)
        members = yield self.client.smembers(key)
        self.assertNotIn(member, members)

    @testing.gen_test
    def test_srandmember(self):
        yield self.client.connect()
        key = self.uuid4()
        value1 = self.uuid4()
        value2 = self.uuid4()
        value3 = self.uuid4()
        values = [value1, value2, value3]
        result = yield self.client.sadd(key, *values)
        self.assertTrue(result)
        member = yield self.client.srandmember(key)
        self.assertIn(member, values)
        members = yield self.client.smembers(key)
        self.assertIn(member, members)

    @testing.gen_test
    def test_srandmember_multi(self):
        yield self.client.connect()
        key = self.uuid4()
        value1 = self.uuid4()
        value2 = self.uuid4()
        value3 = self.uuid4()
        values = [value1, value2, value3]
        result = yield self.client.sadd(key, *values)
        self.assertTrue(result)
        members = yield self.client.srandmember(key, 2)
        for member in members:
            self.assertIn(member, values)
        self.assertEqual(len(members), 2)

    @testing.gen_test
    def test_srem(self):
        yield self.client.connect()
        key = self.uuid4()
        value1 = self.uuid4()
        value2 = self.uuid4()
        value3 = self.uuid4()
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
        yield self.client.connect()
        key = self.uuid4()
        value1 = self.uuid4()
        value2 = self.uuid4()
        value3 = self.uuid4()
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
        yield self.client.connect()
        key = self.uuid4()
        value = self.uuid4()
        self._execute_result = exceptions.RedisError('Test Exception')
        with mock.patch.object(self.client, '_execute', self._execute):
            with self.assertRaises(exceptions.RedisError):
                yield self.client.srem(key, value)

    @testing.gen_test
    def test_sscan(self):
        yield self.client.connect()
        key = self.uuid4()
        value1 = self.uuid4()
        value2 = self.uuid4()
        value3 = self.uuid4()
        values = [value1, value2, value3]
        result = yield self.client.sadd(key, *values)
        self.assertTrue(result)
        cursor, result = yield self.client.sscan(key, 0)
        self.assertListEqual(sorted(result), sorted(values))
        self.assertEqual(cursor, 0)

    @testing.gen_test
    def test_sscan_with_pattern(self):
        yield self.client.connect()
        key = self.uuid4()
        value1 = self.uuid4()
        value2 = self.uuid4()
        value3 = self.uuid4()
        values = [value1, value2, value3]
        result = yield self.client.sadd(key, *values)
        self.assertTrue(result)
        cursor, result = yield self.client.sscan(key, 0, '*')
        self.assertListEqual(sorted(result), sorted(values))
        self.assertEqual(cursor, 0)

    @testing.gen_test
    def test_sscan_with_pattern_and_count(self):
        yield self.client.connect()
        key = self.uuid4()
        value1 = self.uuid4()
        value2 = self.uuid4()
        value3 = self.uuid4()
        values = [value1, value2, value3]
        result = yield self.client.sadd(key, *values)
        self.assertTrue(result)
        cursor, result = yield self.client.sscan(key, 0, '*', 10)
        self.assertListEqual(sorted(result), sorted(values))
        self.assertEqual(cursor, 0)

    @testing.gen_test
    def test_sscan_with_error(self):
        yield self.client.connect()
        key = self.uuid4()
        value = self.uuid4()
        self._execute_result = exceptions.RedisError('Test Exception')
        with mock.patch.object(self.client, '_execute', self._execute):
            with self.assertRaises(exceptions.RedisError):
                yield self.client.sscan(key, 0)

    @testing.gen_test
    def test_sunion(self):
        yield self.client.connect()
        key1 = self.uuid4()
        key2 = self.uuid4()
        value1 = self.uuid4()
        value2 = self.uuid4()
        value3 = self.uuid4()
        result = yield self.client.sadd(key1, value1, value2)
        self.assertTrue(result)
        result = yield self.client.sadd(key2, value2, value3)
        self.assertTrue(result)
        result = yield self.client.sunion(key1, key2)
        self.assertListEqual(sorted(result), sorted([value1, value2, value3]))

    @testing.gen_test
    def test_suinionstore(self):
        yield self.client.connect()
        key1 = self.uuid4()
        key2 = self.uuid4()
        key3 = self.uuid4()
        value1 = self.uuid4()
        value2 = self.uuid4()
        value3 = self.uuid4()
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
