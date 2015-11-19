import mock

from tornado import testing

from tredis import exceptions

from . import base


class SetTests(base.AsyncTestCase):

    @testing.gen_test
    def test_sadd_single(self):
        yield self.client.connect()
        key, value = self.uuid4(2)
        result = yield self.client.sadd(key, value)
        self.assertEqual(result, 1)

    @testing.gen_test
    def test_sadd_multiple(self):
        yield self.client.connect()
        key, value1, value2, value3 = self.uuid4(4)
        result = yield self.client.sadd(key, value1, value2, value3)
        self.assertTrue(result)

    @testing.gen_test
    def test_sadd_multiple_dupe(self):
        yield self.client.connect()
        key, value1, value2, value3 = self.uuid4(4)
        result = yield self.client.sadd(key, value1, value2, value3, value3)
        self.assertEqual(result, 3)

    @testing.gen_test
    def test_sadd_with_error(self):
        yield self.client.connect()
        key, value = self.uuid4(2)
        self._execute_result = exceptions.RedisError('Test Exception')
        with mock.patch.object(self.client, '_execute', self._execute):
            with self.assertRaises(exceptions.RedisError):
                yield self.client.sadd(key, value)

    @testing.gen_test
    def test_sdiff(self):
        yield self.client.connect()
        key1, key2, value1, value2, value3 = self.uuid4(5)
        result = yield self.client.sadd(key1, value1, value2)
        self.assertTrue(result)
        result = yield self.client.sadd(key2, value1, value3)
        self.assertTrue(result)
        result = yield self.client.sdiff(key1, key2)
        self.assertListEqual(result, [value2])

    @testing.gen_test
    def test_sdiffstore(self):
        yield self.client.connect()
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
        yield self.client.connect()
        key1, key2, value1, value2, value3 = self.uuid4(5)
        result = yield self.client.sadd(key1, value1, value2)
        self.assertTrue(result)
        result = yield self.client.sadd(key2, value2, value3)
        self.assertTrue(result)
        result = yield self.client.sinter(key1, key2)
        self.assertListEqual(result, [value2])

    @testing.gen_test
    def test_sinterstore(self):
        yield self.client.connect()
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
        yield self.client.connect()
        key, value = self.uuid4(2)
        result = yield self.client.sadd(key, value)
        self.assertTrue(result)
        result = yield self.client.sismember(key, value)
        self.assertTrue(result)

    @testing.gen_test
    def test_sadd_sismember_false(self):
        yield self.client.connect()
        key, value1, value2 = self.uuid4(3)
        result = yield self.client.sadd(key, value1)
        self.assertTrue(result)
        result = yield self.client.sismember(key, value2)
        self.assertFalse(result)

    @testing.gen_test
    def test_scard(self):
        yield self.client.connect()
        key, value1, value2, value3 = self.uuid4(4)
        result = yield self.client.sadd(key, value1, value2, value3)
        self.assertTrue(result)
        result = yield self.client.scard(key)
        self.assertEqual(result, 3)

    @testing.gen_test
    def test_smembers(self):
        yield self.client.connect()
        key, value1, value2, value3 = self.uuid4(4)
        result = yield self.client.sadd(key, value1, value2, value3)
        self.assertTrue(result)
        result = yield self.client.smembers(key)
        self.assertListEqual(sorted(result), sorted([value1, value2, value3]))

    @testing.gen_test
    def test_smove(self):
        yield self.client.connect()
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
        yield self.client.connect()
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
        yield self.client.connect()
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
        yield self.client.connect()
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
        yield self.client.connect()
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
        yield self.client.connect()
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
        yield self.client.connect()
        key, value = self.uuid4(2)
        self._execute_result = exceptions.RedisError('Test Exception')
        with mock.patch.object(self.client, '_execute', self._execute):
            with self.assertRaises(exceptions.RedisError):
                yield self.client.srem(key, value)

    @testing.gen_test
    def test_sscan(self):
        yield self.client.connect()
        key, value1, value2, value3 = self.uuid4(4)
        values = [value1, value2, value3]
        result = yield self.client.sadd(key, *values)
        self.assertTrue(result)
        cursor, result = yield self.client.sscan(key, 0)
        self.assertListEqual(sorted(result), sorted(values))
        self.assertEqual(cursor, 0)

    @testing.gen_test
    def test_sscan_with_pattern(self):
        yield self.client.connect()
        key, value1, value2, value3 = self.uuid4(4)
        values = [value1, value2, value3]
        result = yield self.client.sadd(key, *values)
        self.assertTrue(result)
        cursor, result = yield self.client.sscan(key, 0, '*')
        self.assertListEqual(sorted(result), sorted(values))
        self.assertEqual(cursor, 0)

    @testing.gen_test
    def test_sscan_with_pattern_and_count(self):
        yield self.client.connect()
        key, value1, value2, value3 = self.uuid4(4)
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
        self._execute_result = exceptions.RedisError('Test Exception')
        with mock.patch.object(self.client, '_execute', self._execute):
            with self.assertRaises(exceptions.RedisError):
                yield self.client.sscan(key, 0)

    @testing.gen_test
    def test_sunion(self):
        yield self.client.connect()
        key1, key2, key3, value1, value2, value3 = self.uuid4(6)
        result = yield self.client.sadd(key1, value1, value2)
        self.assertTrue(result)
        result = yield self.client.sadd(key2, value2, value3)
        self.assertTrue(result)
        result = yield self.client.sunion(key1, key2)
        self.assertListEqual(sorted(result), sorted([value1, value2, value3]))

    @testing.gen_test
    def test_suinionstore(self):
        yield self.client.connect()
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


class PipelineTests(base.AsyncTestCase):

    @testing.gen_test
    def test_command_pipeline(self):
        yield self.client.connect()
        self.client.pipeline_start()

        expectation = []

        key1, key2, key3, key4, val1, val2, val3, val4, val5 = self.uuid4(9)
        self.client.sadd(key1, val1, val2, val3)
        expectation.append(True)  # 0
        self.client.sadd(key2, val3, val4, val5)
        expectation.append(True)  # 1

        self.client.scard(key1)
        expectation.append(3)  # 2

        self.client.sdiff(key1, key2)
        expectation.append(sorted([val1, val2]))  # 3

        self.client.sdiffstore(key3, key1, key2)
        expectation.append(2)  # 4
        self.client.smembers(key3)
        expectation.append(sorted([val1, val2]))  # 5

        self.client.sadd(key4, val1, val2, val2)
        expectation.append(2)  # 6

        self.client.sinter(key1, key2)
        expectation.append(sorted([val3]))  # 7

        self.client.sinterstore(key3, key1, key2)
        expectation.append(1)  # 8
        self.client.smembers(key3)
        expectation.append(sorted([val3]))  # 9
        self.client.sismember(key3, val3)
        expectation.append(True)  # 10

        self.client.smove(key2, key1, val4)
        expectation.append(True)  # 11
        self.client.sismember(key1, val4)
        expectation.append(True)  # 12

        self.client.srem(key1, val4)
        expectation.append(True)  # 13
        self.client.sismember(key1, val4)
        expectation.append(False)  # 14

        self.client.smembers(key1)
        expectation.append(sorted([val1, val2, val3]))  # 15
        print(val1,val2,val3)

        self.client.smembers(key2)
        expectation.append(sorted([val3, val5]))  # 16

        self.client.sunion(key1, key2)
        expectation.append(sorted([val1, val2, val3, val5]))  # 17

        self.client.sunionstore(key3, key1, key2)
        expectation.append(4)  # 18

        self.client.sscan(key3, 0)
        expectation.append((0, sorted([val1, val2, val3, val5])))  # 19

        result = yield self.client.pipeline_execute()
        for index, value in enumerate(result):
            if isinstance(value, list):
                result[index] = sorted(value)
            if isinstance(value, tuple) and isinstance(value[1],list):
                result[index] = value[0], sorted(value[1])

        self.assertListEqual(result, expectation)

    @testing.gen_test
    def test_pipeline_with_spop(self):
        yield self.client.connect()
        self.client.pipeline_start()
        key, value1, value2, value3 = self.uuid4(4)
        values = [value1, value2, value3]
        self.client.sadd(key, *values)
        self.client.spop(key)
        self.client.smembers(key)
        result = yield self.client.pipeline_execute()
        self.assertTrue(result[0])
        member, members = result[1], result[2]
        self.assertIn(member, values)
        self.assertNotIn(member, members)

    @testing.gen_test
    def test_pipeline_with_srandom(self):
        yield self.client.connect()
        self.client.pipeline_start()
        key, value1, value2, value3 = self.uuid4(4)
        values = [value1, value2, value3]
        self.client.sadd(key, *values)
        self.client.srandmember(key)
        result = yield self.client.pipeline_execute()
        self.assertTrue(result[0])
        self.assertIn(result[1], values)
