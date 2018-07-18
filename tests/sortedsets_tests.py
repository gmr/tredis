import mock

from tornado import testing

from tredis import exceptions

from . import base


class SortedSetTests(base.AsyncTestCase):
    @testing.gen_test
    def test_zadd_single(self):
        key, value = self.uuid4(2)
        result = yield self.client.zadd(key, '1', value)
        self.assertEqual(result, 1)

    @testing.gen_test
    def test_zadd_multiple(self):
        key, value1, value2, value3 = self.uuid4(4)
        result = yield self.client.zadd(key, '1', value1, '2', value2,
                                        '3', value3)
        self.assertEqual(result, 3)

    @testing.gen_test
    def test_zadd_dict(self):
        key, value1, value2, value3 = self.uuid4(4)
        result = yield self.client.zadd(key, {'1': value1, '2': value2,
                                        '3': value3})
        self.assertEqual(result, 3)

    @testing.gen_test
    def test_zadd_multiple_dupe(self):
        key, value1, value2, value3 = self.uuid4(4)
        result = yield self.client.zadd(key, '1', value1, '2', value2,
                                        '3', value3, '4', value3)
        self.assertEqual(result, 3)

    @testing.gen_test
    def test_zadd_ch(self):
        key, value1, value2, value3 = self.uuid4(4)
        result = yield self.client.zadd(key, '1', value1, '2', value2)
        self.assertEqual(result, 2)
        result = yield self.client.zadd(key, '2', value1, '3', value2,
                                        '4', value3, ch=True)
        self.assertEqual(result, 3)

    @testing.gen_test
    def test_zadd_xx(self):
        key, value1, value2, value3 = self.uuid4(4)
        result = yield self.client.zadd(key, '1', value1, '2', value2)
        self.assertEqual(result, 2)
        result = yield self.client.zadd(key, '2', value1, '3', value2,
                                        '4', value3, xx=True)
        self.assertEqual(result, 0)

    @testing.gen_test
    def test_zadd_nx(self):
        key, value1, value2, value3 = self.uuid4(4)
        result = yield self.client.zadd(key, '1', value1, '2', value2)
        self.assertEqual(result, 2)
        result = yield self.client.zadd(key, '2', value1, '3', value2,
                                        '4', value3, nx=True, ch=True)
        self.assertEqual(result, 1)

    @testing.gen_test
    def test_zadd_incr(self):
        key, value = self.uuid4(2)
        result = yield self.client.zadd(key, '1', value)
        self.assertEqual(result, 1)
        result = yield self.client.zadd(key, '10', value, incr=True)
        self.assertEqual(result, b'11')

    @testing.gen_test
    def test_zadd_with_error(self):
        key, score, value = self.uuid4(3)
        self._execute_result = exceptions.RedisError('Test Exception')
        with mock.patch.object(self.client, '_execute', self._execute):
            with self.assertRaises(exceptions.RedisError):
                yield self.client.zadd(key, score, value)

    @testing.gen_test
    def test_zcard_with_extant_set(self):
        key, value1, value2, value3 = self.uuid4(4)
        result = yield self.client.zadd(key, '1', value1, '2', value2,
                                        '3', value3)
        self.assertEqual(result, 3)
        result = yield self.client.zcard(key)
        self.assertEqual(result, 3)

    @testing.gen_test
    def test_zcard_with_nonextant_set(self):
        key = self.uuid4()
        result = yield self.client.zcard(key)
        self.assertEqual(result, 0)

    @testing.gen_test
    def test_zrangebyscore(self):
        key, value1, value2, value3 = self.uuid4(4)
        result = yield self.client.zadd(key, '1', value1, '2', value2,
                                        '3', value3)
        self.assertEqual(result, 3)
        result = yield self.client.zrangebyscore(key, '1', '2')
        self.assertListEqual(result, [value1, value2])

    @testing.gen_test
    def test_zrangebyscore_withitems(self):
        key, value1, value2, value3 = self.uuid4(4)
        result = yield self.client.zadd(key, '1', value1, '2', value2,
                                        '3', value3)
        self.assertEqual(result, 3)
        result = yield self.client.zrangebyscore(key, '1', '2',
                                                 with_scores=True)
        self.assertListEqual(result, [value1, b'1', value2, b'2'])

    @testing.gen_test
    def test_zrangebyscore_offset(self):
        key, value1, value2, value3 = self.uuid4(4)
        result = yield self.client.zadd(key, '1', value1, '2', value2,
                                        '3', value3)
        self.assertEqual(result, 3)
        result = yield self.client.zrangebyscore(key, '1', '2',
                                                 offset=1, count=20)
        self.assertListEqual(result, [value2])

    @testing.gen_test
    def test_zrangebyscore_count(self):
        key, value1, value2, value3 = self.uuid4(4)
        result = yield self.client.zadd(key, '1', value1, '2', value2,
                                        '3', value3)
        self.assertEqual(result, 3)
        result = yield self.client.zrangebyscore(key, '1', '3',
                                                 offset=0, count=1)
        self.assertListEqual(result, [value1])

    @testing.gen_test
    def test_zremrangebyscore(self):
        key, value1, value2, value3 = self.uuid4(4)
        result = yield self.client.zadd(key, '1', value1, '2', value2,
                                        '3', value3)
        self.assertEqual(result, 3)
        result = yield self.client.zremrangebyscore(key, '1', '2')
        self.assertEqual(result, 2)

    @testing.gen_test
    def test_zremrangebyscore_inf(self):
        key, value1, value2, value3 = self.uuid4(4)
        result = yield self.client.zadd(key, '1', value1, '2', value2,
                                        '3', value3)
        self.assertEqual(result, 3)
        result = yield self.client.zremrangebyscore(key, '(1', 'inf')
        self.assertEqual(result, 2)

    @testing.gen_test
    def test_zscore_with_member_of_set(self):
        key, value1, value2, value3 = self.uuid4(4)
        result = yield self.client.zadd(key, '1', value1, '2', value2,
                                        '3', value3)
        self.assertEqual(result, 3)
        result = yield self.client.zscore(key, value1)
        self.assertEqual(result, b'1')

    @testing.gen_test
    def test_zscore_with_nonmember_of_set(self):
        key, value1 = self.uuid4(2)
        result = yield self.client.zscore(key, value1)
        self.assertEqual(result, None)
