from tornado import testing

from . import base


class HashTests(base.AsyncTestCase):

    @testing.gen_test
    def test_hset(self):
        key, field, value = self.uuid4(3)
        result = yield self.client.hset(key, field, value)
        self.assertEqual(result, 1)

    @testing.gen_test
    def test_hset_return_value_for_overwrite(self):
        key, field, init_value, new_value = self.uuid4(4)
        result = yield self.client.hset(key, field, init_value)
        self.assertEqual(result, 1)

        result = yield self.client.hset(key, field, new_value)
        self.assertEqual(result, 0)

    @testing.gen_test
    def test_hget(self):
        key, field, value = self.uuid4(3)
        result = yield self.client.hset(key, field, value)
        self.assertEqual(result, 1)
        result = yield self.client.hget(key, field)
        self.assertEqual(result, value)

    @testing.gen_test
    def test_hset_overwrites(self):
        key, field, init_value, new_value = self.uuid4(4)
        yield self.client.hset(key, field, init_value)
        yield self.client.hset(key, field, new_value)
        value = yield self.client.hget(key, field)
        self.assertEqual(value, new_value)

    @testing.gen_test
    def test_hgetall_returns_dict(self):
        key = self.uuid4()
        field1, value1 = self.uuid4(2)
        yield self.client.hset(key, field1, value1)
        result = yield self.client.hgetall(key)
        self.assertEqual(result, {field1: value1})

        field2, value2 = self.uuid4(2)
        yield self.client.hset(key, field2, value2)

        result = yield self.client.hgetall(key)
        self.assertEqual(result, {field1: value1, field2: value2})

    @testing.gen_test
    def test_hgetall_on_non_hash(self):
        key = self.uuid4()
        result = yield self.client.hgetall(key)
        self.assertEqual(result, {})

    @testing.gen_test
    def test_hmset(self):
        key = self.uuid4()
        field1, value1 = self.uuid4(2)
        field2, value2 = self.uuid4(2)
        field3, value3 = self.uuid4(2)
        result = yield self.client.hmset(
            key, {field1: value1, field2: value2, field3: value3})
        self.assertTrue(result)

        result = yield self.client.hgetall(key)
        self.assertEqual(result,
                         {field1: value1, field2: value2, field3: value3})

    @testing.gen_test
    def test_hmset_of_empty_dict(self):
        key = self.uuid4()
        result = yield self.client.hmset(key, {})
        self.assertFalse(result)

    @testing.gen_test
    def test_hmget(self):
        key = self.uuid4()
        field1, value1 = self.uuid4(2)
        field2, value2 = self.uuid4(2)
        field3, value3 = self.uuid4(2)
        yield self.client.hmset(
            key, {field1: value1, field2: value2, field3: value3})

        result = yield self.client.hmget(key, field1, field3)
        self.assertEqual(result, {field1: value1, field3: value3})

    @testing.gen_test
    def test_that_hmget_returns_none_values_for_nonexistent_key(self):
        key = self.uuid4()
        result = yield self.client.hmget(key, 'one', 'two', 'three')
        self.assertEqual(result, {'one': None, 'two': None, 'three': None})

    @testing.gen_test
    def test_hdel(self):
        key = self.uuid4()
        field1, value1 = self.uuid4(2)
        field2, value2 = self.uuid4(2)
        field3, value3 = self.uuid4(2)
        yield self.client.hmset(
            key, {field1: value1, field2: value2, field3: value3})

        result = yield self.client.hdel(key, field1)
        self.assertEqual(result, 1)

        result = yield self.client.hgetall(key)
        self.assertEqual(result, {field2: value2, field3: value3})

        result = yield self.client.hdel(key, field2, field3)
        self.assertEqual(result, 2)

        result = yield self.client.hgetall(key)
        self.assertEqual(result, {})

        result = yield self.client.hdel(key, 'nofield')
        self.assertEqual(result, 0)

        result = yield self.client.hdel(self.uuid4(), 'nofield')
        self.assertEqual(result, 0)

    @testing.gen_test
    def test_hdel_without_fields_returns_zero(self):
        result = yield self.client.hdel(self.uuid4())
        self.assertEqual(result, 0)

    @testing.gen_test
    def test_hexist(self):
        key = self.uuid4()
        result = yield self.client.hexists(key, 'nofield')
        self.assertFalse(result)

        yield self.client.hset(key, 'field', self.uuid4())
        result = yield self.client.hexists(key, 'field')
        self.assertTrue(result)

        result = yield self.client.hexists(key, 'nofield')
        self.assertFalse(result)

    @testing.gen_test
    def test_hincrby(self):
        key, field = self.uuid4(2)

        result = yield self.client.hincrby(key, field, 1)
        self.assertEqual(result, 1)

        result = yield self.client.hget(key, field)
        self.assertEqual(result, b'1')

        result = yield self.client.hincrby(key, field, 0x7fffFFFFffffFFFE)
        self.assertEqual(result, 0x7fffFFFFffffFFFF)

        result = yield self.client.hincrby(key, field, -0x7fffFFFFffffFFFF)
        self.assertEqual(result, 0)

    @testing.gen_test
    def test_hincrbyfloat(self):
        key, field = self.uuid4(2)

        result = yield self.client.hincrbyfloat(key, field, 10.5)
        self.assertAlmostEqual(result, 10.500000)

        result = yield self.client.hincrbyfloat(key, field, 0.1)
        self.assertAlmostEqual(result, 10.600000)

        result = yield self.client.hincrbyfloat(key, field, -5)
        self.assertAlmostEqual(result, 5.600000)

        result = yield self.client.hincrbyfloat(key, field, 2.0e2)
        self.assertAlmostEqual(result, 205.600000)

    @testing.gen_test
    def test_hkeys(self):
        key, field1, field2, field3 = self.uuid4(4)

        result = yield self.client.hkeys(key)
        self.assertEqual(result, [])

        yield self.client.hmset(key, {field1: self.uuid4(),
                                      field2: self.uuid4(),
                                      field3: self.uuid4()})
        result = yield self.client.hkeys(key)
        self.assertEqual(sorted(result), sorted([field1, field2, field3]))

    @testing.gen_test
    def test_hlen(self):
        key = self.uuid4()

        result = yield self.client.hlen(key)
        self.assertEqual(result, 0)

        yield self.client.hmset(key, {'a': 1, 'b': 2, 'c': 3})
        result = yield self.client.hlen(key)
        self.assertEqual(result, 3)

    @testing.gen_test
    def test_hsetnx(self):
        key, field, value = self.uuid4(3)

        result = yield self.client.hsetnx(key, field, value)
        self.assertEqual(result, 1)

        result = yield self.client.hget(key, field)
        self.assertEqual(result, value)

        result = yield self.client.hsetnx(key, field, self.uuid4())
        self.assertEqual(result, 0)

        result = yield self.client.hget(key, field)
        self.assertEqual(result, value)

    @testing.gen_test
    def test_hvals(self):
        key = self.uuid4()
        result = yield self.client.hvals(key)
        self.assertEqual(result, [])

        yield self.client.hmset(key,
                                {'f1': 'HelloWorld', 'f2': 99, 'f3': -256})

        result = yield self.client.hvals(key)
        self.assertEqual(sorted(result),
                         sorted([b'HelloWorld', b'99', b'-256']))
