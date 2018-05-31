from tornado import testing

import tredis.exceptions

from . import base


class ListTests(base.AsyncTestCase):

    @testing.gen_test
    def test_llen(self):
        key = self.uuid4()
        values = self.uuid4(5)
        yield self.client.lpush(key, *values)
        result = yield self.client.llen(key)
        self.assertEqual(result, len(values))

    @testing.gen_test
    def test_llen_of_nonexistent_list(self):
        result = yield self.client.llen(self.uuid4())
        self.assertEqual(result, 0)

    @testing.gen_test
    def test_llen_of_nonlist(self):
        key = self.uuid4()
        yield self.client.sadd(key, self.uuid4())
        with self.assertRaises(tredis.exceptions.TRedisException):
            yield self.client.llen(key)

    @testing.gen_test
    def test_lpush(self):
        key, value = self.uuid4(2)
        result = yield self.client.lpush(key, value)
        self.assertEqual(result, 1)  # list length

    @testing.gen_test
    def test_lpush_then_lpop(self):
        key, value = self.uuid4(2)
        yield self.client.lpush(key, value)
        result = yield self.client.lpop(key)
        self.assertEqual(result, value)

    @testing.gen_test
    def test_lpop_of_nonexistent_list(self):
        result = yield self.client.lpop(self.uuid4())
        self.assertIsNone(result)

    @testing.gen_test
    def test_lpushx_of_nonlist(self):
        key, name, value = self.uuid4(3)
        yield self.client.hset(key, name, value)
        with self.assertRaises(tredis.exceptions.RedisError):
            yield self.client.lpushx(key, self.uuid4())

    @testing.gen_test
    def test_lpushx_of_nonexistent_list(self):
        key, value = self.uuid4(2)
        result = yield self.client.lpushx(key, value)
        self.assertEqual(result, 0)

    @testing.gen_test
    def test_ltrim(self):
        key = self.uuid4()
        result = yield self.client.ltrim(key, 0, 10)
        values = self.uuid4(10)
        yield self.client.lpush(key, *values)
        result = yield self.client.ltrim(key, 0, 4)
        self.assertIs(result, True)

        result = yield self.client.llen(key)
        self.assertEqual(result, 5)

    @testing.gen_test
    def test_rpush(self):
        key, value = self.uuid4(2)
        result = yield self.client.rpush(key, value)
        self.assertEqual(result, 1)  # list length

    @testing.gen_test
    def test_rpushx_of_nonlist(self):
        key, name, value = self.uuid4(3)
        yield self.client.hset(key, name, value)
        with self.assertRaises(tredis.exceptions.RedisError):
            yield self.client.rpushx(key, self.uuid4())

    @testing.gen_test
    def test_rpushx_of_nonexistent_list(self):
        key, value = self.uuid4(2)
        result = yield self.client.rpushx(key, value)
        self.assertEqual(result, 0)

    @testing.gen_test
    def test_rpush_then_rpop(self):
        key, value = self.uuid4(2)
        yield self.client.rpush(key, value)
        result = yield self.client.rpop(key)
        self.assertEqual(result, value)

    @testing.gen_test
    def test_lrange(self):
        key = self.uuid4()
        values = list(self.uuid4(10))
        yield self.client.rpush(key, *values)

        result = yield self.client.lrange(key, 0, 4)
        self.assertEqual(result, values[:5])

        result = yield self.client.lrange(key, 0, -1)
        self.assertEqual(result, values)

        result = yield self.client.lrange(key, 0, -(len(values) + 1))
        self.assertEqual(result, [])

        result = yield self.client.lrange(key, 0, 10 * len(values))
        self.assertEqual(result, values)
