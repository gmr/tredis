from tornado import gen
from tornado import testing

import tredis

from . import base


class StringTests(base.AsyncTestCase):

    @testing.gen_test
    def test_append(self):
        key, value1, value2 = self.uuid4(3)
        result = yield self.expiring_set(key, value1)
        self.assertTrue(result)
        result = yield self.client.append(key, value2)
        self.assertTrue(result)
        result = yield self.client.get(key)
        self.assertEqual(result, value1 + value2)

    @testing.gen_test
    def test_append_of_non_existent_value(self):
        key, value = self.uuid4(2)
        result = yield self.client.append(key, value)
        self.assertTrue(result)

    @testing.gen_test
    def test_bitcount(self):
        key = self.uuid4(1)
        result = yield self.expiring_set(key, 'foobar')
        self.assertTrue(result)
        result = yield self.client.bitcount(key)
        self.assertEqual(result, 26)

    @testing.gen_test
    def test_bitcount_with_start_and_not_end(self):
        key = self.uuid4(1)
        with self.assertRaises(ValueError):
            yield self.client.bitcount(key, 1)

    @testing.gen_test
    def test_bitcount_without_start_and_with_end(self):
        key = self.uuid4(1)
        with self.assertRaises(ValueError):
            yield self.client.bitcount(key, end=1)

    @testing.gen_test
    def test_bitcount_with_start_and_end(self):
        key = self.uuid4(1)
        result = yield self.expiring_set(key, 'foobar')
        self.assertTrue(result)
        result = yield self.client.bitcount(key, 1, -1)
        self.assertEqual(result, 22)

    @testing.gen_test
    def test_bitop_and(self):
        key1, key2, key3 = self.uuid4(3)
        result = yield self.expiring_set(key1, 'deadbeef')
        self.assertTrue(result)
        result = yield self.expiring_set(key2, '8badf00d')
        self.assertTrue(result)
        result = yield self.client.bitop(b'AND', key3, key1, key2)
        self.assertEqual(result, 8)
        result = yield self.client.get(key3)
        self.assertEqual(result, b' `adb  d')

    @testing.gen_test
    def test_bitop_and_const(self):
        key1, key2, key3 = self.uuid4(3)
        result = yield self.expiring_set(key1, 'deadbeef')
        self.assertTrue(result)
        result = yield self.expiring_set(key2, '8badf00d')
        self.assertTrue(result)
        result = yield self.client.bitop(tredis.BITOP_AND, key3, key1, key2)
        self.assertEqual(result, 8)
        result = yield self.client.get(key3)
        self.assertEqual(result, b' `adb  d')

    @testing.gen_test
    def test_bitop_or(self):
        key1, key2, key3 = self.uuid4(3)
        result = yield self.expiring_set(key1, 'deadbeef')
        self.assertTrue(result)
        result = yield self.expiring_set(key2, '8badf00d')
        self.assertTrue(result)
        result = yield self.client.bitop(b'OR', key3, key1, key2)
        self.assertEqual(result, 8)
        result = yield self.client.get(key3)
        self.assertEqual(result, b'|gadfuuf')

    @testing.gen_test
    def test_bitop_or_const(self):
        key1, key2, key3 = self.uuid4(3)
        result = yield self.expiring_set(key1, 'deadbeef')
        self.assertTrue(result)
        result = yield self.expiring_set(key2, '8badf00d')
        self.assertTrue(result)
        result = yield self.client.bitop(tredis.BITOP_OR, key3, key1, key2)
        self.assertEqual(result, 8)
        result = yield self.client.get(key3)
        self.assertEqual(result, b'|gadfuuf')

    @testing.gen_test
    def test_bitop_xor(self):
        key1, key2, key3 = self.uuid4(3)
        result = yield self.expiring_set(key1, 'deadbeef')
        self.assertTrue(result)
        result = yield self.expiring_set(key2, '8badf00d')
        self.assertTrue(result)
        result = yield self.client.bitop(b'XOR', key3, key1, key2)
        self.assertEqual(result, 8)
        result = yield self.client.get(key3)
        self.assertEqual(result, b'\\\x07\x00\x00\x04UU\x02')

    @testing.gen_test
    def test_bitop_xor_const(self):
        key1, key2, key3 = self.uuid4(3)
        result = yield self.expiring_set(key1, 'deadbeef')
        self.assertTrue(result)
        result = yield self.expiring_set(key2, '8badf00d')
        self.assertTrue(result)
        result = yield self.client.bitop(tredis.BITOP_XOR, key3, key1, key2)
        self.assertEqual(result, 8)
        result = yield self.client.get(key3)
        self.assertEqual(result, b'\\\x07\x00\x00\x04UU\x02')

    @testing.gen_test
    def test_bitop_not(self):
        key1, key2 = self.uuid4(2)
        result = yield self.expiring_set(key1, 'deadbeef')
        self.assertTrue(result)
        result = yield self.client.bitop(b'NOT', key2, key1)
        self.assertEqual(result, 8)
        result = yield self.client.get(key2)
        self.assertEqual(result, b'\x9b\x9a\x9e\x9b\x9d\x9a\x9a\x99')

    @testing.gen_test
    def test_bitop_not_const(self):
        key1, key2 = self.uuid4(2)
        result = yield self.expiring_set(key1, 'deadbeef')
        self.assertTrue(result)
        result = yield self.client.bitop(tredis.BITOP_NOT, key2, key1)
        self.assertEqual(result, 8)
        result = yield self.client.get(key2)
        self.assertEqual(result, b'\x9b\x9a\x9e\x9b\x9d\x9a\x9a\x99')

    @testing.gen_test
    def test_bitop_not_invalid_operation(self):
        key1, key2 = self.uuid4(2)
        with self.assertRaises(ValueError):
            yield self.client.bitop(b'YO!', key2, key1, key1)

    @testing.gen_test
    def test_bitop_not_too_many_keys(self):
        key1, key2 = self.uuid4(2)
        with self.assertRaises(ValueError):
            yield self.client.bitop(tredis.BITOP_NOT, key2, key1, key1)

    @testing.gen_test
    def test_bitpos(self):
        key = self.uuid4(1)
        result = self.expiring_set(key, b'\xff\xf0\x00')
        self.assertTrue(result)
        result = yield self.client.bitpos(key, 0)
        self.assertEqual(result, 12)

    @testing.gen_test
    def test_bitpos_invalid_bit(self):
        key = self.uuid4(1)
        with self.assertRaises(ValueError):
            yield self.client.bitpos(key, 2)

    @testing.gen_test
    def test_bitpos_with_start_and_end(self):
        key = self.uuid4(1)
        result = self.expiring_set(key, b'\xff\xf0\xf0')
        self.assertTrue(result)
        result = yield self.client.bitpos(key, 1, 1, 1)
        self.assertEqual(result, 8)

    @testing.gen_test
    def test_bitpos_with_start_and_not_end(self):
        key = self.uuid4(1)
        with self.assertRaises(ValueError):
            yield self.client.bitpos(key, 0, 1)

    @testing.gen_test
    def test_bitpos_without_start_and_with_end(self):
        key = self.uuid4(1)
        with self.assertRaises(ValueError):
            yield self.client.bitpos(key, 0, end=1)

    @testing.gen_test
    def test_decr(self):
        key = self.uuid4()
        result = yield self.expiring_set(key, b'10')
        self.assertTrue(result)
        result = yield self.client.decr(key)
        self.assertEqual(result, 9)
        result = yield self.client.decr(key)
        self.assertEqual(result, 8)
        result = yield self.client.get(key)
        self.assertEqual(int(result), 8)

    @testing.gen_test
    def test_decrby(self):
        key = self.uuid4()
        result = yield self.expiring_set(key, b'10')
        self.assertTrue(result)
        result = yield self.client.decrby(key, 2)
        self.assertEqual(result, 8)
        result = yield self.client.decrby(key, 3)
        self.assertEqual(result, 5)
        result = yield self.client.get(key)
        self.assertEqual(int(result), 5)

    @testing.gen_test
    def test_getbit(self):
        key = self.uuid4()
        result = yield self.expiring_set(key, b'\x0f')
        self.assertTrue(result)
        result = yield self.client.getbit(key, 4)
        self.assertEqual(result, 1)

    @testing.gen_test
    def test_getrange(self):
        key, value = self.uuid4(2)
        result = yield self.expiring_set(key, value)
        self.assertTrue(result)
        result = yield self.client.getrange(key, 2, -3)
        self.assertEqual(result, value[2:-2])

    @testing.gen_test
    def test_getset(self):
        key, value1, value2 = self.uuid4(3)
        result = yield self.expiring_set(key, value1)
        self.assertTrue(result)
        result = yield self.client.getset(key, value2)
        self.assertEquals(result, value1)
        result = yield self.client.get(key)
        self.assertEquals(result, value2)

    @testing.gen_test
    def test_incr(self):
        key = self.uuid4()
        result = yield self.client.incr(key)
        self.assertEqual(result, 1)
        result = yield self.client.incr(key)
        self.assertEqual(result, 2)
        result = yield self.client.get(key)
        self.assertEqual(int(result), 2)
        result = yield self.client.delete(key)
        self.assertTrue(result)

    @testing.gen_test
    def test_incrby(self):
        key = self.uuid4()
        result = yield self.expiring_set(key, b'10')
        self.assertTrue(result)
        result = yield self.client.incrby(key, 2)
        self.assertEqual(result, 12)
        result = yield self.client.incrby(key, 3)
        self.assertEqual(result, 15)
        result = yield self.client.get(key)
        self.assertEqual(int(result), 15)

    @testing.gen_test
    def test_incrbyfloat(self):
        key = self.uuid4()
        result = yield self.expiring_set(key, b'10.50')
        self.assertTrue(result)
        result = yield self.client.incrbyfloat(key, 0.1)
        self.assertEqual(result, b'10.6')
        result = yield self.client.get(key)
        self.assertEqual(result, b'10.6')

    @testing.gen_test
    def test_mget(self):
        key1, key2, key3, value1, value2, value3 = self.uuid4(6)
        result = yield self.expiring_set(key1, value1)
        self.assertTrue(result)
        result = yield self.expiring_set(key2, value2)
        self.assertTrue(result)
        result = yield self.expiring_set(key3, value3)
        self.assertTrue(result)
        result = yield self.client.mget(key1, key2, key3)
        self.assertListEqual([value1, value2, value3], result)

    @testing.gen_test
    def test_mset(self):
        key1, key2, key3, value1, value2, value3 = self.uuid4(6)
        values = {key1: value1,
                  key2: value2,
                  key3: value3}
        result = yield self.client.mset(values)
        self.assertTrue(result)
        result = yield self.client.mget(key1, key2, key3)
        self.assertListEqual([value1, value2, value3], result)

    @testing.gen_test
    def test_msetnx(self):
        key1, key2, key3, value1, value2, value3 = self.uuid4(6)
        values = {key1: value1,
                  key2: value2,
                  key3: value3}
        result = yield self.client.msetnx(values)
        self.assertTrue(result)
        result = yield self.client.mget(key1, key2, key3)
        self.assertListEqual([value1, value2, value3], result)

    @testing.gen_test
    def test_msetnx_fail(self):
        key1, key2, key3, value1, value2, value3, value4 = self.uuid4(7)
        result = self.expiring_set(key1, value4)
        self.assertTrue(result)
        values = {key1: value1,
                  key2: value2,
                  key3: value3}
        result = yield self.client.msetnx(values)
        self.assertFalse(result)

    @testing.gen_test
    def test_psetex(self):
        key, value = self.uuid4(2)
        result = yield self.client.psetex(key, 100, value)
        self.assertTrue(result)
        result = yield self.client.get(key)
        self.assertEqual(result, value)
        yield gen.sleep(0.300)
        result = yield self.client.get(key)
        self.assertIsNone(result)

    @testing.gen_test
    def test_setbit(self):
        key = self.uuid4()
        result = yield self.client.setbit(key, 100, 1)
        self.assertEqual(result, 0)
        result = yield self.client.getbit(key, 100)
        self.assertEqual(result, 1)
        result = yield self.client.setbit(key, 100, 0)
        self.assertEqual(result, 1)

    @testing.gen_test
    def test_setbit_invalid_bit(self):
        with self.assertRaises(ValueError):
            key = self.uuid4()
            yield self.client.setbit(key, 100, 2)

    @testing.gen_test
    def test_setex(self):
        key, value = self.uuid4(2)
        result = yield self.client.setex(key, 1, value)
        self.assertTrue(result)
        result = yield self.client.get(key)
        self.assertEqual(result, value)
        yield gen.sleep(1.0)
        result = yield self.client.get(key)
        self.assertIsNone(result)

    @testing.gen_test
    def test_setnx(self):
        key, value = self.uuid4(2)
        result = yield self.client.setnx(key, value)
        self.assertTrue(result)
        result = yield self.client.setnx(key, value)
        self.assertFalse(result)
        result = yield self.client.delete(key)
        self.assertTrue(result)

    @testing.gen_test
    def test_simple_set_and_get(self):
        key, value = self.uuid4(2)
        result = yield self.expiring_set(key, value)
        self.assertTrue(result)
        result = yield self.client.get(key)
        self.assertEqual(result, value)

    @testing.gen_test
    def test_simple_set_int_and_get(self):
        key = self.uuid4()
        result = yield self.expiring_set(key, 2)
        self.assertTrue(result)
        result = yield self.client.get(key)
        self.assertEqual(result, b'2')

    @testing.gen_test
    def test_simple_set_str_and_get(self):
        key = self.uuid4()
        result = yield self.expiring_set(key, 'hi')
        self.assertTrue(result)
        result = yield self.client.get(key)
        self.assertEqual(result, b'hi')

    @testing.gen_test
    def test_simple_set_invalid_type(self):
        key = self.uuid4()
        with self.assertRaises(ValueError):
            yield self.expiring_set(key, {})

    @testing.gen_test
    def test_set_ex(self):
        key, value = self.uuid4(2)
        result = yield self.client.set(key, value, ex=1)
        self.assertTrue(result)
        result = yield self.client.get(key)
        self.assertEqual(result, value)
        yield gen.sleep(1.0)
        result = yield self.client.get(key)
        self.assertIsNone(result)

    @testing.gen_test
    def test_set_px(self):
        key, value = self.uuid4(2)
        result = yield self.client.set(key, value, px=100)
        self.assertTrue(result)
        result = yield self.client.get(key)
        self.assertEqual(result, value)
        yield gen.sleep(0.300)
        result = yield self.client.get(key)
        self.assertIsNone(result)

    @testing.gen_test
    def test_set_nx(self):
        key, value = self.uuid4(2)
        result = yield self.expiring_set(key, value, nx=True)
        self.assertTrue(result)
        result = yield self.client.get(key)
        self.assertEqual(result, value)

    @testing.gen_test
    def test_set_nx_with_value(self):
        key, value = self.uuid4(2)
        result = yield self.expiring_set(key, value, nx=True)
        self.assertTrue(result)
        result = yield self.client.get(key)
        self.assertEqual(result, value)
        result = yield self.expiring_set(key, value, nx=True)
        self.assertFalse(result)

    @testing.gen_test
    def test_set_xx_with_value(self):
        key, value = self.uuid4(2)
        result = yield self.expiring_set(key, value)
        self.assertTrue(result)
        result = yield self.client.get(key)
        self.assertEqual(result, value)
        result = yield self.expiring_set(key, value, xx=True)
        self.assertTrue(result)

    @testing.gen_test
    def test_set_xx_without_value(self):
        key, value = self.uuid4(2)
        result = yield self.expiring_set(key, value, xx=True)
        self.assertFalse(result)

    @testing.gen_test
    def test_setrange(self):
        key, value1, value2 = self.uuid4(3)
        result = yield self.expiring_set(key, value1)
        self.assertTrue(result)
        result = yield self.client.setrange(key, 4, value2)
        self.assertEqual(result, len(value1) + 4)
        result = yield self.client.get(key)
        self.assertEqual(result, value1[0:4] + value2)

    @testing.gen_test
    def test_strlen(self):
        key, value = self.uuid4(2)
        result = yield self.expiring_set(key, value)
        self.assertTrue(result)
        result = yield self.client.strlen(key)
        self.assertTrue(result, len(value))


class PipelineTests(base.AsyncTestCase):

    @testing.gen_test
    def test_command_pipeline(self):
        key1, key2, key3, value1, value2, = self.uuid4(5)
        self.client.pipeline_start()
        self.client.set(key1, value1, 5)
        self.client.set(key2, value2, 5)
        self.client.incr(key3)
        self.client.incr(key3)
        self.client.incr(key3)
        self.client.incr(key3)
        self.client.get(key1)
        self.client.get(key2)
        self.client.get(key3)
        expectation = [True, True, 1, 2, 3, 4, value1, value2, b'4']
        response = yield self.client.pipeline_execute()
        self.assertListEqual(response, expectation)
