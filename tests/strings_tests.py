import uuid

from tornado import gen
from tornado import testing

from . import base


class StringTests(base.AsyncTestCase):

    @testing.gen_test
    def test_simple_set_and_get(self):
        yield self.client.connect()
        key = str(uuid.uuid4()).encode('ascii')
        value = str(uuid.uuid4()).encode('ascii')
        result = yield self.client.set(key, value)
        self.assertTrue(result)
        result = yield self.client.get(key)
        self.assertEqual(result, value)

    @testing.gen_test
    def test_set_ex(self):
        yield self.client.connect()
        key = str(uuid.uuid4()).encode('ascii')
        value = str(uuid.uuid4()).encode('ascii')
        result = yield self.client.set(key, value, ex=1)
        self.assertTrue(result)
        result = yield self.client.get(key)
        self.assertEqual(result, value)
        yield gen.sleep(1.0)
        result = yield self.client.get(key)
        self.assertIsNone(result)

    @testing.gen_test
    def test_set_px(self):
        yield self.client.connect()
        key = str(uuid.uuid4()).encode('ascii')
        value = str(uuid.uuid4()).encode('ascii')
        result = yield self.client.set(key, value, px=100)
        self.assertTrue(result)
        result = yield self.client.get(key)
        self.assertEqual(result, value)
        yield gen.sleep(0.300)
        result = yield self.client.get(key)
        self.assertIsNone(result)

    @testing.gen_test
    def test_set_nx(self):
        yield self.client.connect()
        key = str(uuid.uuid4()).encode('ascii')
        value = str(uuid.uuid4()).encode('ascii')
        result = yield self.client.set(key, value, nx=True)
        self.assertTrue(result)
        result = yield self.client.get(key)
        self.assertEqual(result, value)

    @testing.gen_test
    def test_set_nx_with_value(self):
        yield self.client.connect()
        key = str(uuid.uuid4()).encode('ascii')
        value = str(uuid.uuid4()).encode('ascii')
        result = yield self.client.set(key, value, nx=True)
        self.assertTrue(result)
        result = yield self.client.get(key)
        self.assertEqual(result, value)
        result = yield self.client.set(key, value, nx=True)
        self.assertFalse(result)

    @testing.gen_test
    def test_set_xx_with_value(self):
        yield self.client.connect()
        key = str(uuid.uuid4()).encode('ascii')
        value = str(uuid.uuid4()).encode('ascii')
        result = yield self.client.set(key, value)
        self.assertTrue(result)
        result = yield self.client.get(key)
        self.assertEqual(result, value)
        result = yield self.client.set(key, value, xx=True)
        self.assertTrue(result)

    @testing.gen_test
    def test_set_xx_without_value(self):
        yield self.client.connect()
        key = str(uuid.uuid4()).encode('ascii')
        value = str(uuid.uuid4()).encode('ascii')
        result = yield self.client.set(key, value, xx=True)
        self.assertFalse(result)
