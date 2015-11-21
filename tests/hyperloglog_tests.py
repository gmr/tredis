from tornado import testing

from . import base


class HyperLogLogTests(base.AsyncTestCase):

    @testing.gen_test
    def test_hyperloglog(self):
        key1, key2, key3 = self.uuid4(3)
        result = yield self.client.pfadd(key1, 'foo', 'bar', 'zap', 'a')
        self.assertTrue(result)
        result = yield self.client.pfadd(key2, 'a', 'b', 'c', 'foo')
        self.assertTrue(result)
        result = yield self.client.pfmerge(key3, key1, key2)
        self.assertTrue(result)
        result = yield self.client.pfcount(key3)
        self.assertEqual(result, 6)
        result = yield self.client.delete(key3, key1, key2)
        self.assertTrue(result)
