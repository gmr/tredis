from tornado import testing

from tredis import exceptions

from . import base


TEST_SCRIPT = """\
return redis.call("set", KEYS[1], ARGV[1])
"""


class ScriptingTests(base.AsyncTestCase):

    @testing.gen_test
    def test_eval(self):
        key, value = self.uuid4(2)
        result = yield self.client.eval(TEST_SCRIPT, [key], [value])
        self.assertTrue(result)
        result = yield self.client.get(key)
        self.assertEqual(value, result)

    @testing.gen_test
    def test_load_and_evalsha(self):
        sha1 = yield self.client.script_load(TEST_SCRIPT)
        key, value = self.uuid4(2)
        result = yield self.client.evalsha(sha1, [key], [value])
        self.assertTrue(result)
        result = yield self.client.get(key)
        self.assertEqual(value, result)

    @testing.gen_test
    def test_load_exists_and_flush(self):
        sha1 = yield self.client.script_load(TEST_SCRIPT)
        result = yield self.client.script_exists(sha1)
        self.assertListEqual(result, [1])
        result = yield self.client.script_flush()
        self.assertTrue(result)
        result = yield self.client.script_exists(sha1)
        self.assertListEqual(result, [0])

    @testing.gen_test
    def test_kill(self):
        with self.assertRaises(exceptions.RedisError):
            result = yield self.client.script_kill()
            self.assertFalse(result)
