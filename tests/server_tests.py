"""

"""
import mock

from tornado import testing

from tredis import exceptions

from . import base


class ServerTests(base.AsyncTestCase):

    @testing.gen_test
    def test_auth_raises_redis_error(self):
        yield self.client.connect()
        with self.assertRaises(exceptions.RedisError):
            yield self.client.auth('boom-goes-the-silver-nitrate')

    @testing.gen_test
    def test_auth_raises_auth_error(self):
        yield self.client.connect()
        self._execute_result = exceptions.RedisError(b'invalid password')
        with mock.patch.object(self.client, '_execute', self._execute):
            with self.assertRaises(exceptions.AuthError):
                yield self.client.auth('boom-goes-the-silver-nitrate')

    @testing.gen_test
    def test_auth_returns_true(self):
        yield self.client.connect()
        self._execute_result = b'OK'
        with mock.patch.object(self.client, '_execute', self._execute):
            result = yield self.client.auth('password')
            self.assertTrue(result)

    @testing.gen_test
    def test_echo_response(self):
        yield self.client.connect()
        value = b'echo-test'
        result = yield self.client.echo(value)
        self.assertEqual(result, value)

    @testing.gen_test
    def test_ping_response(self):
        yield self.client.connect()
        result = yield self.client.ping()
        self.assertEqual(result, b'PONG')

    @testing.gen_test
    def test_quit_response(self):
        yield self.client.connect()
        result = yield self.client.quit()
        self.assertTrue(result)

    @testing.gen_test
    def test_select_response(self):
        yield self.client.connect()
        result = yield self.client.select(1)
        self.assertTrue(result)
