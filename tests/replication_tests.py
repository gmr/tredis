import os

from tornado import testing

from . import base


class ReplicationTests(base.AsyncTestCase):

    def setUp(self):
        super(ReplicationTests, self).setUp()
        self.initial_addr = (self.client.host, self.client.port)
        self.master_addr = (os.environ.get('REDIS_HOST', 'localhost'),
                            int(os.environ.get('REDIS_PORT', '6379')))

    @property
    def redis_host(self):
        return os.environ['DOCKER_IP']

    @property
    def redis_port(self):
        return int(os.environ['REDISSLAVE_PORT'])

    @testing.gen_test
    def test_that_hset_writes_to_master(self):
        key, field, value = self.uuid4(3)
        result = yield self.client.hset(key, field, value)
        self.assertEqual(result, 1)
        redis_addr = (self.client.host, self.client.port)
        self.assertNotEqual(redis_addr, self.initial_addr)
