import os

from tornado import testing

from . import base


class ClusterTests(base.AsyncTestCase):

    @property
    def redis_port(self):
        return int(os.environ['NODE4_PORT'])

    @testing.gen_test()
    def test_cluster_nodes_against_cluster(self):
        results = yield self.client.cluster_nodes()
        self.assertEquals(len(results), 3)

