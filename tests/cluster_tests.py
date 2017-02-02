import os

from tornado import gen, testing

from . import base


class ClusterTests(base.AsyncTestCase):

    CLUSTERING = True

    @property
    def redis_port(self):
        return int(os.environ['NODE1_PORT'])

    @testing.gen_test()
    def test_cluster_nodes_against_cluster(self):
        while not self.client.ready:
            yield gen.sleep(0.01)
        expectation = sorted([
            (os.environ['REDIS_HOST'], int(os.environ['NODE1_PORT'])),
            (os.environ['REDIS_HOST'], int(os.environ['NODE2_PORT'])),
            (os.environ['REDIS_HOST'], int(os.environ['NODE3_PORT'])),
        ])
        results = yield self.client.cluster_nodes()
        values = []
        for node in results:
            values.append((node.ip, node.port))
        self.assertListEqual(sorted(values), expectation)
