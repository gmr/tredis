import os

from tornado import testing

from . import base


class ClusterTests(base.AsyncTestCase):

    CLUSTERING = True

    @property
    def redis_port(self):
        return int(os.environ['NODE1_PORT'])

    @testing.gen_test()
    def test_cluster_nodes_against_cluster(self):
        expectation = sorted([
            (os.environ['NODE1_IP'], int(os.environ['NODE1_PORT'])),
            (os.environ['NODE2_IP'], int(os.environ['NODE2_PORT'])),
            (os.environ['NODE3_IP'], int(os.environ['NODE3_PORT'])),
        ])
        results = yield self.client.cluster_nodes()
        values = []
        for node in results:
            values.append((node.ip, node.port))
        self.assertListEqual(sorted(values), expectation)
