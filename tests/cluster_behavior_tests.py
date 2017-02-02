import os

from tornado import testing

from . import base


class ClusterBehaviorTests(base.AsyncTestCase):

    CLUSTERING = True

    @property
    def redis_port(self):
        return int(os.environ['NODE1_PORT'])

    def test_connection_move(self):
        pass
