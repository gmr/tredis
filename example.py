import os
import pprint

from tornado import gen, ioloop
import tredis


@gen.coroutine
def run():
    print('Connecting')
    client = tredis.Client([{"host": os.environ['REDIS_HOST'],
                             "port": os.environ['NODE1_PORT'],
                             "db": 0}])
    print('Getting Value')
    value = yield client.cluster_nodes()
    pprint.pprint(value)

    value = yield client.cluster_info()
    pprint.pprint(value)

    ioloop.IOLoop.current().stop()

if __name__ == '__main__':
    io_loop = ioloop.IOLoop.current()
    io_loop.add_timeout(100, run)
    io_loop.start()
