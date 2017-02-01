import contextlib
import os
import logging
import socket
import time
import uuid

from tornado import concurrent
from tornado import testing

import tredis

# os.environ['ASYNC_TEST_TIMEOUT'] = '10'


class AsyncTestCase(testing.AsyncTestCase):

    CLUSTERING = False
    DEFAULT_EXPIRATION = 5

    def setUp(self):
        super(AsyncTestCase, self).setUp()
        self.client = tredis.Client(
            [{'host': self.redis_host,
              'port': self.redis_port,
              'db': self.redis_db}],
            clustering=self.CLUSTERING)
        self._execute_result = None

    def tearDown(self):
        try:
            self.client.close()
        except tredis.ConnectionError:
            pass

    def reset_slave_relationship(self):
        logging.debug('Resetting slave relationship')
        self.disable_slave()
        time.sleep(0.5)
        self.enable_slave()
        time.sleep(0.5)

    @staticmethod
    def disable_slave():
        logging.debug('Disabling slave mode on node2')
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM,
                          socket.IPPROTO_TCP)
        with contextlib.closing(s):
            sockaddr = (os.environ['REDIS_HOST'], int(os.environ['NODE2_PORT']))
            logging.debug('making %r a slave of NODE1', sockaddr)
            s.connect(sockaddr)
            s.send('SLAVEOF NO ONE\r\n'.encode('ASCII'))

    @staticmethod
    def enable_slave():
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM,
                          socket.IPPROTO_TCP)
        with contextlib.closing(s):
            sockaddr = (os.environ['REDIS_HOST'], int(os.environ['NODE2_PORT']))
            logging.debug('making %r a slave of NODE1', sockaddr)
            s.connect(sockaddr)
            s.send('SLAVEOF {0} {1}\r\n'.format(
                os.environ['REDIS_HOST'],
                os.environ['NODE1_PORT']).encode('ASCII'))

    @property
    def redis_host(self):
        return os.environ.get('REDIS_HOST', 'localhost')

    @property
    def redis_port(self):
        return int(os.environ.get('NODE1_PORT', '6379'))

    @property
    def redis_db(self):
        return int(os.environ.get('REDIS_DB', '12'))

    def expiring_set(self, key, value, expiration=None, nx=None, xx=None):
        return self.client.set(key, value,
                               expiration or self.DEFAULT_EXPIRATION,
                               nx=nx, xx=xx)

    def _execute(self, parts, expectation=None, format_callback=None):
        future = concurrent.Future()
        if isinstance(self._execute_result, Exception):
            future.set_exception(self._execute_result)
        else:
            future.set_result(self._execute_result)
        return future

    def uuid4(self, qty=1):
        if qty == 1:
            return str(uuid.uuid4()).encode('ascii')
        else:
            return tuple([str(uuid.uuid4()).encode('ascii')
                          for i in range(0, qty)])
