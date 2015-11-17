"""
tredis
======
A pure-python Redis client built for Tornado

"""
import logging

from tornado import concurrent
from tornado import ioloop
from tornado import tcpclient

from tredis import exceptions
from tredis import cluster
from tredis import connection
from tredis import geo
from tredis import hashes
from tredis import hyperloglog
from tredis import keys
from tredis import lists
from tredis import pubsub
from tredis import scripting
from tredis import server
from tredis import sets
from tredis import sortedsets
from tredis import strings
from tredis import transactions
from tredis import utils

__version__ = '0.1.0'

LOGGER = logging.getLogger(__name__)

CRLF = b'\r\n'

# Python 2 support for ascii()
if 'ascii' not in dir(__builtins__):  # pragma: nocover
    ascii = utils.ascii


class _RESPArrayNamespace(object):
    """Class for dealing with recursive async calls"""
    def __init__(self):
        self.depth = 0
        self.values = []


class RedisClient(object):
    """A simple asynchronous Redis client with a subset of overall Redis
    functionality.

    .. code:: python

        client = tredis.RedisClient()
        yield client.connect()

        yield client.strings.set('foo', 'bar')
        value = yield client.strings.get('foo')

    :param str host: The hostname to connect to
    :param int port: The port to connect on
    :param int db: The database number to use
    :param on_close_callback: The method to call if the connection is closed
    :type on_close_callback: method

    """
    DEFAULT_HOST = 'localhost'
    """The default host to connect to"""

    DEFAULT_PORT = 6379
    """The default port to connect to"""

    DEFAULT_DB = 0
    """The default database number to use"""

    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT, db=DEFAULT_DB,
                 on_close_callback=None):
        self._settings = host, port, int(db or self.DEFAULT_DB)
        self._client = tcpclient.TCPClient()
        self._ioloop = ioloop.IOLoop.current()
        self._on_close_callback = on_close_callback
        self._stream = None

        # Command Category Attributes
        self._keys = keys.Keys(self)
        self._server = server.Server(self)
        self._strings = strings.Strings(self)

    def connect(self):
        """Connect to the Redis server, selecting the specified database.

        :rtype: bool
        :raises: :py:class:`ConnectError <tredis.ConnectError>`
                 :py:class:`RedisError <tredis.RedisError>`

        """
        future = concurrent.TracebackFuture()

        def on_connect(response):
            exc = response.exception()
            if exc:
                future.set_exception(exceptions.ConnectError(str(exc)))
            else:
                self._stream = response.result()
                self._stream.set_close_callback(self._on_closed)
                if self._settings[2]:
                    self.execute([b'SELECT',
                                  ascii(self._settings[2]).encode('ascii')],
                                 lambda resp: utils.is_ok(resp, future))
                else:
                    future.set_result(True)

        connect_future = self._client.connect(self._settings[0],
                                              self._settings[1])
        self._ioloop.add_future(connect_future, on_connect)
        return future

    def close(self):
        """Close the connection to the Redis Server"""
        self._stream.close()

    def execute(self, parts, callback=None):
        """Execute a Redis command.

        :param list parts: The list of command parts
        :param method callback: The optional method to invoke when complete
        :rtype: :py:class:`tornado.concurrent.Future`

        """
        future = concurrent.TracebackFuture()
        self._ioloop.add_future(future, callback)

        def on_response(response):
            exc = response.exception()
            if exc:
                future.set_exception(exc)
            else:
                result = response.result()
                future.set_result(result)

        def on_written():
            self._get_response(on_response)

        self._stream.write(self._build_command(parts), callback=on_written)
        return future

    @staticmethod
    def _build_command(parts):
        """Build the command that will be written to Redis via the socket

        :param list parts: The list of strings for building the command
        :type: bytes

        """
        parts = [part.encode('utf-8') if isinstance(part, str) else part
                 for part in parts]
        command = bytearray(b'*') + ascii(len(parts)).encode('ascii') + CRLF
        for part in parts:
            command += b'$' + ascii(len(part)).encode('ascii') + CRLF
            command += part + CRLF
        return bytes(command)

    def _get_response(self, callback):
        """Read and parse command execution responses from Redis

        :param method callback: The method to receive the response data
        :rtype: :py:class:`tornado.concurrent.Future`

        """
        future = concurrent.TracebackFuture()
        self._ioloop.add_future(future, callback)

        def on_first_byte(first_byte):
            if first_byte == b'+':
                def on_response(response):
                    future.set_result(response[0:-2])
                self._stream.read_until(CRLF, on_response)
            elif first_byte == b'-':
                def on_response(response):  # pragma: nocover
                    error = response[0:-2].decode('utf-8')
                    if error.startswith('ERR'):
                        error = error[4:]
                    future.set_exception(exceptions.RedisError(error))
                self._stream.read_until(CRLF, callback=on_response)
            elif first_byte == b':':
                def on_response(response):
                    future.set_result(int(response[:-2]))
                self._stream.read_until(CRLF, callback=on_response)
            elif first_byte == b'$':
                def on_payload(data):
                    future.set_result(data[:-2])

                def on_response(size):
                    if size == b'-1\r\n':
                        future.set_result(None)
                    else:
                        self._stream.read_bytes(int(size.strip()) + 2,
                                                on_payload)
                self._stream.read_until(CRLF, callback=on_response)
            elif first_byte == b'*':
                def on_complete(values):
                    future.set_result(values.result())

                def on_segments(segments):
                    self._read_array(segments, on_complete)

                self._stream.read_until(CRLF, callback=on_segments)

            else:  # pragma: nocover
                future.set_exception(ValueError(
                    'Unknown RESP first-byte: {}'.format(first_byte)))

        self._stream.read_bytes(1, callback=on_first_byte)

    def _read_array(self, segments, callback):
        """Read in an array by calling :py:meth:`RedisClient._get_response`
        for each element in the array.

        :param int segments: The number of segments to read
        :param method callback: The method to call when the array is complete

        """
        future = concurrent.TracebackFuture()
        self._ioloop.add_future(future, callback)
        ns = _RESPArrayNamespace()
        ns.depth = int(segments)

        def on_result(data):
            ns.values.append(data.result())
            ns.depth -= 1
            if not ns.depth:
                future.set_result(ns.values)
            else:
                self._get_response(on_result)
        self._get_response(on_result)

    def _on_closed(self):
        """Invoked when the connection is closed"""
        LOGGER.error('Connection closed')
        if self._on_close_callback:
            self._on_close_callback()
