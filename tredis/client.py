"""
Cluster Supporting Redis Client

"""
import logging

import hiredis
from tornado import concurrent
from tornado import ioloop
from tornado import locks
from tornado import iostream
from tornado import tcpclient

from tredis import crc16
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

LOGGER = logging.getLogger(__name__)

CRLF = b'\r\n'

DEFAULT_HOST = 'localhost'
"""The default host to connect to"""

DEFAULT_PORT = 6379
"""The default port to connect to"""

DEFAULT_DB = 0
"""The default database number to use"""


# Python 2 support for ascii()
if 'ascii' not in dir(__builtins__):  # pragma: nocover
    from tredis.compat import ascii


class _Connection(object):
    """Manages the redis TCP connection.

    :param str host: The hostname to connect to
    :param int port: The port to connect on
    :param int db: The database number to use
    :param method on_close: The method to call if the connection is closed

    """

    def __init__(self, host, port, db, on_response, on_close, io_loop):
        super(_Connection, self).__init__()
        self.io_loop = io_loop
        self.host = host
        self.port = port

        self._default_db = int(db or DEFAULT_DB)
        self._client = tcpclient.TCPClient()
        self._stream = None
        self._on_close = on_close
        self._on_response = on_response

    @property
    def name(self):
        return '{}:{}'.format(self.host, self.port)

    def close(self):
        """Close the stream.

        :raises: :class:`tredis.exceptions.ConnectionError` if the
            stream is not currently connected

        """
        if self._stream is None:
            raise exceptions.ConnectionError('Not connected')
        self._stream.close()

    def read_bytes(self, callback):
        """Issue a read on the stream, invoke callback when completed.

        :raises: :class:`tredis.exceptions.ConnectionError` if the
            stream is not currently connected

        """
        self._stream.read_bytes(65536, callback, None, True)

    def write_command(self, command, future, **kwargs):
        """Execute a command after connecting if necessary.

        :param bytes command: command to execute after the connection
            is established
        :param tornado.concurrent.Future future:  future to resolve
            when the command's response is received.
        :keyword expectation: optional response expectation.
        :keyword format_callback: optional callable that is invoked to
            extract the result from the redis response.

        """

        def on_written():
            self._on_response(future, **kwargs)

        def on_ready(f):
            if f.exception():
                return future.set_exception(f.exception())

            try:
                self._stream.write(command, callback=on_written)

            except iostream.StreamClosedError as error:
                future.set_exception(exceptions.ConnectionError(error))

            except Exception as error:
                LOGGER.exception('unhandled write failure - %r', error)
                future.set_exception(exceptions.ConnectionError(error))

        self.connect(on_ready)

    def connect(self, callback):
        """Connect to the Redis server if necessary.

        :raises: :class:`~tredis.exceptions.ConnectError`
                 :class:`~tredis.exceptinos.RedisError`

        """
        future = concurrent.TracebackFuture()
        self.io_loop.add_future(future, callback)

        if self._stream is not None:
            return future.set_result(True)

        LOGGER.debug('Connecting to %s:%i', self.host, self.port)
        connect_future = self._client.connect(self.host, self.port)

        def on_selected(response):
            """Invoked when the default database is selected when connecting

            :param response: the connection response future
            :type response: :class:`~tornado.concurrent.Future`

            """
            exc = response.exception()
            if exc:
                future.set_exception(exc)
            else:
                future.set_result(response.result == b'OK')

        def on_connected(response):
            """Invoked when the socket stream has connected

            :param response: The connection response future
            :type response: :class:`~tornado.concurrent.Future`

            """
            exc = response.exception()
            if exc:
                return future.set_exception(exceptions.ConnectError(exc))

            self._stream = response.result()
            self._stream.set_close_callback(self._on_closed)
            if not self._default_db:
                return future.set_result(True)

            def on_written():
                select_future = concurrent.TracebackFuture()
                self.io_loop.add_future(select_future, on_selected)
                self._on_response(select_future)

            LOGGER.debug('Selecting the default db: %r', self._default_db)
            command = 'SELECT {0}\r\n'.format(ascii(self._default_db))
            self._stream.write(command.encode('ASCII'), on_written)

        self.io_loop.add_future(connect_future, on_connected)

    def _reconnect(self, host, port, callback):
        """Reconnect to a new redis instance.

        :param str host: host to connect to
        :param str|int port: port number to connect to
        :param callback: callable to invoke when the connection
            is finished.

        The *callback* is passed to :meth:`._maybe_connect`

        """
        self.close()
        self._stream = None
        self.host, self.port = host, int(port)
        self.connect(callback)

    def _on_closed(self):
        """Invoked when the connection is closed"""
        LOGGER.error('Redis connection closed')
        self._on_close()
        self._stream = None


class Client(server.ServerMixin,
             keys.KeysMixin,
             strings.StringsMixin,
             geo.GeoMixin,
             hashes.HashesMixin,
             hyperloglog.HyperLogLogMixin,
             lists.ListsMixin,
             sets.SetsMixin,
             sortedsets.SortedSetsMixin,
             pubsub.PubSubMixin,
             connection.ConnectionMixin,
             cluster.ClusterMixin,
             scripting.ScriptingMixin,
             transactions.TransactionsMixin):
    """Base client with common functionality for both the RedisClient and the
    ClusterClient.

    """
    def __init__(self, hosts, on_close=None, io_loop=None, clustering=False):
        self._buffer = bytes()
        self._busy = locks.Lock()
        self._clustering = False
        self._current_command = None
        self._current_database = 0
        self._current_host = None
        self._on_close_callback = on_close
        self._reader = hiredis.Reader()
        self.io_loop = io_loop or ioloop.IOLoop.current()
        self._connections = self._create_connections(hosts)

    def close(self):
        for host in self._connections.keys():
            self._connections[host].close()

    @property
    def _connection(self):
        if not self._current_host:
            hosts = self._connections.keys()
            self._current_host = hosts[0]
            LOGGER.debug('Set current host to %s', self._current_host)
        return self._connections[self._current_host]

    def _create_connections(self, values):
        connections = {}
        for row in values:
            conn = _Connection(
                row['host'], row['port'], row.get('db'),
                self._on_response,
                self._on_close,
                self.io_loop)
            connections[conn.name] = conn
        return connections

    def _build_command(self, parts):
        """Build the command that will be written to Redis via the socket

        :param list parts: The list of strings for building the command
        :rtype: bytes

        """
        return self._encode_resp(parts)

    def _encode_resp(self, value):
        """Dynamically build the RESP payload based upon the list provided.

        :param mixed value: The list of command parts to encode
        :rtype: bytes

        """
        if isinstance(value, bytes):
            return b''.join([b'$', ascii(len(value)).encode('ascii'), CRLF,
                             value, CRLF])
        elif isinstance(value, str):  # pragma: nocover
            return self._encode_resp(value.encode('utf-8'))
        elif isinstance(value, int):
            return self._encode_resp(ascii(value).encode('ascii'))
        elif isinstance(value, float):
            return self._encode_resp(ascii(value).encode('ascii'))
        elif isinstance(value, list):
            output = [b'*', ascii(len(value)).encode('ascii'), CRLF]
            for item in value:
                output.append(self._encode_resp(item))
            return b''.join(output)
        else:
            raise ValueError('Unsupported type: {0}'.format(type(value)))

    def _execute(self, parts, expectation=None, format_callback=None):
        """Really execute a redis command

        :param list parts: The list of command parts
        :param mixed expectation: Optional response expectation

        :rtype: :class:`~tornado.concurrent.Future`
        :raises: :exc:`~tredis.exceptions.SubscribedError`

        """
        LOGGER.debug('_execute (%r, %r, %r)',
                     parts, expectation, format_callback)

        command = self._build_command(parts)
        future = concurrent.TracebackFuture()

        def on_locked(_):
            """Invoked once the lock has been acquired."""
            LOGGER.debug('Executing %r (%r)', command, expectation)
            self._current_command = command
            self._connection.write_command(
                command, future, expectation=expectation,
                format_callback=format_callback)

        # Start executing once locked
        lock_future = self._busy.acquire()
        self.io_loop.add_future(lock_future, on_locked)

        # Release the lock when the future is complete
        self.io_loop.add_future(future, lambda r: self._busy.release())
        return future

    def _on_response(self, future, expectation=None, format_callback=None):

        def after_reconnected(f):
            if f.exception():
                return future.set_exception(f.exception())

            self._connection.write_command(
                self._current_command, future,
                expectation=expectation,
                format_callback=format_callback)

        def on_replication_info(f):
            if f.exception():
                return future.set_exception(f.exception())

            host, port = None, None
            for line in f.result().decode('ASCII').splitlines():
                if line.startswith('master_host'):
                    _, _, host = line.partition(':')
                elif line.startswith('master_port'):
                    _, _, port = line.partition(':')

            if host and port:

                self._connection.close()
                del self._connections[self._current_host]

                master = '{}:{}'.format(host, port)
                if master not in self._connections:
                    self._connections[master] = _Connection(
                        host, int(port),
                        self._current_database,
                        self._on_response,
                        self._on_close,
                        self.io_loop)
                    self._current_host = master
                    self._connection.connect(after_reconnected)
            else:
                future.set_exception(exceptions.ConnectError(
                    'master host or port missing from replication info'))

        response = self._reader.gets()
        if response is not False:
            if isinstance(response, hiredis.ReplyError):
                if response.args[0].startswith('READONLY '):
                    LOGGER.debug('command performed against readonly '
                                 'replica, finding master')
                    new_future = concurrent.TracebackFuture()
                    self.io_loop.add_future(new_future, on_replication_info)
                    self._connection.write_command(
                        b'INFO REPLICATION\r\n', new_future)
                else:
                    future.set_exception(exceptions.RedisError(response))

            elif format_callback is not None:
                future.set_result(format_callback(response))

            elif expectation is not None:
                if isinstance(expectation, int) and expectation > 1:
                    future.set_result(response == expectation or response)
                else:
                    future.set_result(response == expectation)
            else:
                future.set_result(response)

        else:

            def on_data(data):
                LOGGER.debug('Read %r', data)
                self._reader.feed(data)
                self._on_response(future, expectation=expectation,
                                  format_callback=format_callback)

            self._connection.read_bytes(on_data)

    def _on_close(self):
        if self._on_close_callback:
            self._on_close_callback()


class RedisClient(Client):
    """A simple asynchronous Redis client with a subset of overall Redis
    functionality. The client will automatically connect the first time you
    issue a command to the Redis server. The following example demonstrates
    how to set a key in Redis and then retrieve it.

    .. code-block:: python
       :caption: Simple Example

        client = tredis.RedisClient()

        yield client.strings.set('foo', 'bar')
        value = yield client.strings.get('foo')

    :param str host: The hostname to connect to
    :param int port: The port to connect on
    :param int db: The database number to use
    :param method on_close: The method to call if the connection is closed

    """
    def __init__(self,
                 host=DEFAULT_HOST,
                 port=DEFAULT_PORT,
                 db=DEFAULT_DB,
                 on_close=None):
        super(RedisClient, self).__init__([
            {'host': host, 'port': port, 'db': db}], on_close)
