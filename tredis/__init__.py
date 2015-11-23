"""
TRedis
======
An asynchronous Redis client for Tornado

"""
import logging

import hiredis
from tornado import concurrent
from tornado import ioloop
from tornado import locks
from tornado import iostream
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

from tredis.strings import BITOP_AND, BITOP_OR, BITOP_XOR, BITOP_NOT

__version__ = '0.2.1'

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


class RedisClient(server.ServerMixin,
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
                  transactions.TransactionsMixin,
                  object):
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
        self._buffer = bytes()
        self._busy = locks.Lock()
        self._client = tcpclient.TCPClient()
        self._connecting = None
        self._default_db = int(db or DEFAULT_DB)
        self._host = host
        self._port = port
        self._ioloop = ioloop.IOLoop.current()
        self._on_close = on_close
        self._pipeline = False
        self._pipeline_commands = []
        self._pool = []
        self._reader = hiredis.Reader()
        self._stream = None
        super(RedisClient, self).__init__()

    def close(self):
        """Close the Redis server connection

        :raises: :class:`~tredis.exceptions.ConnectionError`

        """
        if not self._stream:
            raise exceptions.ConnectionError('Not connected')
        self._stream.close()

    def pipeline_start(self):
        """Start a command pipeline. The pipeline will only run when you invoke
        :meth:`~tredis.RedisClient.pipeline_execute`.

        .. code-block:: python
           :caption: Pipeline Example

           # Start the pipeline
           client.pipeline_start()

           client.set('foo1', 'bar1')
           client.set('foo2', 'bar2')
           client.set('foo3', 'bar3')

           # Execute the pipeline
           responses = yield client.pipeline_execute()

        .. warning:: Yielding after calling
           :meth:`~tredis.RedisClient.pipeline_start` and before calling
           :meth:`~tredis.RedisClient.pipeline_execute` can cause asynchronous
           request scope issues, as the client does not protect against other
           asynchronous requests from populating the pipeline. The only way to
           prevent this from happening is to make all pipeline additions inline
           without yielding to the :class:`~tornado.ioloop.IOLoop`.

        .. note:: While the client sends commands using pipelining, the server
           will be forced to queue the replies, using memory. So if you need
           to send a lot of commands with pipelining, it is better to send
           them as batches having a reasonable number, for instance 10k
           commands, read the replies, and then send another 10k commands
           again, and so forth. The speed will be nearly the same, but the
           additional memory used will be at max the amount needed to queue the
           replies for this 10k commands.

        :raises: :exc:`~tredis.exceptions.SubscribedError`

        """
        if hasattr(super(RedisClient, self), 'pipeline_start'):
            super(RedisClient, self).pipeline_start()
        self._pipeline = True
        self._pipeline_commands = []

    def pipeline_execute(self):
        """Execute the pipeline created by issuing commands after invoking
        :meth:`~tredis.RedisClient.pipeline_start`. Returns a list of responses
        from the Redis server

        :rtype: list
        :raises: :exc:`ValueError`, :exc:`~tredis.exceptions.SubscribedError`

        """
        commands = len(self._pipeline_commands)
        if not commands:
            raise ValueError('Empty pipeline')

        if hasattr(super(RedisClient, self), 'pipeline_execute'):
            future = super(RedisClient, self).pipeline_execute() or \
                     concurrent.TracebackFuture()
        else:
            future = concurrent.TracebackFuture()

        pipeline_responses = _PipelineResponses()

        def on_response(response):
            """Process the response future

            :param response: The response future
            :type response: :class:`tornado.concurrent.Future`

            """
            exc = response.exception()
            if exc:
                pipeline_responses.append(exc)
            else:
                pipeline_responses.append(response.result())

            index = len(pipeline_responses.values)

            if index == len(self._pipeline_commands):
                self._pipeline = False
                self._pipeline_commands = []
                future.set_result(pipeline_responses.values)
            else:
                response_future = concurrent.TracebackFuture()
                self._ioloop.add_future(response_future, on_response)
                self._get_response(response_future,
                                   self._pipeline_commands[index][1],
                                   self._pipeline_commands[index][2])

        def on_ready(connection_ready):
            """Invoked once the connection has been established

            :param connection_ready: The connection future
            :type connection_ready: tornado.concurrent.Future

            """
            connection_error = connection_ready.exception()
            if connection_error:
                return future.set_exception(connection_error)

            def on_written():
                """Invoked when the command has been written to the socket"""
                response_future = concurrent.TracebackFuture()
                self._ioloop.add_future(response_future, on_response)
                self._get_response(response_future,
                                   self._pipeline_commands[0][1],
                                   self._pipeline_commands[0][2])

            pipeline = b''.join([cmd[0] for cmd in self._pipeline_commands])
            self._stream.write(pipeline, callback=on_written)

        def on_locked(lock):
            """Invoked once the lock has been acquired.

            :param tornado.concurrent.Future lock: The lock future

            """
            LOGGER.debug('Executing pipeline with lock %r', lock)
            self._maybe_connect(on_ready)

        # Start executing once locked
        lock_future = self._busy.acquire()
        self._ioloop.add_future(lock_future, on_locked)

        # Release the lock when the future is complete
        self._ioloop.add_future(future, lambda r: self._busy.release())
        return future

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
        if self._pipeline:
            return self._pipeline_add(command, expectation, format_callback)

        if hasattr(super(RedisClient, self), '_execute'):
            future = super(RedisClient,
                           self)._execute(parts, expectation=None,
                                          format_callback=None) or \
                     concurrent.TracebackFuture()
        else:
            future = concurrent.TracebackFuture()

        def on_ready(connection_ready):
            """Invoked once the connection has been established

            :param connection_ready: The connection future
            :type connection_ready: tornado.concurrent.Future

            """
            connection_error = connection_ready.exception()
            if connection_error:
                return future.set_exception(connection_error)

            def on_written():
                """Invoked when the command has been written to the socket"""
                self._get_response(future, expectation, format_callback)

            try:
                self._stream.write(command, callback=on_written)
            except iostream.StreamClosedError as error:
                future.set_exception(exceptions.ConnectionError(error))

        def on_locked(lock):
            """Invoked once the lock has been acquired.

            :param tornado.concurrent.Future lock: The lock future

            """
            LOGGER.debug('Executing %r (%r) with lock %r',
                         command, expectation, lock)
            self._maybe_connect(on_ready)

        # Start executing once locked
        lock_future = self._busy.acquire()
        self._ioloop.add_future(lock_future, on_locked)

        # Release the lock when the future is complete
        self._ioloop.add_future(future, lambda r: self._busy.release())
        return future

    def _get_response(self, future, expectation=None, format_callback=None):
        """Read and parse command execution responses from Redis

        :param future: The future for the possible response
        :type future: :class:`~tornado.concurrent.Future`
        :param mixed expectation: An optional response expectation

        """

        def on_data(data):
            LOGGER.debug('Read %r', data)
            self._reader.feed(data)
            self._get_response(future, expectation, format_callback)

        response = self._reader.gets()
        if response is not False:
            if isinstance(response, hiredis.ReplyError):
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
            self._read(on_data)

    def _maybe_connect(self, callback):
        """Connect to the Redis server, selecting the specified database.

        :raises: :class:`~tredis.exceptions.ConnectError`
                 :class:`~tredis.exceptions..RedisError`

        """
        future = concurrent.TracebackFuture()
        self._ioloop.add_future(future, callback)

        if self._stream:
            return future.set_result(True)

        LOGGER.info('Connecting to %s:%i', self._host, self._port)
        connect_future = self._client.connect(self._host, self._port)

        def on_selected(response):
            """Invoked when the default database is selected when connecting

            :param response: The connection response future
            :type response: :class:`~tornado.concurrent.Future`

            """
            exc = response.exception()
            if exc:
                future.set_exception(exceptions.RedisError(exc))
            else:
                future.set_result(response.result == b'OK')

        def on_connect(response):
            """Invoked when the socket stream has connected

            :param response: The connection response future
            :type response: :class:`~tornado.concurrent.Future`

            """
            exc = response.exception()
            if exc:
                return future.set_exception(exceptions.ConnectError(str(exc)))

            self._stream = response.result()
            self._stream.set_close_callback(self._on_closed)
            if not self._default_db:
                return future.set_result(True)

            def on_written():
                select_future = concurrent.TracebackFuture()
                self._get_response(select_future)
                self._ioloop.add_future(select_future, on_selected)

            LOGGER.debug('Selecting the default db: %r', self._default_db)
            command = self._build_command(['SELECT', ascii(self._default_db)])
            self._stream.write(command, on_written)

        self._ioloop.add_future(connect_future, on_connect)

    def _on_closed(self):
        """Invoked when the connection is closed"""
        LOGGER.error('Redis connection closed')
        if self._on_close:
            LOGGER.debug('Calling on_close callback: %r', self._on_close)
            self._on_close()

    def _pipeline_add(self, command, expectation, format_callback):
        """Add a command to execute to the pipeline

        :param bytes command: The command to execute
        :param mixed expectation: Expectation for response evaluation

        """
        LOGGER.debug('Adding to pipeline (%r, %r)' % (command, expectation))
        self._pipeline_commands.append((command, expectation, format_callback))

    def _read(self, callback=None):
        """Asynchronously read a number of bytes.

        :param method callback: The method to call when the read is done

        """
        LOGGER.debug('Reading from the stream')
        self._stream.read_bytes(65536, callback, None, True)


class _PipelineResponses(object):
    """Class for returning pipeline responses"""

    def __init__(self):
        self.values = []

    def __len__(self):
        """Return the length of the pipeline responses

        :rtype: int

        """
        return len(self.values)

    def append(self, value):
        """Add a value to the pipeline response list

        :param mixed value: The value to append

        """
        self.values.append(value)
