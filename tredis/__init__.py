"""
tredis
======
A pure-python Redis client built for Tornado

"""
import logging
import time

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

__version__ = '0.1.0'

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
    functionality.

    .. code:: python

        client = tredis.RedisClient()
        yield client.connect()

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
        self._default_db = int(db or DEFAULT_DB)
        self._host = host
        self._port = port
        self._ioloop = ioloop.IOLoop.current()
        self._on_close = on_close
        self._pipeline = False
        self._pipeline_commands = []
        self._pool = []
        super(RedisClient, self).__init__()

    def close(self):
        """Close all of the pooled connections to the Redis Server"""
        for conn in self._pool:
            if not conn.closed:
                conn.close()
        self._pool = []

    def pipeline_start(self):
        """Start a command pipeline. The pipeline will only run when you invoke
        :py:meth:`pipeline_execute <tredis.RedisClient.pipeline_execute`.

        **Example:**

        .. code::

            # Start the pipeline
            client.pipeline_start()

            client.set('foo1', 'bar1')
            client.set('foo2', 'bar2')
            client.set('foo3', 'bar3')

            # Execute the pipeline
            responses = yield client.pipeline_execute()

        .. note:: While the client sends commands using pipelining, the server
           will be forced to queue the replies, using memory. So if you need
           to send a lot of commands with pipelining, it is better to send
           them as batches having a reasonable number, for instance 10k
           commands, read the replies, and then send another 10k commands
           again, and so forth. The speed will be nearly the same, but the
           additional memory used will be at max the amount needed to queue the
           replies for this 10k commands.

        """
        self._pipeline = True
        self._pipeline_commands = []

    def pipeline_execute(self):
        """Execute the pipeline created by issuing commands after invoking
        :py:meth:`pipeline_start <tredis.RedisClient.pipeline_start`.

        :raises: ValueError

        """
        commands = len(self._pipeline_commands)
        if not commands:
            raise ValueError('Empty pipeline')

        future = concurrent.TracebackFuture()
        conn_future = self._get_connection()

        def on_connected(response):
            exc = response.exception()
            if exc:
                future.set_exception(exceptions.ConnectError(str(exc)))
                return

            conn = response.result()

            def on_complete(pipeline_response):
                conn.release()
                err = response.exception()
                if err:
                    future.set_exception(err)
                    return
                future.set_result(pipeline_response.result())
            response_future = self._pipeline_execute(conn)
            self._ioloop.add_future(response_future, on_complete)
        self._ioloop.add_future(conn_future, on_connected)
        return future

    def _build_command(self, parts):
        """Build the command that will be written to Redis via the socket

        :param list parts: The list of strings for building the command
        :type: bytes

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

    def _execute(self, parts, callback=None):
        """Execute a Redis command.

        :param list parts: The list of command parts
        :param method callback: The optional method to invoke when complete
        :rtype: :py:class:`tornado.concurrent.Future`

        """
        if self._pipeline:
            return self._pipeline_add(parts)
        return self._execute1(parts, callback)

    def _execute1(self, parts, callback=None):
        """Really execute a redis command

        :param list parts: The list of command parts
        :param method callback: The optional method to invoke when complete
        :rtype: :py:class:`tornado.concurrent.Future`

        """
        command = self._build_command(parts)

        future = concurrent.TracebackFuture()
        if callback:
            self._ioloop.add_future(future, callback)

        connection_future = self._get_connection()

        def on_connected(response):
            exc = response.exception()
            if exc:
                LOGGER.debug('Connection exception: %r', exc)
                future.set_exception(exc)
                return

            conn = response.result()
            LOGGER.debug('Connected: %r', conn)

            def on_complete(command_response):
                err = command_response.exception()
                if err:
                    LOGGER.debug('_execute_command exception: %r', err)
                    future.set_exception(err)
                else:
                    LOGGER.debug('_execute_command complete')
                    future.set_result(command_response.result())

            execute_future = self._execute_command(conn, command)
            self._ioloop.add_future(execute_future, on_complete)

        self._ioloop.add_future(connection_future, on_connected)
        return future

    def _execute_command(self, conn, command):
        """Execute a Redis command.

        :param tredis.Connection conn: The connection to use
        :param bytes command: The command to run
        :rtype: :py:class:`tornado.concurrent.Future`

        """
        LOGGER.debug('Executing command %r on %r', command, conn)

        future = concurrent.TracebackFuture()

        def on_response(response):
            """Process the response future

            :param response: The response future
            :type response: :py:class:`tornado.concurrent.Future`

            """
            conn.release()
            exc = response.exception()
            if exc:
                LOGGER.debug('Error calling get_response: %r', exc)
                future.set_exception(exc)
            else:
                LOGGER.debug('get_response result: %r', response.result())
                future.set_result(response.result())

        def on_written():
            """Invoked when the command has been written to the socket"""
            LOGGER.debug('Fetching response: %r', conn)
            response_future = self._get_response(conn)
            self._ioloop.add_future(response_future, on_response)

        conn.write(command, callback=on_written)
        return future

    def _execute_and_eval_int_resp(self, parts):
        """Execute a command returning a boolean based upon the response.

        :param list parts: The command parts
        :rtype: bool

        """
        if self._pipeline:
            return self._pipeline_add(parts, self._pipeline_int_is_1)

        future = concurrent.TracebackFuture()

        def on_response(response):
            """Process the response future

            :param response: The response future
            :type response: :py:class:`tornado.concurrent.Future`

            """
            exc = response.exception()
            if exc:
                future.set_exception(exc)
            else:
                LOGGER.debug('%r == %r', parts, response.result())
                future.set_result(response.result() == 1)

        execute_future = self._execute1(parts)
        self._ioloop.add_future(execute_future, on_response)
        return future

    def _execute_and_eval_ok_resp(self, parts):
        """Execute a command returning a boolean based upon the response.

        :param list parts: The command parts
        :rtype: bool

        """
        if self._pipeline:
            return self._pipeline_add(parts, self._pipeline_is_ok)

        future = concurrent.TracebackFuture()

        def on_response(response):
            """Process the response future

            :param response: The response future
            :type response: :py:class:`tornado.concurrent.Future`

            """
            exc = response.exception()
            if exc:
                future.set_exception(exc)
            else:
                future.set_result(response.result() == b'OK')

        execute_future = self._execute1(parts)
        self._ioloop.add_future(execute_future, on_response)
        return future

    def _get_array(self, conn, segments, callback):
        """Read in an array by calling :py:meth:`RedisClient._get_response`
        for each element in the array.

        :param tredis.Connection conn: The connection to use
        :param int segments: The number of segments to read
        :param method callback: The method to call when the array is complete

        """
        future = concurrent.TracebackFuture()
        self._ioloop.add_future(future, callback)
        ns = _RESPArrayNamespace()
        ns.depth = int(segments)

        def on_response(response):
            """Process the response data

            :param response: The future with the response
            :type response: :py:class:`tornado.concurrent.Future`

            """
            ns.values.append(response.result())
            ns.depth -= 1
            if not ns.depth:
                future.set_result(ns.values)
            else:
                nested_future = self._get_response(conn)
                self._ioloop.add_future(nested_future, on_response)

        response_future = self._get_response(conn)
        self._ioloop.add_future(response_future, on_response)
        return future

    def _get_connection(self):
        """Get and lock an established connection from the connection pool,
        adding a new one if it does not exist, connecting the first unused
        connection in the pool, if it exists.

        :rtype: :py:class:`tredis.Connection`
        :raises: :py:class:`ConnectError <tredis.ConnectError>`
                 :py:class:`RedisError <tredis.RedisError>`

        """
        future = concurrent.TracebackFuture()

        connections = self._idle_connections()
        if not connections:
            LOGGER.debug('Creating a new connection')
            conn = Connection(self._host, self._port, self._on_closed)
            self._pool.append(conn)

            def on_connect(response):
                """Invoked when the socket stream has connected

                :param response: The connection response future
                :type response: :py:class:`tornado.concurrent.Future`

                """
                exc = response.exception()
                if exc:
                    LOGGER.error('Error connecting to %s:%s: %s',
                                 self._host, self._port, exc)
                    future.set_exception(exceptions.ConnectError(str(exc)))
                else:
                    conn.lock()
                    if self._default_db:

                        def on_selected(selected_response):
                            err = selected_response.exception()
                            if err:
                                future.set_exception(err)
                                return
                            future.set_result(conn)
                        cmd = self._build_command(['SELECT',
                                                   ascii(self._default_db)])
                        select_future = self._execute_command(conn, cmd)
                        self._ioloop.add_future(select_future, on_selected)
                    else:
                        future.set_result(conn)

            connect_future = conn.connect()
            self._ioloop.add_future(connect_future, on_connect)
            return future

        connections[0].lock()
        future.set_result(connections[0])
        return future

    def _get_response(self, conn):
        """Read and parse command execution responses from Redis

        :param tredis.Connection conn: The connection to use
        :rtype: :py:class:`tornado.concurrent.Future`

        """
        future = concurrent.TracebackFuture()

        def on_first_byte(first_byte):
            """Process the first byte response data

            :param first_byte: The byte indicating the RESP data type
            :type first_byte: bytes

            """
            if first_byte == b'+':

                def on_response(response):
                    """Process the response data

                    :param response: The response data
                    :type response: bytes

                    """
                    future.set_result(response[0:-2])

                conn.read_until(CRLF, on_response)
            elif first_byte == b'-':

                def on_response(response):  # pragma: nocover
                    """Process the response data

                    :param response: The response data
                    :type response: bytes

                    """
                    error = response[0:-2].decode('utf-8')
                    if error.startswith('ERR'):
                        error = error[4:]
                    future.set_exception(exceptions.RedisError(error))

                conn.read_until(CRLF, callback=on_response)
            elif first_byte == b':':

                def on_response(response):
                    """Process the response data

                    :param response: The response data
                    :type response: bytes

                    """
                    future.set_result(int(response[:-2]))

                conn.read_until(CRLF, callback=on_response)
            elif first_byte == b'$':

                def on_payload(response):
                    """Process the response data

                    :param response: The response data
                    :type response: bytes

                    """
                    future.set_result(response[:-2])

                def on_response(response):
                    """Process the response data

                    :param response: The response data
                    :type response: bytes

                    """
                    if response == b'-1\r\n':
                        future.set_result(None)
                    else:
                        conn.read_bytes(int(response.strip()) + 2, on_payload)

                conn.read_until(CRLF, callback=on_response)
            elif first_byte == b'*':

                def on_complete(response):
                    """Process the response data

                    :param response: The future with the response
                    :type response: :py:class:`tornado.concurrent.Future`

                    """
                    future.set_result(response.result())

                def on_segments(response):
                    """Process the response segment data

                    :param response: The response array segment data
                    :type response: int

                    """
                    self._get_array(conn, response, on_complete)

                conn.read_until(CRLF, callback=on_segments)

            else:  # pragma: nocover
                future.set_exception(ValueError(
                    'Unknown RESP first-byte: {}'.format(first_byte)))

        conn.read_bytes(1, callback=on_first_byte)
        return future

    def _idle_connections(self):
        """Return a list of idle :py:class:`tredis.Connection` objects.

        :rtype: list

        """
        return [c for c in sorted(self._pool,
                                  key=lambda x: x.idle,
                                  reverse=True) if not c.locked]

    def _on_closed(self, conn):
        """Invoked when the connection is closed"""
        LOGGER.error('Connection closed: %r', conn)
        if self._on_close:
            self._on_close()

    def _pipeline_add(self, parts, response_processor=None):
        """Add a command to execute to the pipeline

        :param list parts: The command parts
        :param method response_processor: A method for command response eval

        """
        self._pipeline_commands.append((self._build_command(parts),
                                        response_processor))

    def _pipeline_execute(self, conn):

        pipeline_responses = _PipelineResponses()
        future = concurrent.TracebackFuture()

        def on_response(response):
            """Process the response future

            :param response: The response future
            :type response: :py:class:`tornado.concurrent.Future`

            """
            exc = response.exception()
            if exc:
                pipeline_responses.append(exc)
            else:
                index = len(pipeline_responses.values)
                method = self._pipeline_commands[index][1]
                if method:
                    pipeline_responses.append(method(response.result()))
                else:
                    pipeline_responses.append(response.result())

            if len(pipeline_responses) == len(self._pipeline_commands):
                self._pipeline = False
                self._pipeline_commands = []
                future.set_result(pipeline_responses.values)
            else:
                response_future = self._get_response(conn)
                self._ioloop.add_future(response_future, on_response)

        def on_written():
            """Invoked when the command has been written to the socket"""
            response = self._get_response(conn)
            self._ioloop.add_future(response, on_response)

        pipeline = b''.join([cmd[0] for cmd in self._pipeline_commands])
        conn.write(pipeline, callback=on_written)
        return future

    @staticmethod
    def _pipeline_int_is_1(value):
        """Method invoked when evaluating a pipeline response looking for
        the value of 1

        :param int value: The Redis response to evaluate
        :rtype: bool

        """
        return value == 1

    @staticmethod
    def _pipeline_is_ok(value):
        """Method invoked when evaluating a pipeline response for b'OK'

        :param bytes value: The Redis response to evaluate
        :rtype: bool

        """
        return value == b'OK'


class Connection(object):
    """A Wrapper to the IOStream for the Connection for use by the RedisClient
    connection pool.

    :param str host: The host to connect to
    :param int port: The port to connect on
    :param method on_close: The method to call if the socket closes

    """
    def __init__(self, host, port, on_close):
        self._client = tcpclient.TCPClient()
        self._host = host
        self._port = port
        self._ioloop = ioloop.IOLoop.current()
        self._last_command = time.time()
        self._locked = False
        self._on_close = on_close
        self._stream = None

    def connect(self):
        """Connect to the Redis server, selecting the specified database.

        :param method callback: The method to invoke when connected
        :raises: :py:class:`ConnectError <tredis.ConnectError>`
                 :py:class:`RedisError <tredis.RedisError>`

        """
        LOGGER.debug('Connecting to %s:%i (%s)',
                     self._host, self._port, id(self))
        future = concurrent.TracebackFuture()

        def on_connect(response):
            """Invoked when the socket stream has connected

            :param response: The connection response future
            :type response: :py:class:`tornado.concurrent.Future`

            """
            exc = response.exception()
            if exc:
                future.set_exception(exceptions.ConnectError(str(exc)))
            else:
                self._stream = response.result()
                self._stream.set_close_callback(self.on_closed)
                future.set_result(True)

        connect_future = self._client.connect(self._host, self._port)
        self._ioloop.add_future(connect_future, on_connect)
        return future

    def close(self):
        """Close the connection to the Redis Server"""
        self._stream.close()

    @property
    def closed(self):
        """Returns :py:data:`True` if the stream is closed.

        :rtype: bool

        """
        return self._stream.closed()

    def lock(self):
        """Lock the connection"""
        if self._locked:
            raise ValueError('Already locked')
        self._locked = True

    @property
    def locked(self):
        """Returns :py:data:`True` if the connection is locked.

        :rtype: bool

        """
        return self._locked

    @property
    def idle(self):
        """Return the time in seconds that the connection has been idle

        :rtype: int

        """
        return time.time() - (self._last_command or 0)

    def on_closed(self):
        """Invoked when the connection is closed"""
        LOGGER.error('Connection closed (%r)', self)
        if self._on_close:
            self._on_close(self)

    def read_bytes(self, num_bytes, callback=None, partial=False):
        """Asynchronously read a number of bytes.

        :param num_bytes: # of bytes to read
        :param method callback: The method to call when the read is done
        :param bool partial: Return with any bytes <= num_bytes
        :rtype: tornado.concurrent.Future

        """
        self._last_command = time.time()
        self._stream.read_bytes(num_bytes=num_bytes, callback=callback,
                                partial=partial)

    def read_until(self, delimiter, callback=None, max_bytes=None):
        """Asynchronously read until we have found the given delimiter.

        :param bytes delimiter: The delimiter to read until
        :param method callback: The method to call when the read is done
        :param int max_bytes: Maximum # of bytes to read
        :rtype: tornado.concurrent.Future

        """
        self._last_command = time.time()
        return self._stream.read_until(delimiter=delimiter,
                                       callback=callback,
                                       max_bytes=max_bytes)

    def release(self):
        """Unlock the connection"""
        #if not self._locked:
        #    raise ValueError('Node is unlocked')
        self._locked = False

    def write(self, data, callback=None):
        """Asynchronously write the given data to this stream.

        :param bytes data: The data to write to the stream
        :param method callback: The method to call when the data is written

        """
        LOGGER.debug('Writing data: %r', data)
        self._last_command = time.time()
        return self._stream.write(data, callback)


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


class _RESPArrayNamespace(object):
    """Class for dealing with recursive async calls"""

    def __init__(self):
        self.depth = 0
        self.values = []
