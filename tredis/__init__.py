"""
tredis
======
A pure-python Redis client built for Tornado

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
        """Close the Redis server connection"""
        if self._stream:
            self._stream.close()

    def pipeline_start(self):
        """Start a command pipeline. The pipeline will only run when you invoke
        :py:meth:`pipeline_execute <tredis.RedisClient.pipeline_execute>`.

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
           :py:meth:`pipeline_start <tredis.RedisClient.pipeline_start>` and
           before calling
           :py:meth:`pipeline_execute <tredis.RedisClient.pipeline_execute>`
           can cause asynchronous request scope issues, as the client does not
           protect against other asynchronous requests from populating the
           pipeline. The only way to prevent this from happening is to make
           all pipeline additions inline without yielding to the
           :py:class:`IOLoop <tornado.ioloop.IOLoop>`.

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
        :py:meth:`pipeline_start <tredis.RedisClient.pipeline_start>`. Returns
        a list of Redis responses.

        :rtype: list
        :raises: ValueError

        """
        commands = len(self._pipeline_commands)
        if not commands:
            raise ValueError('Empty pipeline')

        future = concurrent.TracebackFuture()
        pipeline_responses = _PipelineResponses()

        def on_response(response):
            """Process the response future

            :param response: The response future
            :type response: :py:class:`tornado.concurrent.Future`

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

        self._maybe_connect(on_ready)
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

    def _execute(self, parts, expectation=None, format_callback=None):
        """Really execute a redis command

        :param list parts: The list of command parts
        :param mixed expectation: Optional response expectation

        :rtype: :py:class:`tornado.concurrent.Future`

        """
        LOGGER.debug('_execute (%r, %r)', expectation, format_callback)
        command = self._build_command(parts)
        if self._pipeline:
            return self._pipeline_add(command, expectation, format_callback)

        future = concurrent.TracebackFuture()
        LOGGER.debug('Executing %r (%r)', command, expectation)

        def on_ready(connection_ready):
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

        self._maybe_connect(on_ready)
        return future

    def _get_response(self, future, expectation=None, format_callback=None):
        """Read and parse command execution responses from Redis

        :param future: The future for the possible response
        :type future: :py:class:`tornado.concurrent.Future`
        :param mixed expectation: An optional response expectation

        """

        def on_data(data):
            self._reader.feed(data)
            self._get_response(future, expectation, format_callback)

        response = self._reader.gets()
        if response is not False:

            LOGGER.debug('Processing response (%r, %r, %r)', future, expectation, format_callback)

            if isinstance(response, hiredis.ReplyError):
                return future.set_exception(exceptions.RedisError(response))
            elif format_callback is not None:
                return future.set_result(format_callback(response))
            elif expectation is not None:
                if isinstance(expectation, int) and expectation > 1:
                    return future.set_result(response == expectation or
                                             response)
                return future.set_result(response == expectation)
            future.set_result(response)
        else:
            self._read(on_data)

    def _maybe_connect(self, callback):
        """Connect to the Redis server, selecting the specified database.

        :raises: :py:class:`ConnectError <tredis.ConnectError>`
                 :py:class:`RedisError <tredis.RedisError>`

        """
        future = concurrent.TracebackFuture()
        self._ioloop.add_future(future, callback)

        if self._stream:
            return future.set_result(True)

        def on_connect(response):
            """Invoked when the socket stream has connected

            :param response: The connection response future
            :type response: :py:class:`tornado.concurrent.Future`

            """
            exc = response.exception()
            if exc:
                return future.set_exception(exceptions.ConnectError(str(exc)))

            LOGGER.debug('Connected')
            self._stream = response.result()
            self._stream.set_close_callback(self._on_closed)

            if not self._default_db:
                self._connecting.set()
                return future.set_result(True)

            def on_selected(selected_response):
                err = selected_response.exception()
                if err:
                    future.set_exception(err)
                    return
                self._connecting.set()
                future.set_result(True)
            LOGGER.debug('Selected the default database')
            selected = self._execute(['SELECT', ascii(self._default_db)])
            self._ioloop.add_future(selected, on_selected)

        if self._connecting and not self._connecting.is_set():
            def on_connected(_response):
                future.set_result(True)
            LOGGER.debug('Waiting on other connection attempt')
            wait = self._connecting.wait()
            self._ioloop.add_future(wait, on_connected)

        else:
            LOGGER.debug('Connecting to %s:%i', self._host, self._port)
            self._connecting = locks.Event()
            connect_future = self._client.connect(self._host, self._port)
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


class _RESPArrayNamespace(object):
    """Class for dealing with recursive async calls"""

    def __init__(self):
        self.depth = 0
        self.values = []
