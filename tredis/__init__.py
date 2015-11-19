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

__version__ = '0.1.0'

LOGGER = logging.getLogger(__name__)

CRLF = b'\r\n'

# Python 2 support for ascii()
if 'ascii' not in dir(__builtins__):  # pragma: nocover
    from tredis.compat import ascii


class _RESPArrayNamespace(object):
    """Class for dealing with recursive async calls"""

    def __init__(self):
        self.depth = 0
        self.values = []


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


class RedisClient(
        server.ServerMixin, keys.KeysMixin, strings.StringsMixin, geo.GeoMixin,
        hashes.HashesMixin, hyperloglog.HyperLogLogMixin, lists.ListsMixin,
        sets.SetsMixin, sortedsets.SortedSetsMixin, pubsub.PubSubMixin,
        connection.ConnectionMixin, cluster.ClusterMixin,
        scripting.ScriptingMixin, transactions.TransactionsMixin, object):
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

    def __init__(self,
                 host=DEFAULT_HOST,
                 port=DEFAULT_PORT,
                 db=DEFAULT_DB,
                 on_close_callback=None):
        self._settings = host, port, int(db or self.DEFAULT_DB)
        self._client = tcpclient.TCPClient()
        self._ioloop = ioloop.IOLoop.current()
        self._on_close_callback = on_close_callback
        self._stream = None
        self._pipeline = False
        self._pipeline_commands = []
        super(RedisClient, self).__init__()

    def connect(self):
        """Connect to the Redis server, selecting the specified database.

        :rtype: bool
        :raises: :py:class:`ConnectError <tredis.ConnectError>`
                 :py:class:`RedisError <tredis.RedisError>`

        """
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
                self._stream.set_close_callback(self._on_closed)
                if self._settings[2]:
                    self._execute(
                        [b'SELECT', ascii(self._settings[2]).encode('ascii')],
                        lambda resp: self._is_ok(resp, future))
                else:
                    future.set_result(True)

        connect_future = self._client.connect(self._settings[0],
                                              self._settings[1])
        self._ioloop.add_future(connect_future, on_connect)
        return future

    def close(self):
        """Close the connection to the Redis Server"""
        self._stream.close()

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
                eval_method = self._pipeline_commands[index][1]
                if eval_method:
                    pipeline_responses.append(eval_method(response.result()))
                else:
                    pipeline_responses.append(response.result())

            if len(pipeline_responses) == commands:
                future.set_result(pipeline_responses.values)
            else:
                self._get_response(on_response)

        def on_written():
            """Invoked when the command has been written to the socket"""
            self._get_response(on_response)

        pipeline = b''.join([cmd[0] for cmd in self._pipeline_commands])
        self._stream.write(pipeline, callback=on_written)
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
            raise exceptions.PipelineError()

        future = concurrent.TracebackFuture()
        self._ioloop.add_future(future, callback)

        def on_response(response):
            """Process the response future

            :param response: The response future
            :type response: :py:class:`tornado.concurrent.Future`

            """
            exc = response.exception()
            if exc:
                future.set_exception(exc)
            else:
                result = response.result()
                future.set_result(result)

        def on_written():
            """Invoked when the command has been written to the socket"""
            self._get_response(on_response)

        self._stream.write(self._build_command(parts), callback=on_written)
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
                future.set_result(response.result() == 1)

        self._execute(parts, on_response)
        return future

    def _get_response(self, callback):
        """Read and parse command execution responses from Redis

        :param method callback: The method to receive the response data
        :rtype: :py:class:`tornado.concurrent.Future`

        """
        future = concurrent.TracebackFuture()
        self._ioloop.add_future(future, callback)

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

                self._stream.read_until(CRLF, on_response)
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

                self._stream.read_until(CRLF, callback=on_response)
            elif first_byte == b':':

                def on_response(response):
                    """Process the response data

                    :param response: The response data
                    :type response: bytes

                    """
                    future.set_result(int(response[:-2]))

                self._stream.read_until(CRLF, callback=on_response)
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
                        self._stream.read_bytes(int(response.strip()) + 2,
                                                on_payload)

                self._stream.read_until(CRLF, callback=on_response)
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
                    self._get_array(response, on_complete)

                self._stream.read_until(CRLF, callback=on_segments)

            else:  # pragma: nocover
                future.set_exception(ValueError(
                    'Unknown RESP first-byte: {}'.format(first_byte)))

        self._stream.read_bytes(1, callback=on_first_byte)

    def _get_array(self, segments, callback):
        """Read in an array by calling :py:meth:`RedisClient._get_response`
        for each element in the array.

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
                self._get_response(on_response)

        self._get_response(on_response)

    @staticmethod
    def _is_ok(response, future):
        """Method invoked in a lambda to abbreviate the amount of code in
        each method when checking for an ``OK`` response.

        :param response: The RedisClient._execute future
        :type response: :py:class:`tornado.concurrent.Future`
        :param concurrent.Future future: The current method's future
        :type future: :py:class:`tornado.concurrent.Future`

        """
        exc = response.exception()
        if exc:
            future.set_exception(exc)
        else:
            result = response.result()
            future.set_result(result == b'OK')

    def _on_closed(self):
        """Invoked when the connection is closed"""
        LOGGER.error('Connection closed')
        if self._on_close_callback:
            self._on_close_callback()

    def _pipeline_add(self, parts, response_processor=None):
        """Add a command to execute to the pipeline

        :param list parts: The command parts
        :param method response_processor: A method for command response eval

        """
        self._pipeline_commands.append((self._build_command(parts),
                                        response_processor))

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
