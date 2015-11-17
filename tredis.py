"""
tredis
======
An simple asynchronous Redis client for Tornado

"""
import logging

from tornado import concurrent
from tornado import ioloop
from tornado import tcpclient

__version__ = '0.1.0'

LOGGER = logging.getLogger(__name__)

CRLF = b'\r\n'

# Python 2 support for ascii()
if 'ascii' not in dir(__builtins__):  # pragma: nocover
    def ascii(value):
        return '%s' % value


class RedisClient(object):
    """A simple asynchronous Redis client with a subset of overall Redis
    functionality.

    .. code:: python

        client = tredis.RedisClient()
        yield client.connect()

        yield client.set('foo', 'bar')
        value = yield client.get('foo')

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
        self._settings = host, port, int(db or 0)
        self._client = tcpclient.TCPClient()
        self._ioloop = ioloop.IOLoop.current()
        self._on_close_callback = on_close_callback
        self._stream = None

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
                future.set_exception(ConnectError(str(exc)))
            else:
                self._stream = response.result()
                self._stream.set_close_callback(self._on_closed)
                if self._settings[2]:
                    self._execute([b'SELECT',
                                   ascii(self._settings[2]).encode('ascii')],
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

    def _on_closed(self):
        """Invoked when the connection is closed,

        """
        LOGGER.error('Connection closed')
        if self._on_close_callback:
            self._on_close_callback()

    # Server Commands

    def auth(self, password):
        """Request for authentication in a password-protected Redis server.
        Redis can be instructed to require a password before allowing clients
        to execute commands. This is done using the ``requirepass`` directive
        in the configuration file.

        If the password does not match, an
        :py:class:`AuthError <tredis.AuthError>` exception
        will be raised.

        **Command Type**: Server

        :param password: The password to authenticate with
        :type password: str, bytes
        :rtype: bool
        :raises: :py:class:`AuthError <tredis.AuthError>`
                 :py:class:`RedisError <tredis.RedisError>`

        """
        future = concurrent.TracebackFuture()

        def on_response(response):
            exc = response.exception()
            if exc:
                if exc.args[0] == b'invalid password':
                    future.set_exception(AuthError(exc))
                else:
                    future.set_exception(exc)
            else:
                future.set_result(response.result() == b'OK')
        self._execute([b'AUTH', password], on_response)
        return future

    def echo(self, message):
        """Returns the message that was sent to the Redis server.

        **Command Type**: Server

        :param message: The message to echo
        :type message: str, bytes
        :rtype: bytes
        :raises: :py:class:`RedisError <tredis.RedisError>`


        """
        return self._execute([b'ECHO', message])

    def ping(self):
        """Returns ``PONG`` if no argument is provided, otherwise return a copy
        of the argument as a bulk. This command is often used to test if a
        connection is still alive, or to measure latency.

        If the client is subscribed to a channel or a pattern, it will instead
        return a multi-bulk with a ``pong`` in the first position and an empty
        bulk in the second position, unless an argument is provided in which
        case it returns a copy of the argument.

        **Command Type**: Server

        :rtype: bytes
        :raises: :py:class:`RedisError <tredis.RedisError>`

        """
        return self._execute([b'PING'])

    def quit(self):
        """Ask the server to close the connection. The connection is closed as
        soon as all pending replies have been written to the client.

        **Command Type**: Server

        :rtype: bool
        :raises: :py:class:`RedisError <tredis.RedisError>`

        """
        future = concurrent.TracebackFuture()
        return self._execute([b'QUIT'],
                             lambda response: self._is_ok(response, future))

    def select(self, index=0):
        """Select the DB with having the specified zero-based numeric index.
        New connections always use DB ``0``.

        **Command Type**: Server

        :param int index: The database to select
        :rtype: bool
        :raises: :py:class:`RedisError <tredis.RedisError>`

        """
        future = concurrent.TracebackFuture()
        self._execute([b'SELECT', ascii(index).encode('ascii')],
                      lambda response: self._is_ok(response, future))
        return future

    # Key Commands

    def delete(self, *keys):
        """Removes the specified keys. A key is ignored if it does not exist.
        Returns ``True`` if all keys are removed. If more than one key is
        passed in and not all keys are remove, the number of removed keys is
        returned.

        **Time complexity**: O(N) where N is the number of keys that will be
        removed. When a key to remove holds a value other than a string, the
        individual complexity for this key is O(M) where M is the number of
        elements in the list, set, sorted set or hash. Removing a single key
        that holds a string value is O(1).

        **Command Type**: Key

        :param keys: One or more keys to remove
        :type keys: str, bytes
        :rtype: bool
        :raises: :py:class:`RedisError <tredis.RedisError>`

        """
        future = concurrent.TracebackFuture()

        def on_response(response):
            exc = response.exception()
            if exc:
                future.set_exception(exc)
            else:
                result = response.result()
                if result == len(keys):
                    future.set_result(True)
                else:
                    future.set_result(result)

        self._execute([b'DEL'] + list(keys), on_response)
        return future

    def dump(self, key):
        """Serialize the value stored at key in a Redis-specific format and
        return it to the user. The returned value can be synthesized back into
        a Redis key using the :py:meth:`restore <tredis.RedisClient.restore>`
        command.

        The serialization format is opaque and non-standard, however it has a
        few semantic characteristics:
            - It contains a 64-bit checksum that is used to make sure errors
              will be detected. The
              :py:meth:`restore <tredis.RedisClient.restore>`  command makes
              sure to check the checksum before synthesizing a key using the
              serialized value.
            - Values are encoded in the same format used by RDB.
            - An RDB version is encoded inside the serialized value, so that
              different Redis versions with incompatible RDB formats will
              refuse to process the serialized value.
            - The serialized value does NOT contain expire information. In
              order to capture the time to live of the current value the
              :py:meth:`pttl <tredis.RedisClient.pttl>` command should be used.

        If key does not exist ``None`` is returned.

        **Time complexity**: O(1) to access the key and additional O(N*M) to
        serialized it, where N is the number of Redis objects composing the
        value and M their average size. For small string values the time
        complexity is thus O(1)+O(1*M) where M is small, so simply O(1).

        **Command Type**: Key

        :param key: The key to dump
        :type key: str, bytes
        :rtype: bytes, None

        """
        return self._execute([b'DUMP', key])

    def exists(self, *keys):
        """Returns if key exists.

        Since Redis 3.0.3 it is possible to specify multiple keys instead of a
        single one. In such a case, it returns the total number of keys
        existing. Note that returning 1 or 0 for a single key is just a special
        case of the variadic usage, so the command is completely backward
        compatible.

        **Time complexity**: O(1)

        **Command Type**: String

        :param keys: One or more keys to check for
        :type keys: str, bytes
        :rtype: int
        :raises: :py:class:`RedisError <tredis.RedisError>`

        """
        return self._execute([b'EXISTS'] + list(keys))

    def expire(self, key, timeout):
        """Set a timeout on key. After the timeout has expired, the key will
        automatically be deleted. A key with an associated timeout is often
        said to be volatile in Redis terminology.

        The timeout is cleared only when the key is removed using the
        :py:meth:`delete <tredis.RedisClient.delete>` method or overwritten
        using the :py:meth:`set <tredis.RedisClient.set>` or
        :py:meth:`getset <tredis.RedisClient.getset>` methods. This means that
        all the operations that conceptually alter the value stored at the key
        without replacing it with a new one will leave the timeout untouched.
        For instance, incrementing the value of a key with
        :py:meth:`incr <tredis.RedisClient.incr>`, pushing a new value into a
        list with :py:meth:`incr <tredis.RedisClient.lpush>`, or altering the
        field value of a hash with :py:meth:`hset <tredis.RedisClient.hset>`
        are all operations that will leave the timeout untouched.

        The timeout can also be cleared, turning the key back into a
        persistent key, using the
        :py:meth:`persist <tredis.RedisClient.persist>` method.

        If a key is renamed with :py:meth:`rename <tredis.RedisClient.rename>`,
        the associated time to live is transferred to the new key name.

        If a key is overwritten by
        :py:meth:`rename <tredis.RedisClient.rename>`, like in the case of an
        existing key ``Key_A`` that is overwritten by a call like
        ``client.rename(Key_B, Key_A)`` it does not matter if the original
        ``Key_A`` had a timeout associated or not, the new key ``Key_A`` will
        inherit all the characteristics of ``Key_B``.

        **Time complexity**: O(1)

        **Command Type**: Key

        :param key: The key to set an expiration for
        :type key: str, bytes
        :param int timeout: The number of seconds to set the timeout to
        :rtype: bool
        :raises: :py:class:`RedisError <tredis.RedisError>`

        """
        future = concurrent.TracebackFuture()

        def on_response(response):
            exc = response.exception()
            if exc:
                future.set_exception(exc)
            else:
                result = response.result()
                future.set_result(result == 1)

        self._execute([b'EXPIRE', key, ascii(timeout).encode('ascii')],
                      on_response)
        return future

    def expireat(self, key, timestamp):
        """:py:class:`expireat <tredis.RedisClient.expireat>` has the same
        effect and semantic as :py:class:`expire <tredis.RedisClient.expire>`,
        but instead of specifying the number of seconds representing the
        TTL (time to live), it takes an absolute Unix timestamp (seconds since
        January 1, 1970).

        Please for the specific semantics of the command refer to the
        documentation of :py:class:`expire <tredis.RedisClient.expire>`.

        **Time complexity**: O(1)

        **Command Type**: Key

        :param key: The key to set an expiration for
        :type key: str, bytes
        :param int timeout: The number of seconds to set the timeout to
        :rtype: bool
        :raises: :py:class:`RedisError <tredis.RedisError>`

        """
        future = concurrent.TracebackFuture()

        def on_response(response):
            exc = response.exception()
            if exc:
                future.set_exception(exc)
            else:
                result = response.result()
                future.set_result(result == 1)

        self._execute([b'EXPIRE', key, ascii(timestamp).encode('ascii')],
                      on_response)
        return future

    def ttl(self, key):
        """Returns the remaining time to live of a key that has a timeout.
        This introspection capability allows a Redis client to check how many
        seconds a given key will continue to be part of the dataset.

        **Time complexity**: O(1)

        **Command Type**: Key

        :param key: The key to get the TTL for
        :type key: str, bytes
        :rtype: int
        :raises: :py:class:`RedisError <tredis.RedisError>`

        """
        return self._execute([b'TTL', key])

    # String Commands

    def get(self, key):
        """Get the value of key. If the key does not exist the special value
        ``None`` is returned. An error is returned if the value stored at key
        is not a string, because ``get`` only handles string values.

        **Time complexity**: O(1)

        **Command Type**: String

        :param key: The key to get
        :type key: str, bytes
        :rtype: bytes|None
        :raises: :py:class:`RedisError <tredis.RedisError>`

        """
        return self._execute([b'GET', key])

    def set(self, key, value, ex=None, px=None, nx=False, xx=False):
        """Set key to hold the string value. If key already holds a value, it
        is overwritten, regardless of its type. Any previous time to live
        associated with the key is discarded on successful SET operation.

        **Time complexity**: O(1)

        **Command Type**: String

        :param key: The key to remove
        :type key: str, bytes
        :param value: The value to set
        :type value: str, bytes, int
        :param int ex: Set the specified expire time, in seconds
        :param int px: Set the specified expire time, in milliseconds
        :param bool nx: Only set the key if it does not already exist
        :param bool xx: Only set the key if it already exist
        :rtype: bool
        :raises: :py:class:`RedisError <tredis.RedisError>`

        """
        future = concurrent.TracebackFuture()
        command = [b'SET', key, value]
        if ex:
            command += [b'EX', ascii(ex).encode('ascii')]
        if px:
            command += [b'PX', ascii(px).encode('ascii')]
        if nx:
            command.append(b'NX')
        if xx:
            command.append(b'XX')
        self._execute(command, lambda response: self._is_ok(response, future))
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

    def _execute(self, parts, callback=None):
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

    def _get_response(self, callback):
        """Read and parse command execution responses from Redis

        :param method callback: The method to receive the response data
        :rtype: :py:class:`tornado.concurrent.Future`

        """
        future = concurrent.TracebackFuture()
        self._ioloop.add_future(future, callback)

        def on_first_byte(first_byte):
            LOGGER.debug('future: %r, first_byte: %r', future, first_byte)
            if first_byte == b'+':
                def on_response(response):
                    future.set_result(response[0:-2])
                self._stream.read_until(CRLF, on_response)
            elif first_byte == b'-':
                def on_response(response):  # pragma: nocover
                    error = response[0:-2].decode('utf-8')
                    if error.startswith('ERR'):
                        error = error[4:]
                    future.set_exception(RedisError(error))
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
                """
                # Todo Arrays
                elif first_byte == b'*':
                    pass
                """
            else:  # pragma: nocover
                future.set_exception(ValueError(
                    'Unknown RESP first-byte: {}'.format(first_byte)))

        self._stream.read_bytes(1, callback=on_first_byte)

    @staticmethod
    def _is_ok(response, future):
        """Method invoked in a lambda to abbreviate the amount of code in
        each method when checking for an ``OK`` response.

        :param concurrent.Future response: The RedisClient._execute future
        :param concurrent.Future future: The current method's future

        """
        exc = response.exception()
        if exc:
            future.set_exception(exc)
        else:
            result = response.result()
            future.set_result(result == b'OK')


class RedisError(Exception):
    """Raised as a top-level exception class for all exceptions raised by
    :py:class:`RedisClient <tredis.RedisClient>`. The string representation
    of this class will contain the error resposne from the Redis server,
    if one is sent.

    """
    pass


class ConnectError(RedisError):
    """Raised when :py:class:`RedisClient <tredis.RedisClient>` can not connect
    to the specified Redis server.

    """
    pass


class AuthError(RedisError):
    """Raised when :py:meth:`RedisClient.auth <tredis.RedisClient.auth>` is
    invoked and the Redis server returns an error.

    """
    pass
