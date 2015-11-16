"""
tredis
======
An simple asynchronous Redis client for Tornado

"""
import logging

from tornado import gen
from tornado import tcpclient

__version__ = '0.1.0'

LOGGER = logging.getLogger(__name__)

CRLF = b'\r\n'

# Python 2 support for ascii()
if 'ascii' not in dir(__builtins__):
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

    """
    DEFAULT_HOST = 'localhost'
    """The default host to connect to"""

    DEFAULT_PORT = 6379
    """The default port to connect to"""

    DEFAULT_DB = 0
    """The default database number to use"""

    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT, db=DEFAULT_DB):
        self._settings = host, port, int(db or 0)
        self._client = tcpclient.TCPClient()
        self._stream = None

    @gen.coroutine
    def connect(self):
        self._stream = yield self._client.connect(self._settings[0],
                                                  self._settings[1])
        if self._settings[2]:
            yield self.select(self._settings[2])

    # Server Commands

    @gen.coroutine
    def auth(self, password):
        """Request for authentication in a password-protected Redis server.
        Redis can be instructed to require a password before allowing clients
        to execute commands. This is done using the ``requirepass`` directive
        in the configuration file.

        If the password does not match, an
        :py:class:`AuthenticationError <tredis.AuthenticationError>` exception
        will be raised.

        .. describe:: Server Command

        :param str|bytes password: The password to authenticate with
        :rtype bool: Will be ``True`` if authenticated
        :raises: tredis.AuthenticationError

        """
        try:
            response = yield self._execute([b'AUTH', password])
        except RedisError as error:
            raise AuthenticationError(str(error)[4:])
        raise gen.Return(response == b'OK')

    @gen.coroutine
    def echo(self, message):
        """Returns the message that was sent to the Redis server.

        .. describe:: Server Command

        :param str|bytes message: The message to echo
        :rtype: bytes
        :raises: RedisError

        """
        response = yield self._execute([b'ECHO', message])
        raise gen.Return(response)

    @gen.coroutine
    def ping(self):
        """Returns ``PONG`` if no argument is provided, otherwise return a copy
        of the argument as a bulk. This command is often used to test if a
        connection is still alive, or to measure latency.

        If the client is subscribed to a channel or a pattern, it will instead
        return a multi-bulk with a ``pong`` in the first position and an empty
        bulk in the second position, unless an argument is provided in which
        case it returns a copy of the argument.

        .. describe:: Server Command

        :rtype: bytes
        :raises: RedisError

        """
        response = yield self._execute([b'PING'])
        raise gen.Return(response)

    @gen.coroutine
    def quit(self):
        """Ask the server to close the connection. The connection is closed as
        soon as all pending replies have been written to the client.

        .. describe:: Server Command

        :rtype: bytes
        :raises: RedisError

        """
        response = yield self._execute([b'QUIT'])
        raise gen.Return(response)

    @gen.coroutine
    def select(self, index=0):
        """Select the DB with having the specified zero-based numeric index.
        New connections always use DB ``0``.

        .. describe:: Server Command

        :param int index: The database to select
        :rtype: bytes
        :raises: RedisError

        """
        response = yield self._execute([b'SELECT',
                                        ascii(index).encode('ascii')])
        raise gen.Return(response)

    # Key Commands

    @gen.coroutine
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

        .. describe:: Key Command

        :param str|bytes keys: The key to remove
        :rtype: bool

        """
        response = yield self._execute([b'DEL'] + keys)
        if len(keys) == 1:
            raise gen.Return(response == b'OK')
        elif response == len(keys):
            raise gen.Return(True)
        raise gen.Return(response)

    @gen.coroutine
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

        .. describe:: Key Command

        :param str|bytes key: The key to set an expiration for
        :param int timeout: The number of seconds to set the timeout to
        :rtype: bool

        """
        response = yield self._execute([b'EXPIRE', key,
                                        ascii(timeout).encode('ascii')])
        raise gen.Return(response == 1)

    @gen.coroutine
    def ttl(self, key):
        """Returns the remaining time to live of a key that has a timeout.
        This introspection capability allows a Redis client to check how many
        seconds a given key will continue to be part of the dataset.

        **Time complexity**: O(1)

        .. describe:: Key Command

        :param str|bytes key: The key to get the TTL for
        :rtype: int

        """
        response = yield self._execute([b'TTL', key])
        raise gen.Return(int(response))

    # String Commands

    @gen.coroutine
    def get(self, key):
        """Get the value of key. If the key does not exist the special value
        ``None`` is returned. An error is returned if the value stored at key
        is not a string, because ``get`` only handles string values.

        **Time complexity**: O(1)

        .. describe:: String Command

        :param str|bytes key: The key to get
        :rtype: bytes|None

        """
        response = yield self._execute([b'GET', key])
        raise gen.Return(response)

    @gen.coroutine
    def set(self, key, value, ex=None, px=None, nx=False, xx=False):
        """Set key to hold the string value. If key already holds a value, it
        is overwritten, regardless of its type. Any previous time to live
        associated with the key is discarded on successful SET operation.

        **Time complexity**: O(1)

        .. describe:: String Command

        :param str|bytes key: The key to remove
        :param str|bytes|int value: The value to set
        :param int ex: Set the specified expire time, in seconds
        :param int px: Set the specified expire time, in milliseconds
        :param bool nx: Only set the key if it does not already exist
        :param bool xx: Only set the key if it already exist
        :rtype: bool

        """
        command = [b'SET', key, value]
        if ex:
            command += [b'EX', ascii(ex).encode('ascii')]
        if px:
            command += [b'PX', ascii(px).encode('ascii')]
        if nx:
            command.append(b'NX')
        if xx:
            command.append(b'XX')
        response = yield self._execute(command)
        raise gen.Return(response == b'OK')

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

    @gen.coroutine
    def _execute(self, parts):
        yield self._stream.write(self._build_command(parts))
        response = yield self._get_response()
        raise gen.Return(response)

    @gen.coroutine
    def _get_response(self):
        first_byte = yield self._stream.read_bytes(1)
        if first_byte == b'+':
            data = yield self._stream.read_until(CRLF)
            raise gen.Return(data.strip())
        elif first_byte == b'-':
            data = yield self._stream.read_until(CRLF)
            error = data.strip().decode('utf-8')
            if error.startswith('ERR'):
                error = error[4:]
            raise RedisError(error)
        elif first_byte == b':':
            data = yield self._stream.read_until(CRLF)
            raise gen.Return(int(data.strip()))
        elif first_byte == b'$':
            tmp = yield self._stream.read_until(CRLF)
            if tmp == b'-1\r\n':
                raise gen.Return(None)
            str_len = int(tmp.strip()) + 2
            data = yield self._stream.read_bytes(str_len)
            raise gen.Return(data[:-2])
        elif first_byte == b'*':
            tmp = yield self._stream.read_until(CRLF)
            segments = int(tmp.strip()) + 2
            values = []
            for index in range(0, segments):
                value = yield self._get_response()
                values.append(value)
        else:
            raise ValueError('Unknown RESP first-byte: {}'.format(first_byte))


class RedisError(Exception):
    pass


class AuthenticationError(RedisError):
    pass
