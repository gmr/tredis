"""Redis Server Commands Mixin"""
from tornado import concurrent

from tredis import exceptions

# Python 2 support for ascii()
if 'ascii' not in dir(__builtins__):  # pragma: nocover
    from tredis.compat import ascii


class ServerMixin(object):
    """Redis Server Commands Mixin"""

    def auth(self, password):
        """Request for authentication in a password-protected Redis server.
        Redis can be instructed to require a password before allowing clients
        to execute commands. This is done using the ``requirepass`` directive
        in the configuration file.

        If the password does not match, an
        :exc:`~tredis.exceptions.AuthError` exception
        will be raised.

        :param password: The password to authenticate with
        :type password: :class:`str`, :class:`bytes`
        :rtype: bool
        :raises: :exc:`~tredis.exceptions.AuthError`,
                 :exc:`~tredis.exceptions.RedisError`

        """
        future = concurrent.TracebackFuture()

        def on_response(response):
            """Process the redis response

            :param response: The future with the response
            :type response: tornado.concurrent.Future

            """
            exc = response.exception()
            if exc:
                if exc.args[0] == b'invalid password':
                    future.set_exception(exceptions.AuthError(exc))
                else:
                    future.set_exception(exc)
            else:
                future.set_result(response.result())

        execute_future = self._execute([b'AUTH', password], b'OK')
        self.io_loop.add_future(execute_future, on_response)
        return future

    def echo(self, message):
        """Returns the message that was sent to the Redis server.

        :param message: The message to echo
        :type message: :class:`str`, :class:`bytes`
        :rtype: bytes
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute([b'ECHO', message])

    def info(self, section=None):
        """The INFO command returns information and statistics about the server
        in a format that is simple to parse by computers and easy to read by
        humans.

        The optional parameter can be used to select a specific section of
        information:

            - server: General information about the Redis server
            - clients: Client connections section
            - memory: Memory consumption related information
            - persistence: RDB and AOF related information
            - stats: General statistics
            - replication: Master/slave replication information
            - cpu: CPU consumption statistics
            - commandstats: Redis command statistics
            - cluster: Redis Cluster section
            - keyspace: Database related statistics

        It can also take the following values:

            - all: Return all sections
            - default: Return only the default set of sections

        When no parameter is provided, the default option is assumed.

        :param str section: Optional
        :return: dict

        """

        def parse_value(value):
            """

            :param value:
            :return:

            """
            try:
                if b'.' in value:
                    return float(value)
                else:
                    return int(value)
            except ValueError:
                if b',' in value or b'=' in value:
                    retval = {}
                    for row in value.split(b','):
                        key, val = row.rsplit(b'=', 1)
                        retval[key.decode('utf-8')] = parse_value(val)
                    return retval
                return value.decode('utf-8')

        def format_response(value):
            """Format the response from redis

            :param str value: The return response from redis
            :rtype: dict

            """
            info = {}
            for line in value.splitlines():
                if line.startswith(b'#'):
                    continue
                if b':' in line:
                    key, value = line.split(b':', 1)
                    info[key.decode('utf-8')] = parse_value(value)
            return info

        if section:
            return self._execute([b'INFO', section],
                                 format_callback=format_response)
        return self._execute([b'INFO'], format_callback=format_response)

    def ping(self):
        """Returns ``PONG`` if no argument is provided, otherwise return a copy
        of the argument as a bulk. This command is often used to test if a
        connection is still alive, or to measure latency.

        If the client is subscribed to a channel or a pattern, it will instead
        return a multi-bulk with a ``pong`` in the first position and an empty
        bulk in the second position, unless an argument is provided in which
        case it returns a copy of the argument.

        :rtype: bytes
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute([b'PING'])

    def quit(self):
        """Ask the server to close the connection. The connection is closed as
        soon as all pending replies have been written to the client.

        :rtype: bool
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute([b'QUIT'], b'OK')

    def select(self, index=0):
        """Select the DB with having the specified zero-based numeric index.
        New connections always use DB ``0``.

        :param int index: The database to select
        :rtype: bool
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute([b'SELECT', ascii(index).encode('ascii')], b'OK')

    def time(self):
        """Retrieve the current time from the redis server.

        :rtype: float
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        def format_response(value):
            """Format a TIME response into a datetime.datetime

            :param list value: TIME response is a list of the number
                of seconds since the epoch and the number of micros
                as two byte strings
            :rtype: float

            """
            seconds, micros = value
            return float(seconds) + (float(micros) / 1000000.0)
        return self._execute([b'TIME'], format_callback=format_response)
