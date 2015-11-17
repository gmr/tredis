from tornado import concurrent

from tredis import exceptions


class ServerMixin(object):

    def auth(self, password):
        """Request for authentication in a password-protected Redis server.
        Redis can be instructed to require a password before allowing clients
        to execute commands. This is done using the ``requirepass`` directive
        in the configuration file.

        If the password does not match, an
        :py:class:`AuthError <tredis.exceptions.AuthError>` exception
        will be raised.

        :param password: The password to authenticate with
        :type password: str, bytes
        :rtype: bool
        :raises: :py:class:`AuthError <tredis.exceptions.AuthError>`
                 :py:class:`RedisError <tredis.exceptions.RedisError>`

        """
        future = concurrent.TracebackFuture()

        def on_response(response):
            exc = response.exception()
            if exc:
                if exc.args[0] == b'invalid password':
                    future.set_exception(exceptions.AuthError(exc))
                else:
                    future.set_exception(exc)
            else:
                future.set_result(response.result() == b'OK')
        self._execute([b'AUTH', password], on_response)
        return future

    def echo(self, message):
        """Returns the message that was sent to the Redis server.

        :param message: The message to echo
        :type message: str, bytes
        :rtype: bytes
        :raises: :py:class:`RedisError <tredis.exceptions.RedisError>`


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

        :rtype: bytes
        :raises: :py:class:`RedisError <tredis.exceptions.RedisError>`

        """
        return self._execute([b'PING'])

    def quit(self):
        """Ask the server to close the connection. The connection is closed as
        soon as all pending replies have been written to the client.

        :rtype: bool
        :raises: :py:class:`RedisError <tredis.exceptions.RedisError>`

        """
        future = concurrent.TracebackFuture()
        return self._execute([b'QUIT'],
                             lambda response: self._is_ok(response, future))

    def select(self, index=0):
        """Select the DB with having the specified zero-based numeric index.
        New connections always use DB ``0``.

        :param int index: The database to select
        :rtype: bool
        :raises: :py:class:`RedisError <tredis.exceptions.RedisError>`

        """
        future = concurrent.TracebackFuture()
        self._execute([b'SELECT', ascii(index).encode('ascii')],
                      lambda response: self._is_ok(response, future))
        return future
