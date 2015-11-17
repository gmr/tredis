from tornado import concurrent

# Python 2 support for ascii()
if 'ascii' not in dir(__builtins__):  # pragma: nocover
    from tredis.compat import ascii


class StringsMixin(object):

    def get(self, key):
        """Get the value of key. If the key does not exist the special value
        ``None`` is returned. An error is returned if the value stored at key
        is not a string, because ``get`` only handles string values.

        .. note::

           **Time complexity**: ``O(1)``

        :param key: The key to get
        :type key: str, bytes
        :rtype: bytes|None
        :raises: :py:class:`RedisError <tredis.exceptions.RedisError>`

        """
        return self._execute([b'GET', key])

    def set(self, key, value, ex=None, px=None, nx=False, xx=False):
        """Set key to hold the string value. If key already holds a value, it
        is overwritten, regardless of its type. Any previous time to live
        associated with the key is discarded on successful SET operation.

        .. note::

           **Time complexity**: ``O(1)``

        :param key: The key to remove
        :type key: str, bytes
        :param value: The value to set
        :type value: str, bytes, int
        :param int ex: Set the specified expire time, in seconds
        :param int px: Set the specified expire time, in milliseconds
        :param bool nx: Only set the key if it does not already exist
        :param bool xx: Only set the key if it already exist
        :rtype: bool
        :raises: :py:class:`RedisError <tredis.exceptions.RedisError>`

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
