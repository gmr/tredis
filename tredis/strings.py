"""Redis String Commands Mixin"""
from tornado import concurrent

# Python 2 support for ascii()
if 'ascii' not in dir(__builtins__):  # pragma: nocover
    from tredis.compat import ascii


class StringsMixin(object):
    """Redis String Commands Mixin"""

    def get(self, key):
        """Get the value of key. If the key does not exist the special value
        :py:data:`None` is returned. An error is returned if the value stored
        at key is not a string, because :py:meth:`get <tredis.RedisClient.get>`
        only handles string values.

        .. note::

           **Time complexity**: ``O(1)``

        :param key: The key to get
        :type key: str, bytes
        :rtype: bytes|None
        :raises: :py:exc:`RedisError <tredis.exceptions.RedisError>`

        """
        return self._execute([b'GET', key])

    def incr(self, key):
        """Increments the number stored at key by one. If the key does not
        exist, it is set to ``0`` before performing the operation. An error is
        returned if the key contains a value of the wrong type or contains a
        string that can not be represented as integer. This operation is
        limited to 64 bit signed integers.

        .. note:: This is a string operation because Redis does not have a
           dedicated integer type. The string stored at the key is interpreted
           as a base-10 64 bit signed integer to execute the operation.

        Redis stores integers in their integer representation, so for string
        values that actually hold an integer, there is no overhead for storing
        the string representation of the integer.

        .. note::

           **Time complexity**: ``O(1)``

        :param key: The key to increment
        :type key: str, bytes
        :rtype: int
        :raises: :py:exc:`RedisError <tredis.exceptions.RedisError>`

        """
        return self._execute([b'INCR', key])

    def set(self, key, value, ex=None, px=None, nx=False, xx=False):
        """Set key to hold the string value. If key already holds a value, it
        is overwritten, regardless of its type. Any previous time to live
        associated with the key is discarded on successful
        :py:meth:`set <tredis.RedisClient.set>` operation.

        If the value is not one of :py:class:`str`, :py:class:`bytes`, or
        :py:class:`int`, a :py:exc:`ValueError` will be raised.

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
        :raises: :py:exc:`RedisError <tredis.exceptions.RedisError>`
        :raises: :py:exc:`ValueError`

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
        return self._execute(command, b'OK')
