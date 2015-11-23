"""Redis String Commands Mixin"""

# Python 2 support for ascii()
if 'ascii' not in dir(__builtins__):  # pragma: nocover
    from tredis.compat import ascii

BITOP_AND = b'&'
"""Use for specifying a bitwise AND operation with
:meth:`~tredis.RedisClient.bitop`"""

BITOP_OR = b'|'
"""Use for specifying a bitwise OR operation with
:meth:`~tredis.RedisClient.bitop`"""

BITOP_XOR = b'^'
"""Use for specifying a bitwise XOR operation with
:meth:`~tredis.RedisClient.bitop`"""

BITOP_NOT = b'~'
"""Use for specifying a bitwise NOT operation with
:meth:`~tredis.RedisClient.bitop`"""

_BITOPTS = {
    BITOP_AND: b'AND',
    BITOP_OR:  b'OR',
    BITOP_XOR: b'XOR',
    BITOP_NOT: b'NOT',
}


class StringsMixin(object):
    """Redis String Commands Mixin"""

    def append(self, key, value):
        """If key already exists and is a string, this command appends the
        value at the end of the string. If key does not exist it is created and
        set as an empty string, so :meth:`~tredis.RedisClient.append` will be
        similar to :meth:`~tredis.RedisClient.set` in this special case.

        .. versionadded:: 0.2.0

        .. note:: **Time complexity**: ``O(1)``. The amortized time complexity
           is ``O(1)`` assuming the appended value is small and the already
           present value is of any size, since the dynamic string library used
           by Redis will double the free space available on every reallocation.

        :param key: The key to get
        :type key: :class:`str`, :class:`bytes`
        :param value: The value to append to the key
        :type value: :class:`str`, :class:`bytes`
        :returns: The length of the string after the append operation
        :rtype: int
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute([b'APPEND', key, value])

    def bitcount(self, key, start=None, end=None):
        """Count the number of set bits (population counting) in a string.

        By default all the bytes contained in the string are examined. It is
        possible to specify the counting operation only in an interval passing
        the additional arguments start and end.

        Like for the :meth:`~tredis.RedisClient.getrange` command start and
        end can contain negative values in order to index bytes starting from
        the end of the string, where ``-1`` is the last byte, ``-2`` is the
        penultimate, and so forth.

        Non-existent keys are treated as empty strings, so the command will
        return zero.

        .. versionadded:: 0.2.0

        .. note:: **Time complexity**: ``O(N)``

        :param key: The key to get
        :type key: :class:`str`, :class:`bytes`
        :param int start: The start position to evaluate in the string
        :param int end: The end position to evaluate in the string
        :rtype: int
        :raises: :exc:`~tredis.exceptions.RedisError`, :exc:`ValueError`

        """
        command = [b'BITCOUNT', key]
        if start is not None and end is None:
            raise ValueError('Can not specify start without an end')
        elif start is None and end is not None:
            raise ValueError('Can not specify start without an end')
        elif start is not None and end is not None:
            command += [ascii(start), ascii(end)]
        return self._execute(command)

    def bitop(self, operation, dest_key, *keys):
        """Perform a bitwise operation between multiple keys (containing
        string values) and store the result in the destination key.

        The values for operation can be one of:

            - ``b'AND'``
            - ``b'OR'``
            - ``b'XOR'``
            - ``b'NOT'``
            - :data:`tredis.BITOP_AND` or ``b'&'``
            - :data:`tredis.BITOP_OR` or ``b'|'``
            - :data:`tredis.BITOP_XOR` or ``b'^'``
            - :data:`tredis.BITOP_NOT` or ``b'~'``

        ``b'NOT'`` is special as it only takes an input key, because it
        performs inversion of bits so it only makes sense as an unary operator.

        The result of the operation is always stored at ``dest_key``.

        **Handling of strings with different lengths**

        When an operation is performed between strings having different
        lengths, all the strings shorter than the longest string in the set are
        treated as if they were zero-padded up to the length of the longest
        string.

        The same holds true for non-existent keys, that are considered as a
        stream of zero bytes up to the length of the longest string.

        .. versionadded:: 0.2.0

        .. note:: **Time complexity**: ``O(N)``

        :param bytes operation: The operation to perform
        :param dest_key: The key to store the bitwise operation results to
        :type dest_key: :class:`str`, :class:`bytes`
        :param keys: One or more keys as keyword parameters for the bitwise op
        :type keys: :class:`str`, :class:`bytes`
        :return: The size of the string stored in the destination key, that is
                 equal to the size of the longest input string.
        :rtype: int
        :raises: :exc:`~tredis.exceptions.RedisError`, :exc:`ValueError`

        """
        if (operation not in _BITOPTS.keys() and
                operation not in _BITOPTS.values()):
            raise ValueError('Invalid operation value: {}'.format(operation))
        elif operation in [b'~', b'NOT'] and len(keys) > 1:
            raise ValueError('NOT can only be used with 1 key')

        if operation in _BITOPTS.keys():
            operation = _BITOPTS[operation]

        return self._execute([b'BITOP', operation, dest_key] + list(keys))

    def bitpos(self, key, bit, start=None, end=None):
        """Return the position of the first bit set to ``1`` or ``0`` in a
        string.

        The position is returned, thinking of the string as an array of bits
        from left to right, where the first byte's most significant bit is at
        position 0, the second byte's most significant bit is at position
        ``8``, and so forth.

        The same bit position convention is followed by
        :meth:`~tredis.RedisClient.getbit` and
        :meth:`~tredis.RedisClient.setbit`.

        By default, all the bytes contained in the string are examined. It is
        possible to look for bits only in a specified interval passing the
        additional arguments start and end (it is possible to just pass start,
        the operation will assume that the end is the last byte of the string.
        However there are semantic differences as explained later). The range
        is interpreted as a range of bytes and not a range of bits, so
        ``start=0`` and ``end=2`` means to look at the first three bytes.

        Note that bit positions are returned always as absolute values starting
        from bit zero even when start and end are used to specify a range.

        Like for the :meth:`~tredis.RedisClient.getrange` command start and
        end can contain negative values in order to index bytes starting from
        the end of the string, where ``-1`` is the last byte, ``-2`` is the
        penultimate, and so forth.

        Non-existent keys are treated as empty strings.

        .. versionadded:: 0.2.0

        .. note:: **Time complexity**: ``O(N)``

        :param key: The key to get
        :type key: :class:`str`, :class:`bytes`
        :param int bit: The bit value to search for (``1`` or ``0``)
        :param int start: The start position to evaluate in the string
        :param int end: The end position to evaluate in the string
        :returns: The position of the first bit set to ``1`` or ``0``
        :rtype: int
        :raises: :exc:`~tredis.exceptions.RedisError`, :exc:`ValueError`

        """
        if 0 < bit > 1:
            raise ValueError('bit must be 1 or 0, not {}'.format(bit))
        command = [b'BITPOS', key, ascii(bit)]
        if start is not None and end is None:
            raise ValueError('Can not specify start without an end')
        elif start is None and end is not None:
            raise ValueError('Can not specify start without an end')
        elif start is not None and end is not None:
            command += [ascii(start), ascii(end)]
        return self._execute(command)

    def decr(self, key):
        """Decrements the number stored at key by one. If the key does not
        exist, it is set to 0 before performing the operation. An error is
        returned if the key contains a value of the wrong type or contains a
        string that can not be represented as integer. This operation is
        limited to 64 bit signed integers.

        See :meth:`~tredis.RedisClient.incr` for extra information on
        increment/decrement operations.

        .. versionadded:: 0.2.0

        .. note:: **Time complexity**: ``O(1)``

        :param key: The key to decrement
        :type key: :class:`str`, :class:`bytes`
        :returns: The value of key after the decrement
        :rtype: int
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute([b'DECR', key])

    def decrby(self, key, decrement):
        """Decrements the number stored at key by decrement. If the key does
        not exist, it is set to 0 before performing the operation. An error
        is returned if the key contains a value of the wrong type or contains
        a string that can not be represented as integer. This operation is
        limited to 64 bit signed integers.

        See :meth:`~tredis.RedisClient.incr` for extra information on
        increment/decrement operations.

        .. versionadded:: 0.2.0

        .. note:: **Time complexity**: ``O(1)``

        :param key: The key to decrement
        :type key: :class:`str`, :class:`bytes`
        :param int decrement: The amount to decrement by
        :returns: The value of key after the decrement
        :rtype: int
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute([b'DECRBY', key, ascii(decrement)])

    def get(self, key):
        """Get the value of key. If the key does not exist the special value
        :data:`None` is returned. An error is returned if the value stored
        at key is not a string, because :meth:`~tredis.RedisClient.get` only
        handles string values.

        .. note:: **Time complexity**: ``O(1)``

        :param key: The key to get
        :type key: :class:`str`, :class:`bytes`
        :rtype: bytes|None
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute([b'GET', key])

    def getbit(self, key, offset):
        """Returns the bit value at offset in the string value stored at key.

        When offset is beyond the string length, the string is assumed to be a
        contiguous space with 0 bits. When key does not exist it is assumed to
        be an empty string, so offset is always out of range and the value is
        also assumed to be a contiguous space with 0 bits.

        .. versionadded:: 0.2.0

        .. note:: **Time complexity**: ``O(1)``

        :param key: The key to get the bit from
        :type key: :class:`str`, :class:`bytes`
        :param int offset: The bit offset to fetch the bit from
        :rtype: bytes|None
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute([b'GETBIT', key, ascii(offset)])

    def getrange(self, key, start, end):
        """Returns the bit value at offset in the string value stored at key.

        When offset is beyond the string length, the string is assumed to be a
        contiguous space with 0 bits. When key does not exist it is assumed to
        be an empty string, so offset is always out of range and the value is
        also assumed to be a contiguous space with 0 bits.

        .. versionadded:: 0.2.0

        .. note:: **Time complexity**: ``O(N)`` where ``N`` is the length of
           the returned string. The complexity is ultimately determined by the
           returned length, but because creating a substring from an existing
           string is very cheap, it can be considered ``O(1)`` for small
           strings.

        :param key: The key to get the bit from
        :type key: :class:`str`, :class:`bytes`
        :param int start: The start position to evaluate in the string
        :param int end: The end position to evaluate in the string
        :rtype: bytes|None
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute([b'GETRANGE', key, ascii(start), ascii(end)])

    def getset(self, key, value):
        """Atomically sets key to value and returns the old value stored at
        key. Returns an error when key exists but does not hold a string value.

        .. versionadded:: 0.2.0

        .. note:: **Time complexity**: ``O(1)``

        :param key: The key to remove
        :type key: :class:`str`, :class:`bytes`
        :param value: The value to set
        :type value: :class:`str`, :class:`bytes`
        :returns: The previous value
        :rtype: bytes
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute([b'GETSET', key, value])

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

        .. note:: **Time complexity**: ``O(1)``

        :param key: The key to increment
        :type key: :class:`str`, :class:`bytes`
        :rtype: int
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute([b'INCR', key])

    def incrby(self, key, increment):
        """Increments the number stored at key by increment. If the key does
        not exist, it is set to 0 before performing the operation. An error is
        returned if the key contains a value of the wrong type or contains a
        string that can not be represented as integer. This operation is
        limited to 64 bit signed integers.

        See :meth:`~tredis.RedisClient.incr` for extra information on
        increment/decrement operations.

        .. versionadded:: 0.2.0

        .. note:: **Time complexity**: ``O(1)``

        :param key: The key to increment
        :type key: :class:`str`, :class:`bytes`
        :param int increment: The amount to increment by
        :returns: The value of key after the increment
        :rtype: int
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute([b'INCRBY', key, ascii(increment)])

    def incrbyfloat(self, key, increment):
        """Increment the string representing a floating point number stored at
        key by the specified increment. If the key does not exist, it is set to
        0 before performing the operation. An error is returned if one of the
        following conditions occur:

          - The key contains a value of the wrong type (not a string).
          - The current key content or the specified increment are not
            parsable as a double precision floating point number.

        If the command is successful the new incremented value is stored as the
        new value of the key (replacing the old one), and returned to the
        caller as a string.

        Both the value already contained in the string key and the increment
        argument can be optionally provided in exponential notation, however
        the value computed after the increment is stored consistently in the
        same format, that is, an integer number followed (if needed) by a dot,
        and a variable number of digits representing the decimal part of the
        number. Trailing zeroes are always removed.

        The precision of the output is fixed at 17 digits after the decimal
        point regardless of the actual internal precision of the computation.

        .. versionadded:: 0.2.0

        .. note:: **Time complexity**: ``O(1)``

        :param key: The key to increment
        :type key: :class:`str`, :class:`bytes`
        :param float increment: The amount to increment by
        :returns: The value of key after the increment
        :rtype: bytes
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute([b'INCRBYFLOAT', key, ascii(increment)])

    def mget(self, *keys):
        """Returns the values of all specified keys. For every key that does
        not hold a string value or does not exist, the special value nil is
        returned. Because of this, the operation never fails.

        .. versionadded:: 0.2.0

        .. note:: **Time complexity**: ``O(N)`` where ``N`` is the number of
           keys to retrieve.

        :param keys: One or more keys as keyword arguments to the function
        :type keys: :class:`str`, :class:`bytes`
        :rtype: list
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute([b'MGET'] + list(keys))

    def mset(self, mapping):
        """Sets the given keys to their respective values.
        :meth:`~tredis.RedisClient.mset` replaces existing values with new
        values, just as regular :meth:`~tredis.RedisClient.set`. See
        :meth:`~tredis.RedisClient.msetnx` if you don't want to overwrite
        existing values.

        :meth:`~tredis.RedisClient.mset` is atomic, so all given keys are set
        at once. It is not possible for clients to see that some of the keys
        were updated while others are unchanged.

        .. versionadded:: 0.2.0

        .. note:: **Time complexity**: ``O(N)`` where ``N`` is the number of
           keys to set.

        :param dict mapping: A mapping of key/value pairs to set
        :rtype: bool
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        command = [b'MSET']
        for key, value in mapping.items():
            command += [key, value]
        return self._execute(command, b'OK')

    def msetnx(self, mapping):
        """Sets the given keys to their respective values.
        :meth:`~tredis.RedisClient.msetnx` will not perform any operation at
        all even if just a single key already exists.

        Because of this semantic :meth:`~tredis.RedisClient.msetnx` can be used
        in order to set different keys representing different fields of an
        unique logic object in a way that ensures that either all the fields or
        none at all are set.

        :meth:`~tredis.RedisClient.msetnx` is atomic, so all given keys are set
        at once. It is not possible for clients to see that some of the keys
        were updated while others are unchanged.

        .. versionadded:: 0.2.0

        .. note:: **Time complexity**: ``O(N)`` where ``N`` is the number of
           keys to set.

        :param dict mapping: A mapping of key/value pairs to set
        :rtype: bool
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        command = [b'MSETNX']
        for key, value in mapping.items():
            command += [key, value]
        return self._execute(command, 1)

    def psetex(self, key, milliseconds, value):
        """:meth:`~tredis.RedisClient.psetex` works exactly like
        :meth:`~tredis.RedisClient.psetex` with the sole difference that the
        expire time is specified in milliseconds instead of seconds.

        .. versionadded:: 0.2.0

        .. note:: **Time complexity**: ``O(1)``

        :param key: The key to set
        :type key: :class:`str`, :class:`bytes`
        :param int milliseconds: Number of milliseconds for TTL
        :param value: The value to set
        :type value: :class:`str`, :class:`bytes`
        :rtype: bool
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute([b'PSETEX', key, ascii(milliseconds), value],
                             b'OK')

    def set(self, key, value, ex=None, px=None, nx=False, xx=False):
        """Set key to hold the string value. If key already holds a value, it
        is overwritten, regardless of its type. Any previous time to live
        associated with the key is discarded on successful
        :meth:`~tredis.RedisClient.set` operation.

        If the value is not one of :class:`str`, :class:`bytes`, or
        :class:`int`, a :exc:`ValueError` will be raised.

        .. note:: **Time complexity**: ``O(1)``

        :param key: The key to remove
        :type key: :class:`str`, :class:`bytes`
        :param value: The value to set
        :type value: :class:`str`, :class:`bytes`, :class:`int`
        :param int ex: Set the specified expire time, in seconds
        :param int px: Set the specified expire time, in milliseconds
        :param bool nx: Only set the key if it does not already exist
        :param bool xx: Only set the key if it already exist
        :rtype: bool
        :raises: :exc:`~tredis.exceptions.RedisError`
        :raises: :exc:`ValueError`

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

    def setbit(self, key, offset, bit):
        """Sets or clears the bit at offset in the string value stored at key.

        The bit is either set or cleared depending on value, which can be
        either 0 or 1. When key does not exist, a new string value is created.
        The string is grown to make sure it can hold a bit at offset. The
        offset argument is required to be greater than or equal to 0, and
        smaller than 2 :sup:`32` (this limits bitmaps to 512MB). When the
        string at key is grown, added bits are set to 0.

        .. warning:: When setting the last possible bit (offset equal to
           2 :sup:`32` -1) and the string value stored at key does not yet hold
           a string value, or holds a small string value, Redis needs to
           allocate all intermediate memory which can block the server for some
           time. On a 2010 MacBook Pro, setting bit number 2 :sup:`32` -1
           (512MB allocation) takes ~300ms, setting bit number 2 :sup:`30` -1
           (128MB allocation) takes ~80ms, setting bit number 2 :sup:`28` -1
           (32MB allocation) takes ~30ms and setting bit number 2 :sup:`26` -1
           (8MB allocation) takes ~8ms. Note that once this first allocation is
           done, subsequent calls to :meth:`~tredis.RedisClient.setbit` for the
           same key will not have the allocation overhead.

        .. versionadded:: 0.2.0

        .. note:: **Time complexity**: ``O(1)``

        :param key: The key to get the bit from
        :type key: :class:`str`, :class:`bytes`
        :param int offset: The bit offset to fetch the bit from
        :param int bit: The value (``0`` or ``1``) to set for the bit
        :rtype: int
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        if 0 < bit > 1:
            raise ValueError('bit must be 1 or 0, not {}'.format(bit))
        return self._execute([b'SETBIT', key, ascii(offset), ascii(bit)])

    def setex(self, key, seconds, value):
        """Set key to hold the string value and set key to timeout after a
        given number of seconds.

        :meth:`~tredis.RedisClient.setex` is atomic, and can be reproduced by
        using :meth:`~tredis.RedisClient.set` and
        :meth:`~tredis.RedisClient.expire` inside an
        :meth:`~tredis.RedisClient.multi` /
        :meth:`~tredis.RedisClient.exec` block. It is provided as a faster
        alternative to the given sequence of operations, because this operation
        is very common when Redis is used as a cache.

        An error is returned when seconds is invalid.

        .. versionadded:: 0.2.0

        .. note:: **Time complexity**: ``O(1)``

        :param key: The key to set
        :type key: :class:`str`, :class:`bytes`
        :param int seconds: Number of seconds for TTL
        :param value: The value to set
        :type value: :class:`str`, :class:`bytes`
        :rtype: bool
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute([b'SETEX', key, ascii(seconds), value], b'OK')

    def setnx(self, key, value):
        """Set key to hold string value if key does not exist. In that case, it
        is equal to :meth:`~tredis.RedisClient.setnx`. When key already holds a
        value, no operation is performed. :meth:`~tredis.RedisClient.setnx` is
        short for "SET if Not eXists".

        .. versionadded:: 0.2.0

        .. note:: **Time complexity**: ``O(1)``

        :param key: The key to set
        :type key: :class:`str`, :class:`bytes`
        :param value: The value to set
        :type value: :class:`str`, :class:`bytes`, :class:`int`
        :rtype: bool
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute([b'SETNX', key, value], 1)

    def setrange(self, key, offset, value):
        """Overwrites part of the string stored at key, starting at the
        specified offset, for the entire length of value. If the offset is
        larger than the current length of the string at key, the string is
        padded with zero-bytes to make offset fit. Non-existing keys are
        considered as empty strings, so this command will make sure it holds a
        string large enough to be able to set value at offset.

        .. note:: The maximum offset that you can set is 2 :sup:`29` -1
           (536870911), as Redis Strings are limited to 512 megabytes. If you
           need to grow beyond this size, you can use multiple keys.

        .. warning:: When setting the last possible byte and the string value
           stored at key does not yet hold a string value, or holds a small
           string value, Redis needs to allocate all intermediate memory which
           can block the server for some time. On a 2010 MacBook Pro, setting
           byte number 536870911 (512MB allocation) takes ~300ms, setting byte
           number 134217728 (128MB allocation) takes ~80ms, setting bit number
           33554432 (32MB allocation) takes ~30ms and setting bit number
           8388608 (8MB allocation) takes ~8ms. Note that once this first
           allocation is done, subsequent calls to
           :meth:`~tredis.RedisClient.setrange` for the same key will not have
           the allocation overhead.

        .. versionadded:: 0.2.0

        .. note:: **Time complexity**: ``O(1)``, not counting the time taken to
           copy the new string in place. Usually, this string is very small so
           the amortized complexity is ``O(1)``. Otherwise, complexity is
           ``O(M)`` with ``M`` being the length of the value argument.

        :param key: The key to get the bit from
        :type key: :class:`str`, :class:`bytes`
        :param value: The value to set
        :type value: :class:`str`, :class:`bytes`, :class:`int`
        :returns: The length of the string after it was modified by the command
        :rtype: int
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute([b'SETRANGE', key, ascii(offset), value])

    def strlen(self, key):
        """Returns the length of the string value stored at key. An error is
        returned when key holds a non-string value

        .. versionadded:: 0.2.0

        .. note:: **Time complexity**: ``O(1)``

        :param key: The key to set
        :type key: :class:`str`, :class:`bytes`
        :returns: The length of the string at key, or 0 when key does not exist
        :rtype: int
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute([b'STRLEN', key])
