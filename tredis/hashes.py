"""Redis Hash Commands Mixin"""
from tornado import concurrent


class HashesMixin(object):
    """Redis Hash Commands Mixin"""

    def hset(self, key, field, value):
        """Sets `field` in the hash stored at `key` to `value`.

        If `key` does not exist, a new key holding a hash is created. If
        `field` already exists in the hash, it is overwritten.

        .. note::

           **Time complexity**: always ``O(1)``

        :param key: The key of the hash
        :type key: :class:`str`, :class:`bytes`
        :param field: The field in the hash to set
        :type key: :class:`str`, :class:`bytes`
        :param value: The value to set the field to
        :returns: ``1`` if `field` is a new field in the hash and `value`
            was set; otherwise, ``0`` if `field` already exists in the hash
            and the value was updated
        :rtype: int

        """
        return self._execute([b'HSET', key, field, value])

    def hget(self, key, field):
        """
        Returns the value associated with `field` in the hash stored at `key`.

        .. note::

           **Time complexity**: always ``O(1)``

        :param key: The key of the hash
        :type key: :class:`str`, :class:`bytes`
        :param field: The field in the hash to get
        :type key: :class:`str`, :class:`bytes`
        :rtype: bytes, list
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute([b'HGET', key, field])

    def hgetall(self, key):
        """
        Returns all fields and values of the has stored at `key`.

        The underlying redis `HGETALL`_ command returns an array of
        pairs.  This method converts that to a Python :class:`dict`.
        It will return an empty :class:`dict` when the key is not
        found.

        .. note::

           **Time complexity**: ``O(N)`` where ``N`` is the size
           of the hash.

        :param key: The key of the hash
        :type key: :class:`str`, :class:`bytes`
        :returns: a :class:`dict` of key to value mappings for all
            fields in the hash

        .. _HGETALL: http://redis.io/commands/hgetall

        """
        def format_response(value):
            return dict(zip(value[::2], value[1::2]))
        return self._execute([b'HGETALL', key],
                             format_callback=format_response)

    def hmset(self, key, value_dict):
        """
        Sets fields to values as in `value_dict` in the hash stored at `key`.

        Sets the specified fields to their respective values in the hash
        stored at `key`.  This command overwrites any specified fields
        already existing in the hash.  If `key` does not exist, a new  key
        holding a hash is created.

        .. note::

           **Time complexity**: ``O(N)`` where ``N`` is the number of
           fields being set.

        :param key: The key of the hash
        :type key: :class:`str`, :class:`bytes`
        :param value_dict: field to value mapping
        :type value_dict: :class:`dict`
        :rtype: bool
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        if not value_dict:
            future = concurrent.TracebackFuture()
            future.set_result(False)
        else:
            command = [b'HMSET', key]
            command.extend(sum(value_dict.items(), ()))
            future = self._execute(command)
        return future

    def hmget(self, key, *fields):
        """
        Returns the values associated with the specified `fields` in a hash.

        For every ``field`` that does not exist in the hash, :data:`None`
        is returned.  Because a non-existing keys are treated as empty
        hashes, calling :meth:`hmget` against a non-existing key will
        return a list of :data:`None` values.

        .. note::

           *Time complexity*: ``O(N)`` where ``N`` is the number of fields
           being requested.

        :param key: The key of the hash
        :type key: :class:`str`, :class:`bytes`
        :param fields: iterable of field names to retrieve
        :returns: a :class:`dict` of field name to value mappings for
            each of the requested fields
        :rtype: dict

        """
        def format_response(val_array):
            return dict(zip(fields, val_array))

        command = [b'HMGET', key]
        command.extend(fields)
        return self._execute(command, format_callback=format_response)

    def hdel(self, key, *fields):
        """
        Remove the specified fields from the hash stored at `key`.

        Specified fields that do not exist within this hash are ignored.
        If `key` does not exist, it is treated as an empty hash and this
        command returns zero.

        :param key: The key of the hash
        :type key: :class:`str`, :class:`bytes`
        :param fields: iterable of field names to retrieve
        :returns: the number of fields that were removed from the hash,
            not including specified by non-existing fields.
        :rtype: int

        """
        if not fields:
            future = concurrent.TracebackFuture()
            future.set_result(0)
        else:
            future = self._execute([b'HDEL', key] + list(fields))
        return future

    def hexists(self, key, field):
        """
        Returns if `field` is an existing field in the hash stored at `key`.

        .. note::

           *Time complexity*: ``O(1)``

        :param key: The key of the hash
        :type key: :class:`str`, :class:`bytes`
        :param field: name of the field to test for
        :type key: :class:`str`, :class:`bytes`
        :rtype: bool

        """
        return self._execute([b'HEXISTS', key, field])

    def hincrby(self, key, field, increment):
        """
        Increments the number stored at `field` in the hash stored at `key`.

        If `key` does not exist, a new key holding a hash is created.  If
        `field` does not exist the value is set to ``0`` before the operation
        is performed.  The range of values supported is limited to 64-bit
        signed integers.

        :param key: The key of the hash
        :type key: :class:`str`, :class:`bytes`
        :param field: name of the field to increment
        :type key: :class:`str`, :class:`bytes`
        :param increment: amount to increment by
        :type increment: int

        :returns: the value at `field` after the increment occurs
        :rtype: int

        """
        return self._execute([b'HINCRBY', key, field, increment],
                             format_callback=int)

    def hincrbyfloat(self, key, field, increment):
        """
        Increments the number stored at `field` in the hash stored at `key`.

        If the increment value is negative, the result is to have the hash
        field **decremented** instead of incremented.  If the field does not
        exist, it is set to ``0`` before performing the operation.  An error
        is returned if one of the following conditions occur:

        - the field contains a value of the wrong type (not a string)
        - the current field content or the specified increment are not
          parseable as a double precision floating point number

        .. note::

           *Time complexity*: ``O(1)``

        :param key: The key of the hash
        :type key: :class:`str`, :class:`bytes`
        :param field: name of the field to increment
        :type key: :class:`str`, :class:`bytes`
        :param increment: amount to increment by
        :type increment: float

        :returns: the value at `field` after the increment occurs
        :rtype: float

        """
        return self._execute([b'HINCRBYFLOAT', key, field, increment],
                             format_callback=float)

    def hkeys(self, key):
        """
        Returns all field names in the hash stored at `key`.

        .. note::

           *Time complexity*: ``O(N)`` where ``N`` is the size of the hash

        :param key: The key of the hash
        :type key: :class:`str`, :class:`bytes`
        :returns: the list of fields in the hash
        :rtype: list

        """
        return self._execute([b'HKEYS', key])

    def hlen(self, key):
        """
        Returns the number of fields contained in the hash stored at `key`.

        .. note::

           *Time complexity*: ``O(1)``

        :param key: The key of the hash
        :type key: :class:`str`, :class:`bytes`
        :returns: the number of fields in the hash or zero when `key`
            does not exist
        :rtype: int

        """
        return self._execute([b'HLEN', key])

    def hsetnx(self, key, field, value):
        """
        Sets `field` in the hash stored at `key` only if it does not exist.

        Sets `field` in the hash stored at `key` only if `field` does not
        yet exist.  If `key` does not exist, a new key holding a hash is
        created.  If `field` already exists, this operation has no effect.

        .. note::

           *Time complexity*: ``O(1)``

        :param key: The key of the hash
        :type key: :class:`str`, :class:`bytes`
        :param field: The field in the hash to set
        :type key: :class:`str`, :class:`bytes`
        :param value: The value to set the field to
        :returns: ``1`` if `field` is a new field in the hash and `value`
            was set.  ``0`` if `field` already exists in the hash and
            no operation was performed
        :rtype: int

        """
        return self._execute([b'HSETNX', key, field, value])

    def hvals(self, key):
        """
        Returns all values in the hash stored at `key`.

        .. note::

           *Time complexity* ``O(N)`` where ``N`` is the size of the hash

        :param key: The key of the hash
        :type key: :class:`str`, :class:`bytes`
        :returns: a :class:`list` of :class:`bytes` instances or an
            empty list when `key` does not exist
        :rtype: list

        """
        return self._execute([b'HVALS', key])
