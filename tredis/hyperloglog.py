"""Redis HyperLogLog Commands Mixin"""


class HyperLogLogMixin(object):
    """Redis HyperLogLog Commands Mixin"""

    def pfadd(self, key, *elements):
        """Adds all the element arguments to the HyperLogLog data structure
        stored at the variable name specified as first argument.

        As a side effect of this command the HyperLogLog internals may be
        updated to reflect a different estimation of the number of unique items
        added so far (the cardinality of the set).

        If the approximated cardinality estimated by the HyperLogLog changed
        after executing the command, :meth:`~tredis.RedisClient.pfadd` returns
        ``1``, otherwise ``0`` is returned. The command automatically creates
        an empty HyperLogLog structure (that is, a Redis String of a specified
        length and with a given encoding) if the specified key does not exist.

        To call the command without elements but just the variable name is
        valid, this will result into no operation performed if the variable
        already exists, or just the creation of the data structure if the key
        does not exist (in the latter case ``1`` is returned).

        For an introduction to HyperLogLog data structure check
        :meth:`~tredis.RedisClient.pfcount`.

        .. versionadded:: 0.2.0

        .. note:: **Time complexity**: ``O(1)`` to add every element.

        :param key: The key to add the elements to
        :type key: :class:`str`, :class:`bytes`
        :param elements: One or more elements to add
        :type elements: :class:`str`, :class:`bytes`
        :rtype: bool
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute([b'PFADD', key] + list(elements), 1)

    def pfcount(self, *keys):
        """When called with a single key, returns the approximated cardinality
        computed by the HyperLogLog data structure stored at the specified
        variable, which is ``0`` if the variable does not exist.

        When called with multiple keys, returns the approximated cardinality of
        the union of the HyperLogLogs passed, by internally merging the
        HyperLogLogs stored at the provided keys into a temporary HyperLogLog.

        The HyperLogLog data structure can be used in order to count unique
        elements in a set using just a small constant amount of memory,
        specifically 12k bytes for every HyperLogLog (plus a few bytes for the
        key itself).

        The returned cardinality of the observed set is not exact, but
        approximated with a standard error of 0.81%.

        For example in order to take the count of all the unique search queries
        performed in a day, a program needs to call
        :meth:`~tredis.RedisCount.pfcount` every time a query is processed. The
        estimated number of unique queries can be retrieved with
        :meth:`~tredis.RedisCount.pfcount` at any time.

        .. note:: as a side effect of calling this function, it is possible
           that the HyperLogLog is modified, since the last 8 bytes encode the
           latest computed cardinality for caching purposes. So
           :meth:`~tredis.RedisCount.pfcount` is technically a write command.

        .. versionadded:: 0.2.0

        .. note:: **Time complexity**: ``O(1)`` with every small average
           constant times when called with a single key. ``O(N)`` with ``N``
           being the number of keys, and much bigger constant times, when
           called with multiple keys.

        :param keys: One or more keys
        :type keys: :class:`str`, :class:`bytes`
        :rtype: int
        :returns: The approximated number of unique elements observed
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute([b'PFCOUNT'] + list(keys))

    def pfmerge(self, dest_key, *keys):
        """Merge multiple HyperLogLog values into an unique value that will
        approximate the cardinality of the union of the observed Sets of the
        source HyperLogLog structures.

        The computed merged HyperLogLog is set to the destination variable,
        which is created if does not exist (defaulting to an empty
        HyperLogLog).

        .. versionadded:: 0.2.0

        .. note::

           **Time complexity**: ``O(N)`` to merge ``N`` HyperLogLogs, but
           with high constant times.

        :param dest_key: The destination key
        :type dest_key: :class:`str`, :class:`bytes`
        :param keys: One or more keys
        :type keys: :class:`str`, :class:`bytes`
        :rtype: bool
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute([b'PFMERGE', dest_key] + list(keys), b'OK')
