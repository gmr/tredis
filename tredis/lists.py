"""Redis List Commands Mixin"""


class ListsMixin(object):
    """Redis List Commands Mixin"""

    def llen(self, key):
        """
        Returns the length of the list stored at key.

        :param key: The list's key
        :type key: :class:`str`, :class:`bytes`
        :rtype: int
        :raises: :exc:`~tredis.exceptions.TRedisException`

        If key does not exist, it is interpreted as an empty list and 0 is
        returned. An error is returned when the value stored at key is not a
        list.

        .. note::

           **Time complexity** ``O(1)``

        """
        return self._execute([b'LLEN', key])

    def lrange(self, key, start, end):
        """
        Returns the specified elements of the list stored at key.

        :param key: The list's key
        :type key: :class:`str`, :class:`bytes`
        :param int start: zero-based index to start retrieving elements from
        :param int end: zero-based index at which to stop retrieving elements

        :rtype: list
        :raises: :exc:`~tredis.exceptions.TRedisException`

        The offsets start and stop are zero-based indexes, with 0 being the
        first element of the list (the head of the list), 1 being the next
        element and so on.

        These offsets can also be negative numbers indicating offsets
        starting at the end of the list. For example, -1 is the last element
        of the list, -2 the penultimate, and so on.

        Note that if you have a list of numbers from 0 to 100,
        ``lrange(key, 0, 10)`` will return 11 elements, that is, the
        rightmost item is included. This may or may not be consistent with
        behavior of range-related functions in your programming language of
        choice (think Ruby's ``Range.new``, ``Array#slice`` or Python's
        :func:`range` function).

        Out of range indexes will not produce an error. If start is larger
        than the end of the list, an empty list is returned. If stop is
        larger than the actual end of the list, Redis will treat it like the
        last element of the list.

        .. note::

           **Time complexity** ``O(S+N)`` where ``S`` is the distance of
           start offset from ``HEAD`` for small lists, from nearest end
           (``HEAD`` or ``TAIL``) for large lists; and ``N`` is the number
           of elements in the specified range.

        """
        return self._execute([b'LRANGE', key, start, end])

    def ltrim(self, key, start, stop):
        """
        Crop a list to the specified range.

        :param key: The list's key
        :type key: :class:`str`, :class:`bytes`
        :param int start: zero-based index to first element to retain
        :param int stop: zero-based index of the last element to retain
        :returns: did the operation succeed?
        :rtype: bool
        :raises: :exc:`~tredis.exceptions.TRedisException`

        Trim an existing list so that it will contain only the specified
        range of elements specified.

        Both `start` and `stop` are zero-based indexes, where 0 is the first
        element of the list (the head), 1 the next element and so on.
        For example: ``ltrim('foobar', 0, 2)`` will modify the list stored at
        ``foobar`` so that only the first three elements of the list will
        remain.

        `start` and `stop` can also be negative numbers indicating offsets
        from the end of the list, where -1 is the last element of the list,
        -2 the penultimate element and so on.

        Out of range indexes will not produce an error: if `start` is larger
        than the `end` of the list, or `start > end`, the result will be an
        empty list (which causes `key` to be removed). If `end` is larger
        than the end of the list, Redis will treat it like the last element
        of the list.

        A common use of LTRIM is together with LPUSH / RPUSH. For example::

            client.lpush('mylist', 'somelement')
            client.ltrim('mylist', 0, 99)

        This pair of commands will push a new element on the list, while
        making sure that the list will not grow larger than 100 elements.
        This is very useful when using Redis to store logs for example. It is
        important to note that when used in this way LTRIM is an O(1)
        operation because in the average case just one element is removed
        from the tail of the list.

        .. note::

           Time complexity: ``O(N)`` where `N` is the number of elements to
           be removed by the operation.

        """
        return self._execute([b'LTRIM', key, start, stop], b'OK')

    def lpush(self, key, *values):
        """
        Insert all the specified values at the head of the list stored at key.

        :param key: The list's key
        :type key: :class:`str`, :class:`bytes`
        :param values: One or more positional arguments to insert at the
            beginning of the list.  Each value is inserted at the beginning
            of the list individually (see discussion below).
        :returns: the length of the list after push operations
        :rtype: int
        :raises: :exc:`~tredis.exceptions.TRedisException`

        If `key` does not exist, it is created as empty list before
        performing the push operations. When key holds a value that is not a
        list, an error is returned.

        It is possible to push multiple elements using a single command call
        just specifying multiple arguments at the end of the command.
        Elements are inserted one after the other to the head of the list,
        from the leftmost element to the rightmost element. So for instance
        ``client.lpush('mylist', 'a', 'b', 'c')`` will result into a list
        containing ``c`` as first element, ``b`` as second element and ``a``
        as third element.

        .. note::

           **Time complexity**: ``O(1)``

        """
        return self._execute([b'LPUSH', key] + list(values))

    def lpushx(self, key, *values):
        """
        Insert values at the head of an existing list.

        :param key: The list's key
        :type key: :class:`str`, :class:`bytes`
        :param values: One or more positional arguments to insert at the
            beginning of the list.  Each value is inserted at the beginning
            of the list individually (see discussion below).
        :returns: the length of the list after push operations, zero if
            `key` does not refer to a list
        :rtype: int
        :raises: :exc:`~tredis.exceptions.TRedisException`

        This method inserts `values` at the head of the list stored at `key`,
        only if `key` already exists and holds a list. In contrary to
        :meth:`.lpush`, no operation will be performed when key does not yet
        exist.

        .. note::

           **Time complexity**: ``O(1)``

        """
        return self._execute([b'LPUSHX', key] + list(values))

    def lpop(self, key):
        """
        Removes and returns the first element of the list stored at key.

        :param key: The list's key
        :type key: :class:`str`, :class:`bytes`
        :returns: the element at the head of the list, :data:`None` if the
            list does not exist
        :raises: :exc:`~tredis.exceptions.TRedisException`

        .. note::

           **Time complexity**: ``O(1)``

        """
        return self._execute([b'LPOP', key])

    def rpush(self, key, *values):
        """
        Insert all the specified values at the tail of the list stored at key.

        :param key: The list's key
        :type key: :class:`str`, :class:`bytes`
        :param values: One or more positional arguments to insert at the
            tail of the list.
        :returns: the length of the list after push operations
        :rtype: int
        :raises: :exc:`~tredis.exceptions.TRedisException`

        If `key` does not exist, it is created as empty list before performing
        the push operation. When `key` holds a value that is not a list, an
        error is returned.

        It is possible to push multiple elements using a single command call
        just specifying multiple arguments at the end of the command.
        Elements are inserted one after the other to the tail of the list,
        from the leftmost element to the rightmost element. So for instance
        the command  ``client.rpush('mylist', 'a', 'b', 'c')`` will result
        in a list containing ``a`` as first element, ``b`` as second element
        and ``c`` as third element.

        .. note::

           **Time complexity**: ``O(1)``

        """
        return self._execute([b'RPUSH', key] + list(values))

    def rpushx(self, key, *values):
        """
        Insert values at the tail of an existing list.

        :param key: The list's key
        :type key: :class:`str`, :class:`bytes`
        :param values: One or more positional arguments to insert at the
            tail of the list.
        :returns: the length of the list after push operations or
            zero if `key` does not refer to a list
        :rtype: int
        :raises: :exc:`~tredis.exceptions.TRedisException`

        This method inserts value at the tail of the list stored at `key`,
        only if `key` already exists and holds a list. In contrary to
        method:`.rpush`, no operation will be performed when `key` does not
        yet exist.

        .. note::

           **Time complexity**: ``O(1)``

        """
        return self._execute([b'RPUSHX', key] + list(values))

    def rpop(self, key):
        """
        Removes and returns the last element of the list stored at key.

        :param key: The list's key
        :type key: :class:`str`, :class:`bytes`
        :returns: the length of the list after push operations or
            zero if `key` does not refer to a list
        :returns: the element at the tail of the list, :data:`None` if the
            list does not exist
        :rtype: int
        :raises: :exc:`~tredis.exceptions.TRedisException`

        """
        return self._execute([b'RPOP', key])
