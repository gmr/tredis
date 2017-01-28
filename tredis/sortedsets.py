"""Redis Sorted Set Commands Mixin"""


class SortedSetsMixin(object):
    """Redis Sorted Set Commands Mixin"""

    def zadd(self, key, *members, **kwargs):
        """Adds all the specified members with the specified scores to the
        sorted set stored at key. It is possible to specify multiple score /
        member pairs. If a specified member is already a member of the sorted
        set, the score is updated and the element reinserted at the right
        position to ensure the correct ordering.

        If key does not exist, a new sorted set with the specified members as
        sole members is created, like if the sorted set was empty. If the key
        exists but does not hold a sorted set, an error is returned.

        The score values should be the string representation of a double
        precision floating point number. +inf and -inf values are valid values
        as well.

        **Members parameters**

        ``members`` could be either:
        - a single dict where keys correspond to scores and values to elements
        - multiple strings paired as score then element

        .. code::

            yield client.zadd('myzset', {'1': 'one', '2': 'two'})
            yield client.zadd('myzset', '1', 'one', '2', 'two')

        **ZADD options (Redis 3.0.2 or greater)**
        ZADD supports a list of options. Options are:
        ``xx``: Only update elements that already exist. Never add elements.
        ``nx``: Don't update already existing elements. Always add new elements.
        ``ch``: Modify the return value from the number of new elements added,
        to the total number of elements changed (CH is an abbreviation of
        changed). Changed elements are new elements added and elements already
        existing for which the score was updated. So elements specified in the
        command having the same score as they had in the past are not counted.
        Note: normally the return value of ZADD only counts the number of new
        elements added.
        ``incr``: When this option is specified ZADD acts like
        :meth:`~tredis.RedisClient.zincrby`. Only one score-element pair can be
        specified in this mode.

        .. note::

           **Time complexity**: ``O(log(N))`` for each item added, where ``N``
           is the number of elements in the sorted set.

        :param key: The key of the sorted set
        :type key: :class:`str`, :class:`bytes`
        :param members: Elements to add
        :type members: :class:`dict`, :class:`str`, :class:`bytes`
        :keyword bool xx: Only update elements that already exist
        :keyword bool nx: Don't update already existing elements
        :keyword bool ch: Return the number of changed elements
        :keyword bool incr: Increment the score of an element
        :rtype: int, :class:`str`, :class:`bytes`
        :returns: Number of elements changed, or the new score if incr is set
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        xx = kwargs.pop('xx', False)
        nx = kwargs.pop('nx', False)
        ch = kwargs.pop('ch', False)
        incr = kwargs.pop('incr', False)
        command = [b'ZADD', key]
        if xx:
            command += ['XX']
        if nx:
            command += ['NX']
        if ch:
            command += ['CH']
        if incr:
            command += ['INCR']

        if len(members) == 1:
            for k in members[0]:
                command += [k, members[0][k]]
        else:
            command += list(members)
        return self._execute(command)

    def zrangebyscore(self,
                      key,
                      min_score,
                      max_score,
                      with_scores=False,
                      offset=0,
                      count=0):
        """Returns all the elements in the sorted set at key with a score
        between min and max (including elements with score equal to min or
        max). The elements are considered to be ordered from low to high
        scores.

        The elements having the same score are returned in lexicographical
        order (this follows from a property of the sorted set implementation in
        Redis and does not involve further computation).

        The optional ``offset`` and ``count`` arguments can be used to only get
        a range of the matching elements (similar to SELECT LIMIT offset, count
        in SQL). Keep in mind that if offset is large, the sorted set needs to
        be traversed for offset elements before getting to the elements to
        return, which can add up to ``O(N)`` time complexity.

        The optional ``with_scores`` argument makes the command return both the
        element and its score, instead of the element alone. This option is
        available since Redis 2.0.

        **Exclusive intervals and infinity**

        ``min_score`` and ``max_score`` can be ``-inf`` and ``+inf``, so that
        you are not required to know the highest or lowest score in the sorted
        set to get all elements from or up to a certain score.

        By default, the interval specified by ``min_score`` and ``max_score``
        is closed (inclusive). It is possible to specify an open interval
        (exclusive) by prefixing the score with the character ``(``. For
        example:

        .. code::

            ZRANGEBYSCORE zset (1 5

        Will return all elements with ``1 < score <= 5`` while:

        .. code::

            ZRANGEBYSCORE zset (5 (10

        Will return all the elements with ``5 < score < 10`` (5 and 10
        excluded).

        .. note::

           **Time complexity**: ``O(log(N)+M)`` with ``N`` being the number of
           elements in the sorted set and ``M`` the number of elements being
           returned. If ``M`` is constant (e.g. always asking for the first
           10 elements with ``count``), you can consider it ``O(log(N))``.

        :param key: The key of the sorted set
        :type key: :class:`str`, :class:`bytes`
        :param min_score: Lowest score definition
        :type min_score: :class:`str`, :class:`bytes`
        :param max_score: Highest score definition
        :type max_score: :class:`str`, :class:`bytes`
        :param bool with_scores: Return elements and scores
        :param offset: The number of elements to skip
        :type min_score: :class:`str`, :class:`bytes`
        :param count: The number of elements to return
        :type min_score: :class:`str`, :class:`bytes`
        :rtype: list
        :raises: :exc:`~tredis.exceptions.RedisError`
        """
        command = [b'ZRANGEBYSCORE', key, min_score, max_score]
        if with_scores:
            command += ['WITHSCORES']
        if offset or count:
            command += ['LIMIT', offset, count]
        return self._execute(command)

    def zrem(self, key, *members):
        """Removes the specified members from the sorted set stored at key.
         Non existing members are ignored.

        An error is returned when key exists and does not hold a sorted set.

        .. note::

           **Time complexity**: ``O(M*log(N))`` with ``N`` being the number of
           elements in the sorted set and ``M`` the number of elements to be
           removed.

        :param key: The key of the sorted set
        :type key: :class:`str`, :class:`bytes`
        :param members: One or more member values to remove
        :type members: :class:`str`, :class:`bytes`
        :rtype: int
        :raises: :exc:`~tredis.exceptions.RedisError`
        """
        return self._execute([b'ZREM', key] + list(members))

    def zremrangebyscore(self, key, min_score, max_score):
        """Removes all elements in the sorted set stored at key with a score
        between min and max.

        Intervals are described in :meth:`~tredis.RedisClient.zrangebyscore`.

        Returns the number of elements removed.

        .. note::

           **Time complexity**: ``O(log(N)+M)`` with ``N`` being the number of
           elements in the sorted set and M the number of elements removed by
           the operation.

        :param key: The key of the sorted set
        :type key: :class:`str`, :class:`bytes`
        :param min_score: Lowest score definition
        :type min_score: :class:`str`, :class:`bytes`
        :param max_score: Highest score definition
        :type max_score: :class:`str`, :class:`bytes`
        :rtype: int
        :raises: :exc:`~tredis.exceptions.RedisError`
        """
        return self._execute([b'ZREMRANGEBYSCORE', key, min_score, max_score])
