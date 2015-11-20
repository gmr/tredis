"""Redis Key Commands Mixin"""
from tornado import concurrent

# Python 2 support for ascii()
if 'ascii' not in dir(__builtins__):  # pragma: nocover
    from tredis.compat import ascii


class KeysMixin(object):
    """Redis Key Commands Mixin"""

    def delete(self, *keys):
        """Removes the specified keys. A key is ignored if it does not exist.
        Returns :data:`True` if all keys are removed.

        .. note::

           **Time complexity**: ``O(N)`` where ``N`` is the number of keys that
           will be removed. When a key to remove holds a value other than a
           string, the individual complexity for this key is ``O(M)`` where
           ``M`` is the number of elements in the list, set, sorted set or
           hash. Removing a single key that holds a string value is ``O(1)``.

        :param keys: One or more keys to remove
        :type keys: :class:`str`, :class:`bytes`
        :rtype: bool
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute([b'DEL'] + list(keys), len(keys))

    def dump(self, key):
        """Serialize the value stored at key in a Redis-specific format and
        return it to the user. The returned value can be synthesized back into
        a Redis key using the :meth:`~tredis.RedisClient.restore` command.

        The serialization format is opaque and non-standard, however it has a
        few semantic characteristics:

          - It contains a 64-bit checksum that is used to make sure errors
            will be detected. The :meth:`~tredis.RedisClient.restore` command
            makes sure to check the checksum before synthesizing a key using
            the serialized value.
          - Values are encoded in the same format used by RDB.
          - An RDB version is encoded inside the serialized value, so that
            different Redis versions with incompatible RDB formats will
            refuse to process the serialized value.
          - The serialized value does NOT contain expire information. In
            order to capture the time to live of the current value the
            :meth:`~tredis.RedisClient.pttl` command should be used.

        If key does not exist :data:`None` is returned.

        .. note::

           **Time complexity**: ``O(1)`` to access the key and additional
           ``O(N*M)`` to serialized it, where N is the number of Redis objects
           composing the value and ``M`` their average size. For small string
           values the time complexity is thus ``O(1)+O(1*M)`` where ``M`` is
           small, so simply ``O(1)``.

        :param key: The key to dump
        :type key: :class:`str`, :class:`bytes`
        :rtype: bytes, None

        """
        return self._execute([b'DUMP', key])

    def exists(self, key):
        """Returns :data:`True` if the key exists.

        .. note::

           **Time complexity**: ``O(1)``

        **Command Type**: String

        :param key: One or more keys to check for
        :type key: :class:`str`, :class:`bytes`
        :rtype: bool
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute([b'EXISTS', key], 1)

    def expire(self, key, timeout):
        """Set a timeout on key. After the timeout has expired, the key will
        automatically be deleted. A key with an associated timeout is often
        said to be volatile in Redis terminology.

        The timeout is cleared only when the key is removed using the
        :meth:`~tredis.RedisClient.delete` method or overwritten using the
        :meth:`~tredis.RedisClient.set` or :meth:`~tredis.RedisClient.getset`
        methods. This means that all the operations that conceptually alter the
        value stored at the key without replacing it with a new one will leave
        the timeout untouched. For instance, incrementing the value of a key
        with :meth:`~tredis.RedisClient.incr`, pushing a new value into a
        list with :meth:`~tredis.RedisClient.lpush`, or altering the field
        value of a hash with :meth:`~tredis.RedisClient.hset` are all
        operations that will leave the timeout untouched.

        The timeout can also be cleared, turning the key back into a persistent
        key, using the :meth:`~tredis.RedisClient.persist` method.

        If a key is renamed with :meth:`~tredis.RedisClient.rename`,
        the associated time to live is transferred to the new key name.

        If a key is overwritten by :meth:`~tredis.RedisClient.rename`, like in
        the case of an existing key ``Key_A`` that is overwritten by a call
        like ``client.rename(Key_B, Key_A)`` it does not matter if the original
        ``Key_A`` had a timeout associated or not, the new key ``Key_A`` will
        inherit all the characteristics of ``Key_B``.

        .. note::

           **Time complexity**: ``O(1)``

        :param key: The key to set an expiration for
        :type key: :class:`str`, :class:`bytes`
        :param int timeout: The number of seconds to set the timeout to
        :rtype: bool
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute(
            [b'EXPIRE', key, ascii(timeout).encode('ascii')], 1)

    def expireat(self, key, timestamp):
        """:meth:`~tredis.RedisClient.expireat` has the same effect and
        semantic as :meth:`~tredis.RedisClient.expire`, but instead of
        specifying the number of seconds representing the TTL (time to live),
        it takes an absolute Unix timestamp (seconds since January 1, 1970).

        Please for the specific semantics of the command refer to the
        documentation of :meth:`~tredis.RedisClient.expire`.

        .. note::

           **Time complexity**: ``O(1)``

        :param key: The key to set an expiration for
        :type key: :class:`str`, :class:`bytes`
        :param int timestamp: The UNIX epoch value for the expiration
        :rtype: bool
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute(
            [b'EXPIREAT', key, ascii(timestamp).encode('ascii')], 1)

    def keys(self, pattern):
        """Returns all keys matching pattern.

        While the time complexity for this operation is ``O(N)``, the constant
        times are fairly low. For example, Redis running on an entry level
        laptop can scan a 1 million key database in 40 milliseconds.

        .. warning:: Consider :meth:`~tredis.RedisClient.keys` as a
           command that should only be used in production environments with
           extreme care. It may ruin performance when it is executed against
           large databases. This command is intended for debugging and special
           operations, such as changing your keyspace layout. Don't use
           :meth:`~tredis.RedisClient.keys` in your regular application code.
           If you're looking for a way to find keys in a subset of your
           keyspace, consider using :meth:`~tredis.RedisClient.scan` or sets.

        Supported glob-style patterns:

         - ``h?llo`` matches ``hello``, ``hallo`` and ``hxllo``
         - ``h*llo`` matches ``hllo`` and ``heeeello``
         - ``h[ae]llo`` matches ``hello`` and ``hallo``, but not ``hillo``
         - ``h[^e]llo`` matches ``hallo``, ``hbllo``, but not ``hello``
         - ``h[a-b]llo`` matches ``hallo`` and ``hbllo``

        Use ``\`` to escape special characters if you want to match them
        verbatim.

        .. note::

           **Time complexity**: ``O(N)``

        :param pattern: The pattern to use when looking for keys
        :type pattern: :class:`str`, :class:`bytes`
        :rtype: list
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute([b'KEYS', pattern])

    def migrate(self,
                host,
                port,
                key,
                destination_db,
                timeout,
                copy=False,
                replace=False):
        """Atomically transfer a key from a source Redis instance to a
        destination Redis instance. On success the key is deleted from the
        original instance and is guaranteed to exist in the target instance.

        The command is atomic and blocks the two instances for the time
        required to transfer the key, at any given time the key will appear to
        exist in a given instance or in the other instance, unless a timeout
        error occurs.

        .. note::

           **Time complexity**: This command actually executes a DUMP+DEL in
           the source instance, and a RESTORE in the target instance. See the
           pages of these commands for time complexity. Also an ``O(N)`` data
           transfer between the two instances is performed.

        :param host: The host to migrate the key to
        :type host: bytes, str
        :param int port: The port to connect on
        :param key: The key to migrate
        :type key: bytes, str
        :param int destination_db: The database number to select
        :param int timeout: The maximum idle time in milliseconds
        :param bool copy: Do not remove the key from the local instance
        :param bool replace: Replace existing key on the remote instance
        :rtype: bool
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        command = [b'MIGRATE', host, ascii(port).encode('ascii'), key,
                   ascii(destination_db).encode('ascii'),
                   ascii(timeout).encode('ascii')]
        if copy is True:
            command.append(b'COPY')
        if replace is True:
            command.append(b'REPLACE')
        return self._execute(command, b'OK')

    def move(self, key, db):
        """Move key from the currently selected database (see
        :meth:`~tredis.RedisClient.select`) to the specified destination
        database. When key already exists in the destination database, or it
        does not exist in the source database, it does nothing. It is possible
        to use :meth:`~tredis.RedisClient.move` as a locking primitive because
        of this.

        .. note::

           **Time complexity**: ``O(1)``

        :param key: The key to move
        :type key: :class:`str`, :class:`bytes`
        :param int db: The database number
        :rtype: bool
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute([b'MOVE', key, ascii(db).encode('ascii')], 1)

    def object_encoding(self, key):
        """Return the kind of internal representation used in order to store
        the value associated with a key

        .. note::

           **Time complexity**: ``O(1)``

        :param key: The key to get the encoding for
        :type key: :class:`str`, :class:`bytes`
        :rtype: bytes
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute([b'OBJECT', b'ENCODING', key])

    def object_idle_time(self, key):
        """Return the number of seconds since the object stored at the
        specified key is idle (not requested by read or write operations).
        While the value is returned in seconds the actual resolution of this
        timer is 10 seconds, but may vary in future implementations of Redis.

        .. note::

           **Time complexity**: ``O(1)``

        :param key: The key to get the idle time for
        :type key: :class:`str`, :class:`bytes`
        :rtype: int
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute([b'OBJECT', b'IDLETIME', key])

    def object_refcount(self, key):
        """Return the number of references of the value associated with the
        specified key. This command is mainly useful for debugging.

        .. note::

           **Time complexity**: ``O(1)``

        :param key: The key to get the refcount for
        :type key: :class:`str`, :class:`bytes`
        :rtype: int
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute([b'OBJECT', b'REFCOUNT', key])

    def persist(self, key):
        """Remove the existing timeout on key, turning the key from volatile
        (a key with an expire set) to persistent (a key that will never expire
        as no timeout is associated).

         .. note::

           **Time complexity**: ``O(1)``

        :param key: The key to move
        :type key: :class:`str`, :class:`bytes`
        :rtype: bool
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute([b'PERSIST', key], 1)

    def pexpire(self, key, timeout):
        """This command works exactly like :meth:`~tredis.RedisClient.pexpire`
        but the time to live of the key is specified in milliseconds instead of
        seconds.

        .. note::

           **Time complexity**: ``O(1)``

        :param key: The key to set an expiration for
        :type key: :class:`str`, :class:`bytes`
        :param int timeout: The number of milliseconds to set the timeout to
        :rtype: bool
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute(
            [b'PEXPIRE', key, ascii(timeout).encode('ascii')], 1)

    def pexpireat(self, key, timestamp):
        """:meth:`~tredis.RedisClient.pexpireat` has the same effect and
        semantic as :meth:`~tredis.RedisClient.expireat`, but the Unix time
        at which the key will expire is specified in milliseconds instead of
        seconds.

        .. note::

           **Time complexity**: ``O(1)``

        :param key: The key to set an expiration for
        :type key: :class:`str`, :class:`bytes`
        :param int timestamp: The expiration UNIX epoch value in milliseconds
        :rtype: bool
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute(
            [b'PEXPIREAT', key, ascii(timestamp).encode('ascii')], 1)

    def pttl(self, key):
        """Like :meth:`~tredis.RedisClient.ttl` this command returns the
        remaining time to live of a key that has an expire set, with the sole
        difference that :meth:`~tredis.RedisClient.ttl` returns the amount of
        remaining time in seconds while :meth:`~tredis.RedisClient.pttl`
        returns it in milliseconds.

        In Redis 2.6 or older the command returns ``-1`` if the key does not
        exist or if the key exist but has no associated expire.

        Starting with Redis 2.8 the return value in case of error changed:

         - The command returns ``-2`` if the key does not exist.
         - The command returns ``-1`` if the key exists but has no associated
           expire.

        .. note::

           **Time complexity**: ``O(1)``

        :param key: The key to get the PTTL for
        :type key: :class:`str`, :class:`bytes`
        :rtype: int
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute([b'PTTL', key])

    def randomkey(self):
        """Return a random key from the currently selected database.

        .. note::

           **Time complexity**: ``O(1)``

        :rtype: bytes
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute([b'RANDOMKEY'])

    def rename(self, key, new_key):
        """Renames ``key`` to ``new_key``. It returns an error when the source
        and destination names are the same, or when ``key`` does not exist.
        If ``new_key`` already exists it is overwritten, when this happens
        :meth:`~tredis.RedisClient.rename` executes an implicit
        :meth:`~tredis.RedisClient.delete` operation, so if the deleted key
        contains a very big value it may cause high latency even if
        :meth:`~tredis.RedisClient.rename` itself is usually a constant-time
        operation.

        .. note::

           **Time complexity**: ``O(1)``

        :param key: The key to rename
        :type key: :class:`str`, :class:`bytes`
        :param new_key: The key to rename it to
        :type new_key: :class:`str`, :class:`bytes`
        :rtype: bool
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute([b'RENAME', key, new_key], b'OK')

    def renamenx(self, key, new_key):
        """Renames ``key`` to ``new_key`` if ``new_key`` does not yet exist.
        It returns an error under the same conditions as
        :meth:`~tredis.RedisClient.rename`.

        .. note::

           **Time complexity**: ``O(1)``

        :param key: The key to rename
        :type key: :class:`str`, :class:`bytes`
        :param new_key: The key to rename it to
        :type new_key: :class:`str`, :class:`bytes`
        :rtype: bool
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute([b'RENAMENX', key, new_key], 1)

    def restore(self, key, ttl, value, replace=False):
        """Create a key associated with a value that is obtained by
        deserializing the provided serialized value (obtained via
        :meth:`~tredis.RedisClient.dump`).

        If ``ttl`` is ``0`` the key is created without any expire, otherwise
        the specified expire time (in milliseconds) is set.

        :meth:`~tredis.RedisClient.restore` will return a
        ``Target key name is busy`` error when key already exists unless you
        use the :meth:`~tredis.RedisClient.restore` modifier (Redis 3.0 or
        greater).

        :meth:`~tredis.RedisClient.restore` checks the RDB version and data
        checksum. If they don't match an error is returned.

        .. note::

           **Time complexity**: ``O(1)`` to create the new key and additional
           ``O(N*M)`` to reconstruct the serialized value, where ``N`` is the
           number of Redis objects composing the value and ``M`` their average
           size. For small string values the time complexity is thus
           ``O(1)+O(1*M)`` where ``M`` is small, so simply ``O(1)``. However
           for sorted set values the complexity is ``O(N*M*log(N))`` because
           inserting values into sorted sets is ``O(log(N))``.

        :param key: The key to get the TTL for
        :type key: :class:`str`, :class:`bytes`
        :param int ttl: The number of seconds to set the timeout to
        :param value: The value to restore to the key
        :type value: :class:`str`, :class:`bytes`
        :param bool replace: Replace a pre-existing key
        :rtype: bool
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        command = [b'RESTORE', key, ascii(ttl).encode('ascii'), value]
        if replace:
            command.append(b'REPLACE')
        return self._execute(command, b'OK')

    def scan(self, cursor=0, pattern=None, count=None):
        """The :meth:`~tredis.RedisClient.scan` command and the closely related
        commands :meth:`~tredis.RedisClient.sscan`,
        :meth:`~tredis.RedisClient.hscan` and :meth:`~tredis.RedisClient.zscan`
        are used in order to incrementally iterate over a collection of
        elements.

        - :meth:`~tredis.RedisClient.scan` iterates the set of keys in the
          currently selected Redis database.
        - :meth:`~tredis.RedisClient.sscan` iterates elements of Sets types.
        - :meth:`~tredis.RedisClient.hscan` iterates fields of Hash types and
          their associated values.
        - :meth:`~tredis.RedisClient.zscan` iterates elements of Sorted Set
          types and their associated scores.

        **Basic usage**

        :meth:`~tredis.RedisClient.scan` is a cursor based iterator.
        This means that at every call of the command, the server returns an
        updated cursor that the user needs to use as the cursor argument in
        the next call.

        An iteration starts when the cursor is set to ``0``, and terminates
        when the cursor returned by the server is ``0``.

        For more information on :meth:`~tredis.RedisClient.scan`,
        visit the `Redis docs on scan <http://redis.io/commands/scan>`_.

        .. note::

           **Time complexity**: ``O(1)`` for every call. ``O(N)`` for a
           complete iteration, including enough command calls for the cursor to
           return back to ``0``. ``N`` is the number of elements inside the
           collection.

        :param int cursor: The server specified cursor value or ``0``
        :param pattern: An optional pattern to apply for key matching
        :type pattern: :class:`str`, :class:`bytes`
        :param int count: An optional amount of work to perform in the scan
        :rtype: int, list
        :returns: A tuple containing the cursor and the list of keys
        :raises: :exc:`~tredis.exceptions.RedisError`

        """

        def format_response(value):
            """Format the response from redis

            :param tuple value: The return response from redis
            :rtype: tuple(int, list)

            """
            return int(value[0]), value[1]

        command = [b'SCAN', ascii(cursor).encode('ascii')]
        if pattern:
            command += [b'MATCH', pattern]
        if count:
            command += [b'COUNT', ascii(count).encode('ascii')]
        print(command)
        return self._execute(command, format_callback=format_response)

    def sort(self,
             key,
             by=None,
             external=None,
             offset=0,
             limit=None,
             order=None,
             alpha=False,
             store_as=None):
        """Returns or stores the elements contained in the list, set or sorted
        set at key. By default, sorting is numeric and elements are compared by
        their value interpreted as double precision floating point number.

        The ``external`` parameter is used to specify the
        `GET <http://redis.io/commands/sort#retrieving-external-keys>_`
        parameter for retrieving external keys. It can be a single string
        or a list of strings.

        .. note::

           **Time complexity**: ``O(N+M*log(M))`` where ``N`` is the number of
           elements in the list or set to sort, and ``M`` the number of
           returned elements. When the elements are not sorted, complexity is
           currently ``O(N)`` as there is a copy step that will be avoided in
           next releases.

        :param key: The key to get the refcount for
        :type key: :class:`str`, :class:`bytes`

        :param by: The optional pattern for external sorting keys
        :type by: :class:`str`, :class:`bytes`
        :param external: Pattern or list of patterns to return external keys
        :type external: :class:`str`, :class:`bytes`, list
        :param int offset: The starting offset when using limit
        :param int limit: The number of elements to return
        :param order: The sort order - one of ``ASC`` or ``DESC``
        :type order: :class:`str`, :class:`bytes`
        :param bool alpha: Sort the results lexicographically
        :param store_as: When specified, the key to store the results as
        :type store_as: :class:`str`, :class:`bytes`, None
        :rtype: list|int
        :raises: :exc:`~tredis.exceptions.RedisError`
        :raises: :exc:`ValueError`

        """
        if order and order not in [b'ASC', b'DESC', 'ASC', 'DESC']:
            raise ValueError('invalid sort order "{}"'.format(order))

        command = [b'SORT', key]
        if by:
            command += [b'BY', by]
        if external and isinstance(external, list):
            for entry in external:
                command += [b'GET', entry]
        elif external:
            command += [b'GET', external]
        if limit:
            command += [b'LIMIT', ascii(offset).encode('utf-8'),
                        ascii(limit).encode('utf-8')]
        if order:
            command.append(order)
        if alpha is True:
            command.append(b'ALPHA')
        if store_as:
            command += [b'STORE', store_as]

        return self._execute(command)

    def ttl(self, key):
        """Returns the remaining time to live of a key that has a timeout.
        This introspection capability allows a Redis client to check how many
        seconds a given key will continue to be part of the dataset.

        .. note::

           **Time complexity**: ``O(1)``

        :param key: The key to get the TTL for
        :type key: :class:`str`, :class:`bytes`
        :rtype: int
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute([b'TTL', key])

    def type(self, key):
        """Returns the string representation of the type of the value stored at
        key. The different types that can be returned are: ``string``,
        ``list``, ``set``, ``zset``, and ``hash``.

        .. note::

           **Time complexity**: ``O(1)``

        :param key: The key to get the type for
        :type key: :class:`str`, :class:`bytes`
        :rtype: bytes
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute([b'TYPE', key])

    def wait(self, num_slaves, timeout=0):
        """his command blocks the current client until all the previous write
        commands are successfully transferred and acknowledged by at least the
        specified number of slaves. If the timeout, specified in milliseconds,
        is reached, the command returns even if the specified number of slaves
        were not yet reached.

        The command will always return the number of slaves that acknowledged
        the write commands sent before the :meth:`~tredis.RedisClient.wait`
        command, both in the case where the specified number of slaves are
        reached, or when the timeout is reached.

        .. note::

           **Time complexity**: ``O(1)``

        :param int num_slaves: Number of slaves to acknowledge previous writes
        :param int timeout: Timeout in milliseconds
        :rtype: int
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        command = [b'WAIT', ascii(num_slaves).encode('ascii'),
                   ascii(timeout).encode('ascii')]
        return self._execute(command)
