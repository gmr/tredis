from tornado import concurrent

# Python 2 support for ascii()
if 'ascii' not in dir(__builtins__):  # pragma: nocover
    from tredis.compat import ascii


class KeysMixin(object):

    def delete(self, *keys):
        """Removes the specified keys. A key is ignored if it does not exist.
        Returns ``True`` if all keys are removed. If more than one key is
        passed in and not all keys are remove, the number of removed keys is
        returned.

        **Time complexity**: O(N) where N is the number of keys that will be
        removed. When a key to remove holds a value other than a string, the
        individual complexity for this key is O(M) where M is the number of
        elements in the list, set, sorted set or hash. Removing a single key
        that holds a string value is O(1).

        :param keys: One or more keys to remove
        :type keys: str, bytes
        :rtype: bool
        :raises: :py:class:`RedisError <tredis.exceptions.RedisError>`

        """
        future = concurrent.TracebackFuture()

        def on_response(response):
            exc = response.exception()
            if exc:
                future.set_exception(exc)
            else:
                result = response.result()
                if result == len(keys):
                    future.set_result(True)
                else:
                    future.set_result(result)

        self._execute([b'DEL'] + list(keys), on_response)
        return future

    def dump(self, key):
        """Serialize the value stored at key in a Redis-specific format and
        return it to the user. The returned value can be synthesized back into
        a Redis key using the :py:meth:`restore <tredis.RedisClient.restore>`
        command.

        The serialization format is opaque and non-standard, however it has a
        few semantic characteristics:

          - It contains a 64-bit checksum that is used to make sure errors
            will be detected. The
            :py:meth:`restore <tredis.RedisClient.restore>` command makes sure
            to check the checksum before synthesizing a key using the serialized
            value.
          - Values are encoded in the same format used by RDB.
          - An RDB version is encoded inside the serialized value, so that
            different Redis versions with incompatible RDB formats will
            refuse to process the serialized value.
          - The serialized value does NOT contain expire information. In
            order to capture the time to live of the current value the
            :py:meth:`pttl <tredis.RedisClient.pttl>` command should be used.

        If key does not exist ``None`` is returned.

        **Time complexity**: O(1) to access the key and additional O(N*M) to
        serialized it, where N is the number of Redis objects composing the
        value and M their average size. For small string values the time
        complexity is thus O(1)+O(1*M) where M is small, so simply O(1).

        :param key: The key to dump
        :type key: str, bytes
        :rtype: bytes, None

        """
        return self._execute([b'DUMP', key])

    def exists(self, key):
        """Returns ``True`` if the key exists.

        **Time complexity**: O(1)

        **Command Type**: String

        :param key: One or more keys to check for
        :type key: str, bytes
        :rtype: bool
        :raises: :py:class:`RedisError <tredis.exceptions.RedisError>`

        """
        return self._execute_with_bool_response([b'EXISTS', key])

    def expire(self, key, timeout):
        """Set a timeout on key. After the timeout has expired, the key will
        automatically be deleted. A key with an associated timeout is often
        said to be volatile in Redis terminology.

        The timeout is cleared only when the key is removed using the
        :py:meth:`delete <tredis.RedisClient.delete>` method or overwritten
        using the :py:meth:`set <tredis.RedisClient.set>` or
        :py:meth:`getset <tredis.RedisClient.getset>` methods. This means that
        all the operations that conceptually alter the value stored at the key
        without replacing it with a new one will leave the timeout untouched.
        For instance, incrementing the value of a key with
        :py:meth:`incr <tredis.RedisClient.incr>`, pushing a new value into a
        list with :py:meth:`incr <tredis.RedisClient.lpush>`, or altering the
        field value of a hash with :py:meth:`hset <tredis.RedisClient.hset>`
        are all operations that will leave the timeout untouched.

        The timeout can also be cleared, turning the key back into a
        persistent key, using the
        :py:meth:`persist <tredis.RedisClient.persist>` method.

        If a key is renamed with :py:meth:`rename <tredis.RedisClient.rename>`,
        the associated time to live is transferred to the new key name.

        If a key is overwritten by
        :py:meth:`rename <tredis.RedisClient.rename>`, like in the case of an
        existing key ``Key_A`` that is overwritten by a call like
        ``client.rename(Key_B, Key_A)`` it does not matter if the original
        ``Key_A`` had a timeout associated or not, the new key ``Key_A`` will
        inherit all the characteristics of ``Key_B``.

        **Time complexity**: O(1)

        :param key: The key to set an expiration for
        :type key: str, bytes
        :param int timeout: The number of seconds to set the timeout to
        :rtype: bool
        :raises: :py:class:`RedisError <tredis.exceptions.RedisError>`

        """
        return self._execute_with_bool_response(
            [b'EXPIRE', key, ascii(timeout).encode('ascii')])

    def expireat(self, key, timestamp):
        """:py:class:`expireat <tredis.RedisClient.expireat>` has the same
        effect and semantic as :py:class:`expire <tredis.RedisClient.expire>`,
        but instead of specifying the number of seconds representing the
        TTL (time to live), it takes an absolute Unix timestamp (seconds since
        January 1, 1970).

        Please for the specific semantics of the command refer to the
        documentation of :py:class:`expire <tredis.RedisClient.expire>`.

        **Command Type**: Key

        :param key: The key to set an expiration for
        :type key: str, bytes
        :param int timestamp: The UNIX epoch value for the expiration
        :rtype: bool
        :raises: :py:class:`RedisError <tredis.exceptions.RedisError>`

        """
        return self._execute_with_bool_response(
            [b'EXPIREAT', key, ascii(timestamp).encode('ascii')])

    def keys(self, pattern):
        """Returns all keys matching pattern.

        While the time complexity for this operation is O(N), the constant
        times are fairly low. For example, Redis running on an entry level
        laptop can scan a 1 million key database in 40 milliseconds.

        **Warning**: consider :py:class:`keys <tredis.RedisClient.keys>` as a
        command that should only be used in production environments with
        extreme care. It may ruin performance when it is executed against
        large databases. This command is intended for debugging and special
        operations, such as changing your keyspace layout. Don't use
        :py:class:`keys <tredis.RedisClient.keys>` in your regular application
        code. If you're looking for a way to find keys in a subset of your
        keyspace, consider using :py:class:`scan <tredis.RedisClient.scan>`
        or sets.

        Supported glob-style patterns:

         - h?llo matches hello, hallo and hxllo
         - h*llo matches hllo and heeeello
         - h[ae]llo matches hello and hallo, but not hillo
         - h[^e]llo matches hallo, hbllo, ... but not hello
         - h[a-b]llo matches hallo and hbllo

        Use \ to escape special characters if you want to match them verbatim.

        **Time complexity**: O(N)

        :param pattern: The pattern to use when looking for keys
        :type pattern: str, bytes
        :type: list

        """
        future = concurrent.TracebackFuture()

        def on_response(values):
            future.set_result(values.result())
        self._execute([b'KEYS', pattern], on_response)
        return future

    def move(self, key, db):
        """Move key from the currently selected database (see
        :py:class:`select <tredis.RedisClient.select>`) to the specified
        destination database. When key already exists in the destination
        database, or it does not exist in the source database, it does
        nothing. It is possible to use
        :py:class:`move <tredis.RedisClient.move>` as a locking primitive
        because of this.

        **Time complexity**: O(1)

        :param key: The key to move
        :type key: str, bytes
        :param int db: The database number
        :rtype: bool
        :raises: :py:class:`RedisError <tredis.exceptions.RedisError>`

        """
        return self._execute_with_bool_response([b'MOVE', key,
                                                 ascii(db).encode('ascii')])

    def persist(self, key):
        """Remove the existing timeout on key, turning the key from volatile
        (a key with an expire set) to persistent (a key that will never expire
        as no timeout is associated).

        **Time complexity**: O(1)

        :param key: The key to move
        :type key: str, bytes
        :rtype: bool
        :raises: :py:class:`RedisError <tredis.exceptions.RedisError>`

        """
        return self._execute_with_bool_response([b'PERSIST', key])

    def pexpire(self, key, timeout):
        """This command works exactly like
        :py:class:`pexpire <tredis.RedisClient.pexpire>` but the time to live
        of the key is specified in milliseconds instead of seconds.

        **Time complexity**: O(1)

        :param key: The key to set an expiration for
        :type key: str, bytes
        :param int timeout: The number of milliseconds to set the timeout to
        :rtype: bool
        :raises: :py:class:`RedisError <tredis.exceptions.RedisError>`

        """
        return self._execute_with_bool_response(
            [b'PEXPIRE', key, ascii(timeout).encode('ascii')])

    def pexpireat(self, key, timestamp):
        """:py:class:`pexpireat <tredis.RedisClient.pexpireat>` has the same
        effect and semantic as
        :py:class:`expireat <tredis.RedisClient.expireat>`, but the Unix time
        at which the key will expire is specified in milliseconds instead of
        seconds.

        **Time complexity**: O(1)

        :param key: The key to set an expiration for
        :type key: str, bytes
        :param int timestamp: The expiration UNIX epoch value in milliseconds
        :rtype: bool
        :raises: :py:class:`RedisError <tredis.exceptions.RedisError>`

        """
        return self._execute_with_bool_response(
            [b'PEXPIREAT', key, ascii(timestamp).encode('ascii')])

    def pttl(self, key):
        """Like :py:class:`ttl <tredis.RedisClient.ttl>` this command returns
        the remaining time to live of a key that has an expire set, with the
        sole difference that :py:class:`ttl <tredis.RedisClient.ttl>` returns
        the amount of remaining time in seconds while
        :py:class:`pttl <tredis.RedisClient.pttl>` returns it in milliseconds.

        In Redis 2.6 or older the command returns ``-1`` if the key does not
        exist or if the key exist but has no associated expire.

        Starting with Redis 2.8 the return value in case of error changed:

         - The command returns ``-2`` if the key does not exist.
         - The command returns ``-1`` if the key exists but has no associated
           expire.

        **Time complexity**: O(1)

        :param key: The key to get the PTTL for
        :type key: str, bytes
        :rtype: int
        :raises: :py:class:`RedisError <tredis.exceptions.RedisError>`

        """
        return self._execute([b'PTTL', key])

    def randomkey(self):
        """Return a random key from the currently selected database.

        **Time complexity**: O(1)

        :rtype: bytes
        :raises: :py:class:`RedisError <tredis.exceptions.RedisError>`

        """
        return self._execute([b'RANDOMKEY'])

    def rename(self, key, new_key):
        """Renames ``key`` to ``new_key``. It returns an error when the source
        and destination names are the same, or when ``key`` does not exist.
        If ``new_key`` already exists it is overwritten, when this happens
        :py:class:`rename <tredis.RedisClient.rename>` executes an implicit
        :py:class:`delete <tredis.RedisClient.delete>` operation, so if the
        deleted key contains a very big value it may cause high latency even
        if :py:class:`rename <tredis.RedisClient.rename>` itself is usually a
        constant-time operation.

        **Time complexity**: O(1)

        :param key: The key to rename
        :type key: str, bytes
        :param new_key: The key to rename it to
        :type new_key: str, bytes
        :rtype: int
        :raises: :py:class:`RedisError <tredis.exceptions.RedisError>`

        """
        future = concurrent.TracebackFuture()
        self._execute([b'RENAME', key, new_key],
                      lambda response: self._is_ok(response, future))
        return future

    def renamenx(self, key, new_key):
        """Renames ``key`` to ``new_key`` if ``new_key`` does not yet exist.
        It returns an error under the same conditions as
        :py:class:`rename <tredis.RedisClient.rename>`.

        **Time complexity**: O(1)

        :param key: The key to rename
        :type key: str, bytes
        :param new_key: The key to rename it to
        :type new_key: str, bytes
        :rtype: int
        :raises: :py:class:`RedisError <tredis.exceptions.RedisError>`

        """
        future = concurrent.TracebackFuture()
        self._execute([b'RENAMENX', key, new_key],
                      lambda response: self._is_ok(response, future))
        return future

    def restore(self, key, ttl, serialized_value, replace=False):
        """Create a key associated with a value that is obtained by
        deserializing the provided serialized value (obtained via
        :py:class:`dump <tredis.RedisClient.dump>`).

        If ``ttl`` is ``0`` the key is created without any expire, otherwise
        the specified expire time (in milliseconds) is set.

        :py:class:`restore <tredis.RedisClient.restore>` will return a
        ``Target key name is busy`` error when key already exists unless you
        use the :py:class:`restore <tredis.RedisClient.restore>` modifier
        (Redis 3.0 or greater).

        :py:class:`restore <tredis.RedisClient.restore>` checks the RDB
        version and data checksum. If they don't match an error is returned.

        **Time complexity:** O(1) to create the new key and additional O(N*M)
        to reconstruct the serialized value, where N is the number of Redis
        objects composing the value and M their average size. For small string
        values the time complexity is thus O(1)+O(1*M) where M is small, so
        simply O(1). However for sorted set values the complexity is
        O(N*M*log(N)) because inserting values into sorted sets is O(log(N)).

        :param key: The key to get the TTL for
        :type key: str, bytes
        :param int ttl: The number of seconds to set the timeout to
        :param serialized_value: The value to restore to the key
        :type serialized_value: str, bytes
        :param bool replace: Replace a pre-existing key
        :rtype: bool

        """
        future = concurrent.TracebackFuture()
        commands = [b'RESTORE', key, ttl, serialized_value]
        if replace:
            commands.append(b'REPLACE')
        self._execute(commands,
                      lambda response: self._is_ok(response, future))
        return future

    def scan(self, cursor=0, pattern=None, count=None):
        """The SCAN command and the closely related commands
        :py:class:`sscan <tredis.RedisClient.sscan>`,
        :py:class:`hscan <tredis.RedisClient.hscan>` and
        :py:class:`zscan <tredis.RedisClient.zscan>` are used in order to
        incrementally iterate over a collection of elements.

        - :py:class:`scan <tredis.RedisClient.scan>` iterates the set of keys
          in the currently selected Redis database.
        - :py:class:`sscan <tredis.RedisClient.sscan>` iterates elements of
          Sets types.
        - :py:class:`hscan <tredis.RedisClient.hscan>` iterates fields of Hash
          types and their associated values.
        - :py:class:`zscan <tredis.RedisClient.zscan>` iterates elements of
          Sorted Set types and their associated scores.

        **SCAN basic usage**

        :py:class:`scan <tredis.RedisClient.scan>` is a cursor based iterator.
        This means that at every call of the command, the server returns an
        updated cursor that the user needs to use as the cursor argument in
        the next call.

        An iteration starts when the cursor is set to ``0``, and terminates
        when the cursor returned by the server is ``0``.

        For more information on :py:class:`scan <tredis.RedisClient.scan>`,
        visit the `Redis docs on scan <http://redis.io/commands/scan>`_.

        **Time complexity**: O(1) for every call. O(N) for a complete
        iteration, including enough command calls for the cursor to return
        back to 0. N is the number of elements inside the collection.

        :param int cursor: The server specified cursor value or ``0``
        :param pattern: An optional pattern to apply for key matching
        :type pattern: str, bytes
        :param int count: An optional amount of work to perform in the scan
        :return:

        """
        command = [b'SCAN', ascii(cursor).encode('ascii')]
        if pattern:
            command += [b'MATCH', pattern]
        if count:
            command += [b'COUNT', ascii(count).encode('ascii')]
        return self._execute(command)

    def ttl(self, key):
        """Returns the remaining time to live of a key that has a timeout.
        This introspection capability allows a Redis client to check how many
        seconds a given key will continue to be part of the dataset.

        **Time complexity**: O(1)

        :param key: The key to get the TTL for
        :type key: str, bytes
        :rtype: int
        :raises: :py:class:`RedisError <tredis.exceptions.RedisError>`

        """
        return self._execute([b'TTL', key])

    def type(self, key):
        """Returns the string representation of the type of the value stored at
        key. The different types that can be returned are: string, list, set,
        zset and hash.

        **Time complexity**: O(1)

        :param key: The key to get the type for
        :type key: str, bytes
        :rtype: bytes
        :raises: :py:class:`RedisError <tredis.exceptions.RedisError>`

        """
        return self._execute([b'TYPE', key])
