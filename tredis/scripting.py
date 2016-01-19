"""Redis Scripting Commands Mixin"""


class ScriptingMixin(object):
    """Redis Scripting Commands Mixin"""

    def eval(self, script, keys=None, args=None):
        """:meth:`~tredis.RedisClient.eval` and
        :meth:`~tredis.RedisClient.evalsha` are used to evaluate scripts using
        the Lua interpreter built into Redis starting from version 2.6.0.

        The first argument of EVAL is a Lua 5.1 script. The script does not
        need to define a Lua function (and should not). It is just a Lua
        program that will run in the context of the Redis server.

        .. note::

           **Time complexity**: Depends on the script that is executed.

        :param str script: The Lua script to execute
        :param list keys: A list of keys to pass into the script
        :param list args: A list of args to pass into the script
        :return: mixed

        """
        if not keys:
            keys = []
        if not args:
            args = []
        return self._execute([b'EVAL', script, str(len(keys))] + keys + args)

    def evalsha(self, sha1, keys=None, args=None):
        """Evaluates a script cached on the server side by its SHA1 digest.
        Scripts are cached on the server side using the
        :meth:`~tredis.RedisClient.script_load` command. The command is
        otherwise identical to :meth:`~tredis.RedisClient.eval`.

        .. note::

           **Time complexity**: Depends on the script that is executed.

        :param str sha1: The sha1 hash of the script to execute
        :param list keys: A list of keys to pass into the script
        :param list args: A list of args to pass into the script
        :return: mixed

        """
        if not keys:
            keys = []
        if not args:
            args = []
        return self._execute([b'EVALSHA', sha1, str(len(keys))] + keys + args)

    def script_exists(self, *hashes):
        """Returns information about the existence of the scripts in the script
        cache.

        This command accepts one or more SHA1 digests and returns a list of
        ones or zeros to signal if the scripts are already defined or not
        inside the script cache. This can be useful before a pipelining
        operation to ensure that scripts are loaded (and if not, to load them
        using :meth:`~tredis.RedisClient.script_load`) so that the pipelining
        operation can be performed solely using
        :meth:`~tredis.RedisClient.evalsha` instead of
        :meth:`~tredis.RedisClient.eval` to save bandwidth.

        Please refer to the :meth:`~tredis.RedisClient.eval` documentation for
        detailed information about Redis Lua scripting.

        .. note::

           **Time complexity**: ``O(N)`` with ``N`` being the number of scripts
           to check (so checking a single script is an ``O(1)`` operation).

        :param str hashes: One or more sha1 hashes to check for in the cache
        :rtype: list
        :return: Returns a list of ``1`` or ``0`` indicating if the specified
            script(s) exist in the cache.

        """
        return self._execute([b'SCRIPT', b'EXISTS'] + list(hashes))

    def script_flush(self):
        """Flush the Lua scripts cache.

        Please refer to the :meth:`~tredis.RedisClient.eval` documentation for
        detailed information about Redis Lua scripting.

        .. note::

           **Time complexity**: ``O(N)`` with ``N`` being the number of scripts
           in cache

        :rtype: bool

        """
        return self._execute([b'SCRIPT', b'FLUSH'], b'OK')

    def script_kill(self):
        """Kills the currently executing Lua script, assuming no write
        operation was yet performed by the script.

        This command is mainly useful to kill a script that is running for too
        much time(for instance because it entered an infinite loop because of
        a bug). The script will be killed and the client currently blocked into
        :meth:`~tredis.RedisClient.eval` will see the command returning with an
        error.

        If the script already performed write operations it can not be killed
        in this way because it would violate Lua script atomicity contract. In
        such a case only SHUTDOWN NOSAVE is able to kill the script, killing
        the Redis process in an hard way preventing it to persist with
        half-written information.

        Please refer to the :meth:`~tredis.RedisClient.eval` documentation for
        detailed information about Redis Lua scripting.

        .. note::

           **Time complexity**: ``O(1)``

        :rtype: bool

        """
        return self._execute([b'SCRIPT', b'KILL'], b'OK')

    def script_load(self, script):
        """Load a script into the scripts cache, without executing it. After
        the specified command is loaded into the script cache it will be
        callable using :meth:`~tredis.RedisClient.evalsha` with the correct
        SHA1 digest of the script, exactly like after the first successful
        invocation of :meth:`~tredis.RedisClient.eval`.

        The script is guaranteed to stay in the script cache forever (unless
        :meth:`~tredis.RedisClient.script_flush` is called).

        The command works in the same way even if the script was already
        present in the script cache.

        Please refer to the :meth:`~tredis.RedisClient.eval` documentation for
        detailed information about Redis Lua scripting.

        .. note::

           **Time complexity**: ``O(N)`` with ``N`` being the length in bytes
           of the script body.

        :param str script: The script to load into the script cache
        :return: str

        """
        return self._execute([b'SCRIPT', b'LOAD', script])
