class RedisError(Exception):
    """Raised as a top-level exception class for all exceptions raised by
    :py:class:`RedisClient <tredis.RedisClient>`. The string representation
    of this class will contain the error resposne from the Redis server,
    if one is sent.

    """
    pass


class ConnectError(RedisError):
    """Raised when :py:class:`RedisClient <tredis.RedisClient>` can not connect
    to the specified Redis server.

    """
    pass


class AuthError(RedisError):
    """Raised when :py:meth:`RedisClient.auth <tredis.RedisClient.auth>` is
    invoked and the Redis server returns an error.

    """
    pass
