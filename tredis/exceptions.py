"""TRedis Exceptions"""

class TRedisException(Exception):
    """Raised as a top-level exception class for all exceptions raised by
    :py:class:`RedisClient <tredis.RedisClient>`.

    """
    pass


class ConnectError(TRedisException):
    """Raised when :py:class:`RedisClient <tredis.RedisClient>` can not connect
    to the specified Redis server.

    """
    pass


class ConnectionError(TRedisException):
    """Raised when :py:class:`RedisClient <tredis.RedisClient>` has had its
    connection to the Redis server interrupted unexpectedly.

    """
    pass


class AuthError(TRedisException):
    """Raised when :py:meth:`RedisClient.auth <tredis.RedisClient.auth>` is
    invoked and the Redis server returns an error.

    """
    pass


class RedisError(TRedisException):
    """Raised when the Redis server returns a error to
    :py:class:`RedisClient <tredis.RedisClient>`. The string representation
    of this class will contain the error response from the Redis server,
    if one is sent.

    """
    pass
