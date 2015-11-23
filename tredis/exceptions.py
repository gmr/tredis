"""TRedis Exceptions"""


class TRedisException(Exception):
    """Raised as a top-level exception class for all exceptions raised by
    :class:`~tredis.RedisClient`.

    """
    pass


class ConnectError(TRedisException):
    """Raised when :class:`~tredis.RedisClient` can not connect to the
    specified Redis server.

    """
    pass


class ConnectionError(TRedisException):
    """Raised when :class:`~tredis.RedisClient` has had its connection to the
    Redis server interrupted unexpectedly.

    """
    pass


class AuthError(TRedisException):
    """Raised when :meth:`~tredis.RedisClient.auth` is invoked and the Redis
    server returns an error.

    """
    pass


class RedisError(TRedisException):
    """Raised when the Redis server returns a error to
    :class:`~tredis.RedisClient`. The string representation of this class will
    contain the error response from the Redis server, if one is sent.

    """
    pass


class SubscribedError(TRedisException):
    """Raised when a client is subscribed via
    :meth:`~tredis.RedisClient.subscribe` or
    :meth:`~tredis.RedisClient.psubscribe` and a command other than
    :meth:`~tredis.RedisClient.subscribe`,
    :meth:`~tredis.RedisClient.unsubscribe`,
    :meth:`~tredis.RedisClient.psubscribe`, or
    :meth:`~tredis.RedisClient.punsubscribe` was requested. Once the client
    enters the subscribed state it is not supposed to issue any other commands.

    """
    pass
