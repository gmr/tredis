"""
Common Utility Functions

"""


def ascii(value):
    """Return the string of value

    :param mixed value: The value to return
    :rtype: str

    """
    return '{0}'.format(value)


def is_ok(response, future):
    """Method invoked in a lambda to abbreviate the amount of code in
    each method when checking for an ``OK`` response.

    :param concurrent.Future response: The RedisClient._execute future
    :param concurrent.Future future: The current method's future

    """
    exc = response.exception()
    if exc:
        future.set_exception(exc)
    else:
        result = response.result()
        future.set_result(result == b'OK')
