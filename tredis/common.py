"""
Common utility methods

"""


def parse_info_value(value):
    """

    :param value:
    :return:

    """
    try:
        if '.' in value:
            return float(value)
        else:
            return int(value)
    except ValueError:
        if ',' in value or '=' in value:
            retval = {}
            for row in value.split(','):
                key, val = row.rsplit('=', 1)
                retval[key] = parse_info_value(val)
            return retval
        return value


def format_info_response(value):
    """Format the response from redis

    :param str value: The return response from redis
    :rtype: dict

    """
    info = {}
    for line in value.decode('utf-8').splitlines():
        if not line or line[0] == '#':
            continue
        if ':' in line:
            key, value = line.split(':', 1)
            info[key] = parse_info_value(value)
    return info
