"""
Common utility methods

"""


def parse_info_value(value):
    """

    :param value:
    :return:

    """
    try:
        if b'.' in value:
            return float(value)
        else:
            return int(value)
    except ValueError:
        if b',' in value or b'=' in value:
            retval = {}
            for row in value.split(b','):
                key, val = row.rsplit(b'=', 1)
                retval[key.decode('utf-8')] = parse_info_value(val)
            return retval
        return value.decode('utf-8')


def format_info_response(value):
    """Format the response from redis

    :param str value: The return response from redis
    :rtype: dict

    """
    info = {}
    for line in value.splitlines():
        if line.startswith(b'#'):
            continue
        if b':' in line:
            key, value = line.split(b':', 1)
            info[key.decode('utf-8')] = parse_info_value(value)
    return info
