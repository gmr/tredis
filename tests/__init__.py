import contextlib
import os
import logging
import socket


def setup_module():
    try:
        with open('build/env-vars') as env_file:
            for line_num, line in enumerate(env_file):
                if line.startswith('export '):
                    line = line[7:].strip()
                name, sep, value = line.partition('=')
                name, value = name.strip(), value.strip()
                if sep != '=':
                    logging.warning(
                        'ignoring line %d in environment file - %s',
                        line_num, line)
                elif value:
                    logging.info('setting environment variable %s = %r',
                                 name, value)
                    os.environ[name] = value
                else:
                    logging.info('clearing environment variable %s', name)
                    os.environ.pop('name', None)

    except IOError:
        logging.info('expected environment file at build/env-vars')

    # Make the redis slave a slave of the first redis instance.
    #
    # The public IP address of the redis master is used instead of the
    # docker internal address.  Unfortunately this results in a broken
    # replica but a working client ... the client assumes that it can
    # connect to the master address and port returned by the INFO
    # REPLICATION command.
    #
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)
    with contextlib.closing(s):
        sockaddr = (os.environ['DOCKER_IP'],
                    int(os.environ['REDISSLAVE_PORT']))
        logging.info('making %r a slave of redis1', sockaddr)
        s.connect(sockaddr)
        s.send('SLAVEOF NO ONE\r\n'.encode('ASCII'))

        cmd = 'SLAVEOF {0} {1}\r\n'.format(os.environ['DOCKER_IP'],
                                           os.environ['REDIS_PORT'])
        s.send(cmd.encode('ASCII'))
