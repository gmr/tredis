import os
import logging


try:
    with open('build/env-vars') as env_file:
        for line_num, line in enumerate(env_file):
            if line.startswith('export '):
                line = line[7:].strip()
            name, sep, value = line.partition('=')
            name, value = name.strip(), value.strip()
            if sep != '=':
                logging.warning('ignoring line %d in environment file - %s',
                                line_num, line)
                continue

            if value:
                logging.info('setting environment variable %s = %r',
                             name, value)
                os.environ[name] = value
            else:
                logging.info('clearing environment variable %s', name)
                os.environ.pop('name', None)
except IOError:
    logging.info('expected environment file at build/env-vars')
