import os
import logging


def setup_module():
    try:
        with open('build/test-environment') as env_file:
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
                    logging.debug('setting environment variable %s = %r',
                                  name, value)
                    os.environ[name] = value
                else:
                    logging.debug('clearing environment variable %s', name)
                    os.environ.pop('name', None)

    except IOError:
        logging.info('expected environment file at build/env-vars')

