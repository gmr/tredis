#!/usr/bin/env python
#

import setuptools

classifiers = ['Development Status :: 5 - Production/Stable',
               'Intended Audience :: Developers',
               'License :: OSI Approved :: BSD License',
               'Operating System :: OS Independent',
               'Programming Language :: Python :: 2',
               'Programming Language :: Python :: 2.7',
               'Programming Language :: Python :: 3',
               'Programming Language :: Python :: 3.3',
               'Programming Language :: Python :: 3.4',
               'Programming Language :: Python :: 3.5',
               'Topic :: Communications', 'Topic :: Internet',
               'Topic :: Software Development :: Libraries',
               'Topic :: Database']

requirements = ['tornado>4.0', 'hiredis>=0.2.0,<1']
tests_require = ['nose', 'mock', 'codecov', 'coverage']

setuptools.setup(name='tredis',
                 version='0.6.0',
                 description='An asynchronous Redis client for Tornado',
                 long_description=open('README.rst').read(),
                 author='Gavin M. Roy',
                 author_email='gavinmroy@gmail.com',
                 url='http://github.com/gmr/tredis',
                 packages=['tredis'],
                 package_data={'': ['LICENSE', 'README.rst']},
                 include_package_data=True,
                 install_requires=requirements,
                 tests_require=tests_require,
                 license='BSD',
                 classifiers=classifiers)
