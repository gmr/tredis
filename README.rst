TRedis
======
An simple asynchronous Redis client for Tornado

|Version| |Downloads| |Status| |Coverage| |CodeClimate| |License| |PythonVersions|

Example
-------

.. code:: python

   client = tredis.RedisClient()
   yield client.connect()

   yield client.set('foo', 'bar')
   value = yield client.get('foo')

.. |Version| image:: https://img.shields.io/pypi/v/tredis.svg?
   :target: https://pypi.python.org/pypi/tredis

.. |Status| image:: https://img.shields.io/travis/gmr/tredis.svg?
   :target: https://travis-ci.org/gmr/tredis

.. |Coverage| image:: https://img.shields.io/codecov/c/github/gmr/tredis.svg?
   :target: https://codecov.io/github/gmr/tredis?branch=master

.. |Downloads| image:: https://img.shields.io/pypi/dm/tredis.svg?
   :target: https://pypi.python.org/pypi/tredis

.. |License| image:: https://img.shields.io/github/license/gmr/tredis.svg?
   :target: https://github.com/gmr/tredis

.. |CodeClimate| image:: https://img.shields.io/codeclimate/github/gmr/tredis.svg?
   :target: https://codeclimate.com/github/gmr/tredis

.. |PythonVersions| image:: https://img.shields.io/pypi/pyversions/tredis.svg?
   :target: https://github.com/gmr/tredis
