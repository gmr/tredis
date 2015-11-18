TRedis
======
An asynchronous Redis client for Tornado

|Version| |Downloads| |Status| |Coverage| |CodeClimate| |PythonVersions|

Commands Implemented
--------------------
TRedis is a work in progress and not all commands are implemented. The following
list details each command category and the number of commands implemented in each.

If you need functionality that is not yet implemented, follow the patterns for
the category mixins that are complete and submit a PR!

+--------------+----------+
| Category     | Count    |
+==============+==========+
| Cluster      | 0 of 20  |
+--------------+----------+
| Connection   | 5 of 5   |
+--------------+----------+
| Geo          | 0 of 6   |
+--------------+----------+
| Hashes       | 0 of 15  |
+--------------+----------+
| HyperLogLog  | 0 of 3   |
+--------------+----------+
| Keys         | 22 of 22 |
+--------------+----------+
| Lists        | 0 of 17  |
+--------------+----------+
| Pub/Sub      | 0 of 6   |
+--------------+----------+
| Scripting    | 0 of 6   |
+--------------+----------+
| Server       | 0 of 30  |
+--------------+----------+
| Sets         | 15 of 15 |
+--------------+----------+
| Sorted Sets  | 0 of 21  |
+--------------+----------+
| Strings      | 3 of 23  |
+--------------+----------+
| Transactions | 0 of 5   |
+--------------+----------+

Example
-------

.. code:: python

   client = tredis.RedisClient()
   yield client.connect()

   yield client.set('foo', 'bar')
   value = yield client.get('foo')

Local Development
-----------------
The development environment for tredis uses `docker-compose <https://docs.docker.com/compose/>`_
and `docker-machine <https://docs.docker.com/machine/>`_

To get setup in the environment and run the tests, take the following steps:
.. code:: bash

    virtualenv -p python3 env
    source env/bin/activate

    ./bootstrap
    source build/env-vars

    nosetests


.. |Version| image:: https://img.shields.io/pypi/v/tredis.svg?
   :target: https://pypi.python.org/pypi/tredis

.. |Status| image:: https://img.shields.io/travis/gmr/tredis.svg?
   :target: https://travis-ci.org/gmr/tredis

.. |Coverage| image:: https://img.shields.io/codecov/c/github/gmr/tredis.svg?
   :target: https://codecov.io/github/gmr/tredis?branch=master

.. |Downloads| image:: https://img.shields.io/pypi/dm/tredis.svg?
   :target: https://pypi.python.org/pypi/tredis

.. |CodeClimate| image:: https://img.shields.io/codeclimate/github/gmr/tredis.svg?
   :target: https://codeclimate.com/github/gmr/tredis

.. |PythonVersions| image:: https://img.shields.io/pypi/pyversions/tredis.svg?
   :target: https://github.com/gmr/tredis
