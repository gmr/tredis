TRedis
======
An asynchronous Redis client for Tornado

|Version| |Downloads| |Status| |Coverage| |CodeClimate| |PythonVersions|

Documentation is available at `tredis.readthedocs.org <http://tredis.readthedocs.org>`_.

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

For information on local development or contributing, see `CONTRIBUTING.rst <CONTRIBUTING.rst>`_

Example
-------

.. code:: python

   client = tredis.RedisClient()

   yield client.set('foo', 'bar')
   value = yield client.get('foo')

Pipelining
----------
tredis supports pipelining in a different way than other redis clients. To use
pipelining, simply call the ``tredis.RedisClient.pipeline_start()`` method,
then invoke all of the normal commands without yielding to them. When you have
created the pipeline, execute it with ``tredis.RedisClient.pipeline_execute()``:

.. code:: python

   client = tredis.RedisClient()

   # Start the pipeline
   client.pipeline_start()

   client.set('foo1', 'bar1')
   client.set('foo2', 'bar2')
   client.set('foo3', 'bar3')
   client.get('foo1')
   client.get('foo2')
   client.get('foo3')
   client.incr('foo4')
   client.incr('foo4')
   client.get('foo4')

   # Execute the pipeline
   responses = yield client.pipeline_execute()

   # The expected responses should match this list
   assert responses == [True, True, True, b'bar1', b'bar2', b'bar3', 1, 2, b'2']

.. warning:: Yielding after calling ``RedisClient.pipeline_start()`` and before
 calling ``yield RedisClient.pipeline_execute()`` can cause asynchronous request
 scope issues, as the client does not protect against other asynchronous requests
 from populating the pipeline. The only way to prevent this from happening is
 to make all pipeline additions inline without yielding to the ``IOLoop``.

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
