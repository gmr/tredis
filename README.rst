TRedis
======
An asynchronous Redis client for Tornado

|Version| |Downloads| |PythonVersions| |Status| |Coverage| |CodeClimate| |QuantifiedCode|

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
| Cluster      | 2 of 20  |
+--------------+----------+
| Connection   | 5 of 5   |
+--------------+----------+
| Geo          | 0 of 6   |
+--------------+----------+
| Hashes       | 13 of 15 |
+--------------+----------+
| HyperLogLog  | 3 of 3   |
+--------------+----------+
| Keys         | 22 of 22 |
+--------------+----------+
| Lists        | 0 of 17  |
+--------------+----------+
| Pub/Sub      | 0 of 6   |
+--------------+----------+
| Scripting    | 6 of 6   |
+--------------+----------+
| Server       | 7 of 30  |
+--------------+----------+
| Sets         | 15 of 15 |
+--------------+----------+
| Sorted Sets  | 6 of 21  |
+--------------+----------+
| Strings      | 23 of 23 |
+--------------+----------+
| Transactions | 0 of 5   |
+--------------+----------+

For information on local development or contributing, see `CONTRIBUTING.rst <CONTRIBUTING.rst>`_

Example
-------

.. code:: python

   import logging
   import pprint

   from tornado import gen, ioloop
   import tredis


   @gen.engine
   def run():
       client = tredis.Client([{"host": "127.0.0.1", "port": 6379, "db": 0}],
                              auto_connect=False)
       yield client.connect()
       value = yield client.info()
       pprint.pprint(value)
       ioloop.IOLoop.current().stop()

   if __name__ == '__main__':
       logging.basicConfig(level=logging.DEBUG)
       io_loop = ioloop.IOLoop.current()
       io_loop.add_callback(run)
       io_loop.start()


.. |Version| image:: https://img.shields.io/pypi/v/tredis.svg?
   :target: https://pypi.python.org/pypi/tredis

.. |PythonVersions| image:: https://img.shields.io/pypi/pyversions/tredis.svg?
   :target: https://github.com/gmr/tredis

.. |Status| image:: https://img.shields.io/travis/gmr/tredis.svg?
   :target: https://travis-ci.org/gmr/tredis

.. |Coverage| image:: https://img.shields.io/codecov/c/github/gmr/tredis.svg?
   :target: https://codecov.io/github/gmr/tredis?branch=master

.. |Downloads| image:: https://img.shields.io/pypi/dm/tredis.svg?
   :target: https://pypi.python.org/pypi/tredis

.. |CodeClimate| image:: https://codeclimate.com/github/gmr/tredis/badges/gpa.svg
   :target: https://codeclimate.com/github/gmr/tredis
   :alt: Code Climate

.. |QuantifiedCode| image:: https://www.quantifiedcode.com/api/v1/project/cbf1bf1b78cd441ba6078cfada0a8a9a/badge.svg
   :target: https://www.quantifiedcode.com/app/project/cbf1bf1b78cd441ba6078cfada0a8a9a
   :alt: Code issues

