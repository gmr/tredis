Supported Commands
==================
The following table summarizes the number of commands supported by category. Categories
that are partially implemented enumerate the supported commands below. Implemented
commands are documented in the :py:class:`RedisClient <tredis.RedisClient>` documentation.

+--------------+----------+---------------+
| Category     | Count    | Version Added |
+==============+==========+===============+
| Cluster      | 0 of 20  | —             |
+--------------+----------+---------------+
| Connection   | 5 of 5   | 0.1.0         |
+--------------+----------+---------------+
| Geo          | 0 of 6   | —             |
+--------------+----------+---------------+
| Hashes       | 0 of 15  | —             |
+--------------+----------+---------------+
| HyperLogLog  | 0 of 3   | —             |
+--------------+----------+---------------+
| Keys         | 22 of 22 | 0.1.0         |
+--------------+----------+---------------+
| Lists        | 0 of 17  | —             |
+--------------+----------+---------------+
| Pub/Sub      | 0 of 6   | —             |
+--------------+----------+---------------+
| Scripting    | 0 of 6   | —             |
+--------------+----------+---------------+
| Server       | 0 of 30  | —             |
+--------------+----------+---------------+
| Sets         | 15 of 15 | 0.1.0         |
+--------------+----------+---------------+
| Sorted Sets  | 0 of 21  | —             |
+--------------+----------+---------------+
| Strings      | 3 of 23  | 0.1.0 [1]_    |
+--------------+----------+---------------+
| Transactions | 0 of 5   | —             |
+--------------+----------+---------------+

.. rubric:: Partial Category Implementations

.. [1] **Strings**: :py:meth:`get <tredis.RedisClient.get>`, :py:meth:`incr <tredis.RedisClient.incr>`, :py:meth:`set <tredis.RedisClient.set>`
