Example
=======
The following examples expect a pre-existing asynchronous application:

.. code-block:: python
   :caption: A simple set and get of a key from Redis

   client = tredis.RedisClient()

   yield client.set('foo', 'bar')
   value = yield client.get('foo')
